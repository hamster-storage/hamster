package datapath

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"math/rand/v2"
	"strings"
	"testing"
	"time"

	"github.com/hamster-storage/hamster/internal/meta"
	"github.com/hamster-storage/hamster/internal/seam"
	"github.com/hamster-storage/hamster/internal/sim"
)

var testID = meta.VersionID{0xDA, 0x7A, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14}

// TestGoldenWire pins the envelope and message encodings: these bytes
// travel between nodes of possibly different versions, so they may never
// drift (critical invariant 2).
func TestGoldenWire(t *testing.T) {
	cases := []struct {
		name string
		got  []byte
		want string
	}{
		{"envelope", Wrap(ChannelData, []byte("hi")), "080110021a026869"},
		{"chunk", encodeChunk(chunkMsg{key: shardKey{testID, 3}, offset: 65536, data: []byte("shard-bytes"), stream: 12}),
			"080112270a10da7a0102030405060708090a0b0c0d0e100318808004220b73686172642d6279746573280c"},
		{"commit", encodeCommit(commitMsg{key: shardKey{testID, 3}, length: 12345, checksum: []byte{0xAA, 0xBB}, stream: 12}),
			"08011a1d0a10da7a0102030405060708090a0b0c0d0e100318b9602202aabb280c"},
		{"writeAck", encodeWriteAck(writeAckMsg{key: shardKey{testID, 3}, staged: 4096, committed: true, errMsg: "x", stream: 12}),
			"0801221e0a10da7a0102030405060708090a0b0c0d0e100318802020012a0178300c"},
		{"read", encodeRead(readMsg{reqID: 7, key: shardKey{testID, 3}, offset: 512, length: 256}),
			"08012a1c08071210da7a0102030405060708090a0b0c0d0e1803208004288002"},
		{"readResult", encodeReadResult(readResultMsg{reqID: 7, data: []byte("data")}),
			"080132080807120464617461"},
		{"delete", encodeDelete(deleteMsg{reqID: 9, key: shardKey{testID, 3}}),
			"08013a1608091210da7a0102030405060708090a0b0c0d0e1803"},
		{"deleteAck", encodeDeleteAck(deleteAckMsg{reqID: 9}),
			"080142020809"},
		{"verify", encodeVerify(verifyMsg{reqID: 11, key: shardKey{testID, 3}}),
			"08014a16080b1210da7a0102030405060708090a0b0c0d0e1803"},
		{"verifyAck", encodeVerifyAck(verifyAckMsg{reqID: 11, committed: true, checksum: []byte{0xCC, 0xDD}, length: 7777}),
			"0801520b080b10011a02ccdd20e13c"},
	}
	for _, c := range cases {
		if got := hex.EncodeToString(c.got); got != strings.ReplaceAll(c.want, " ", "") {
			t.Errorf("%s = %s, want %s", c.name, got, c.want)
		}
	}
}

func TestWireRoundTrip(t *testing.T) {
	ch, payload, err := Unwrap(Wrap(ChannelRaft, []byte("raftmsg")))
	if err != nil || ch != ChannelRaft || string(payload) != "raftmsg" {
		t.Fatalf("envelope round trip: ch=%d payload=%q err=%v", ch, payload, err)
	}
	c, err := decodeChunk(mustBody(t, encodeChunk(chunkMsg{key: shardKey{testID, 5}, offset: 99, data: []byte("zz")})))
	if err != nil || c.key.index != 5 || c.offset != 99 || string(c.data) != "zz" || c.key.id != testID {
		t.Fatalf("chunk round trip: %+v err=%v", c, err)
	}
	// An unknown command must refuse, not half-apply.
	bad := putUvarint(nil, 1, wireVersion)
	bad = putBytes(bad, 60, []byte("future"))
	if _, _, err := decodeMessage(bad); err == nil {
		t.Fatal("unknown command accepted")
	}
}

func mustBody(t *testing.T, msg []byte) []byte {
	t.Helper()
	_, body, err := decodeMessage(msg)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

// node is the sim composition of one data-plane endpoint: the channel
// demux in front of a Service — the same wiring the real node will use.
type node struct {
	w   *sim.World
	svc *Service
}

func (n *node) HandleMessage(from seam.NodeID, msg []byte) {
	ch, payload, err := Unwrap(msg)
	if err != nil || ch != ChannelData {
		return
	}
	_ = n.svc.HandleData(from, payload)
}

// world wires a sim with the named nodes and returns them by ID.
func world(s *sim.Sim, ids ...seam.NodeID) map[seam.NodeID]*node {
	nodes := make(map[seam.NodeID]*node)
	for _, id := range ids {
		id := id
		s.AddNode(id, func(w *sim.World) seam.MessageHandler {
			n := &node{w: w, svc: New(Config{Clock: w.Clock, Transport: w.Transport, Disk: w.Disk})}
			nodes[id] = n
			return n
		})
	}
	return nodes
}

// outcome captures a callback result the sim will fill in later.
type outcome struct {
	done bool
	err  error
}

// startWrite drives a full shard write from n to target on n's loop,
// feeding data as the window allows and committing with its true checksum
// (unless a bad one is supplied).
func startWrite(n *node, target seam.NodeID, id meta.VersionID, index uint32, data []byte, badSum []byte, res *outcome) {
	n.w.Loop.Post(func() {
		sum := sha256.Sum256(data)
		checksum := sum[:]
		if badSum != nil {
			checksum = badSum
		}
		var ws *WriteStream
		pos := 0
		committed := false
		feed := func() {
			for pos < len(data) && ws.Window() > 0 {
				end := min(pos+ws.Window(), len(data))
				ws.Write(data[pos:end])
				pos = end
			}
			if pos == len(data) && !committed {
				committed = true
				ws.Commit(int64(len(data)), checksum)
			}
		}
		ws = n.svc.NewWrite(target, id, index, func() { feed() }, func(err error) {
			res.done, res.err = true, err
		})
		feed()
	})
}

func randomBytes(rng *rand.Rand, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(rng.UintN(256))
	}
	return b
}

func TestWriteHappyPath(t *testing.T) {
	s := sim.New(1, sim.NetConfig{})
	nodes := world(s, "a", "b")
	data := randomBytes(rand.New(rand.NewPCG(1, 1)), 1<<20+333)

	var res outcome
	startWrite(nodes["a"], "b", testID, 2, data, nil, &res)
	s.Run(time.Minute)

	if !res.done || res.err != nil {
		t.Fatalf("write: done=%v err=%v", res.done, res.err)
	}
	got, err := nodes["b"].w.Disk.ReadFile(ShardFileName(testID, 2))
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("shard on b: %d bytes, err=%v, equal=%v", len(got), err, bytes.Equal(got, data))
	}
	if _, err := nodes["b"].w.Disk.ReadFileAt(markerName(ShardFileName(testID, 2)), 0, 0); err != nil {
		t.Fatalf("commit marker missing: %v", err)
	}
}

// TestWriteUnderFaults runs the transfer over a hostile network — drops,
// duplicates, jitter — across seeds. Same bytes, same marker, every time.
func TestWriteUnderFaults(t *testing.T) {
	for seed := uint64(0); seed < 12; seed++ {
		s := sim.New(seed, sim.NetConfig{
			MinLatency: time.Millisecond, MaxLatency: 20 * time.Millisecond,
			DropProb: 0.05, DuplicateProb: 0.05,
		})
		nodes := world(s, "a", "b")
		data := randomBytes(rand.New(rand.NewPCG(seed, 7)), 300<<10+17)

		var res outcome
		startWrite(nodes["a"], "b", testID, 0, data, nil, &res)
		s.Run(2 * time.Minute)

		if !res.done || res.err != nil {
			t.Fatalf("seed %d: done=%v err=%v", seed, res.done, res.err)
		}
		got, err := nodes["b"].w.Disk.ReadFile(ShardFileName(testID, 0))
		if err != nil || !bytes.Equal(got, data) {
			t.Fatalf("seed %d: shard corrupt: %d bytes, err=%v", seed, len(got), err)
		}
	}
}

// TestReceiverCrashFailsWrite: the receiver crashing mid-transfer loses
// its staging, and the stream must fail — never resume over untrustworthy
// bytes — leaving no commit marker. A fresh attempt over the leftovers
// succeeds.
func TestReceiverCrashFailsWrite(t *testing.T) {
	s := sim.New(3, sim.NetConfig{MinLatency: 5 * time.Millisecond, MaxLatency: 10 * time.Millisecond})
	nodes := world(s, "a", "b")
	data := randomBytes(rand.New(rand.NewPCG(3, 3)), 4<<20)

	var res outcome
	startWrite(nodes["a"], "b", testID, 1, data, nil, &res)
	s.Run(22 * time.Millisecond) // a couple of windows in, nowhere near done
	if res.done {
		t.Fatal("4 MiB transfer finished in 22ms; the test needs a mid-transfer crash")
	}
	s.Crash("b")
	s.Run(50 * time.Millisecond)
	s.Restart("b")
	s.Run(time.Minute)

	if !res.done || res.err == nil {
		t.Fatalf("write into a crashed receiver: done=%v err=%v", res.done, res.err)
	}
	if _, err := nodes["b"].w.Disk.ReadFileAt(markerName(ShardFileName(testID, 1)), 0, 0); err == nil {
		t.Fatal("commit marker exists after a failed transfer")
	}

	// The retry — a new stream over whatever garbage survived — succeeds.
	var retry outcome
	startWrite(nodes["a"], "b", testID, 1, data, nil, &retry)
	s.Run(time.Minute)
	if !retry.done || retry.err != nil {
		t.Fatalf("retry: done=%v err=%v", retry.done, retry.err)
	}
	if got, err := nodes["b"].w.Disk.ReadFile(ShardFileName(testID, 1)); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("retried shard corrupt: err=%v", err)
	}
}

// TestPartitionTimesOut: a sender that can never be heard gives up with a
// terminal error instead of hanging forever.
func TestPartitionTimesOut(t *testing.T) {
	oldRTO, oldAttempts := rto, maxAttempts
	rto, maxAttempts = 50*time.Millisecond, 4
	defer func() { rto, maxAttempts = oldRTO, oldAttempts }()

	s := sim.New(4, sim.NetConfig{})
	nodes := world(s, "a", "b")
	s.Partition("a", "b")

	var res outcome
	startWrite(nodes["a"], "b", testID, 0, []byte("unreachable"), nil, &res)
	s.Run(time.Minute)

	if !res.done || res.err == nil || !strings.Contains(res.err.Error(), "no response") {
		t.Fatalf("partitioned write: done=%v err=%v", res.done, res.err)
	}
}

// TestChecksumMismatchIsTerminal: a commit whose checksum does not match
// the staged bytes must fail the transfer and leave nothing committed —
// the shard counted at ack provably matches the metadata that names it.
func TestChecksumMismatchIsTerminal(t *testing.T) {
	s := sim.New(5, sim.NetConfig{})
	nodes := world(s, "a", "b")

	bad := bytes.Repeat([]byte{0xEE}, 32)
	var res outcome
	startWrite(nodes["a"], "b", testID, 6, []byte("these bytes are fine"), bad, &res)
	s.Run(time.Minute)

	if !res.done || res.err == nil || !strings.Contains(res.err.Error(), "checksum") {
		t.Fatalf("bad checksum: done=%v err=%v", res.done, res.err)
	}
	if _, err := nodes["b"].w.Disk.ReadFileAt(markerName(ShardFileName(testID, 6)), 0, 0); err == nil {
		t.Fatal("commit marker exists after checksum mismatch")
	}
	if _, err := nodes["b"].w.Disk.ReadFile(ShardFileName(testID, 6)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("failed staging not removed: err=%v", err)
	}
}

// TestStragglerCannotClobber: a duplicate first chunk delivered after the
// transfer committed must not disturb the durable shard.
func TestStragglerCannotClobber(t *testing.T) {
	s := sim.New(6, sim.NetConfig{})
	nodes := world(s, "a", "b")
	data := []byte("the committed truth")

	var res outcome
	startWrite(nodes["a"], "b", testID, 0, data, nil, &res)
	s.Run(time.Minute)
	if !res.done || res.err != nil {
		t.Fatalf("setup write failed: %v", res.err)
	}

	// Replay the start of the transfer, as a duplicated message would.
	nodes["a"].w.Loop.Post(func() {
		nodes["a"].svc.send("b", encodeChunk(chunkMsg{key: shardKey{testID, 0}, offset: 0, data: []byte("GARBAGE")}))
	})
	s.Run(time.Minute)

	if got, err := nodes["b"].w.Disk.ReadFile(ShardFileName(testID, 0)); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("straggler clobbered the shard: %q err=%v", got, err)
	}
}

func TestFetch(t *testing.T) {
	s := sim.New(7, sim.NetConfig{
		MinLatency: time.Millisecond, MaxLatency: 10 * time.Millisecond,
		DropProb: 0.05, DuplicateProb: 0.05,
	})
	nodes := world(s, "a", "b")
	data := randomBytes(rand.New(rand.NewPCG(7, 7)), 100<<10)

	var res outcome
	startWrite(nodes["a"], "b", testID, 4, data, nil, &res)
	s.Run(2 * time.Minute)
	if !res.done || res.err != nil {
		t.Fatalf("setup write failed: %v", res.err)
	}

	fetch := func(offset int64, length int) ([]byte, error) {
		var got []byte
		var ferr error
		done := false
		nodes["a"].w.Loop.Post(func() {
			nodes["a"].svc.Fetch("b", testID, 4, offset, length, func(b []byte, err error) {
				got, ferr, done = b, err, true
			})
		})
		s.Run(2 * time.Minute)
		if !done {
			t.Fatal("fetch never finished")
		}
		return got, ferr
	}

	if got, err := fetch(0, 1000); err != nil || !bytes.Equal(got, data[:1000]) {
		t.Fatalf("head read: err=%v", err)
	}
	if got, err := fetch(50_000, 4096); err != nil || !bytes.Equal(got, data[50_000:54_096]) {
		t.Fatalf("middle read: err=%v", err)
	}
	if got, err := fetch(int64(len(data))-10, 100); err != nil || !bytes.Equal(got, data[len(data)-10:]) {
		t.Fatalf("EOF-short read: %d bytes err=%v", len(got), err)
	}
	if got, err := fetch(int64(len(data))+5, 10); err != nil || len(got) != 0 {
		t.Fatalf("past-EOF read: %d bytes err=%v", len(got), err)
	}
	// A shard that does not exist is an error, not silence.
	var missErr error
	missDone := false
	nodes["a"].w.Loop.Post(func() {
		nodes["a"].svc.Fetch("b", meta.VersionID{9, 9, 9}, 0, 0, 16, func(_ []byte, err error) {
			missErr, missDone = err, true
		})
	})
	s.Run(2 * time.Minute)
	if !missDone || missErr == nil {
		t.Fatalf("missing shard: done=%v err=%v", missDone, missErr)
	}
}

func TestDelete(t *testing.T) {
	s := sim.New(8, sim.NetConfig{})
	nodes := world(s, "a", "b")

	var res outcome
	startWrite(nodes["a"], "b", testID, 0, []byte("to be deleted"), nil, &res)
	s.Run(time.Minute)
	if !res.done || res.err != nil {
		t.Fatalf("setup write failed: %v", res.err)
	}

	del := func() error {
		var derr error
		done := false
		nodes["a"].w.Loop.Post(func() {
			nodes["a"].svc.Delete("b", testID, 0, func(err error) { derr, done = err, true })
		})
		s.Run(time.Minute)
		if !done {
			t.Fatal("delete never finished")
		}
		return derr
	}

	if err := del(); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := nodes["b"].w.Disk.ReadFile(ShardFileName(testID, 0)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("shard survives delete: err=%v", err)
	}
	// Idempotent: deleting the absent shard succeeds (retries demand it).
	if err := del(); err != nil {
		t.Fatalf("repeat delete: %v", err)
	}
}

// TestZeroLengthRefused: shard files always carry at least a header; a
// zero-length commit is a protocol error, refused terminally.
func TestZeroLengthRefused(t *testing.T) {
	s := sim.New(9, sim.NetConfig{})
	nodes := world(s, "a", "b")

	var res outcome
	startWrite(nodes["a"], "b", testID, 0, nil, nil, &res)
	s.Run(time.Minute)
	if !res.done || res.err == nil || !strings.Contains(res.err.Error(), "zero-length") {
		t.Fatalf("zero-length write: done=%v err=%v", res.done, res.err)
	}
}

// TestDeterminism: the same seed replays the same run — byte-identical
// outcome and disk state. The harness's whole premise, spot-checked here.
func TestDeterminism(t *testing.T) {
	run := func() (errStr string, shard []byte) {
		s := sim.New(11, sim.NetConfig{
			MinLatency: time.Millisecond, MaxLatency: 30 * time.Millisecond,
			DropProb: 0.1, DuplicateProb: 0.1,
		})
		nodes := world(s, "a", "b")
		data := randomBytes(rand.New(rand.NewPCG(11, 11)), 200<<10)
		var res outcome
		startWrite(nodes["a"], "b", testID, 0, data, nil, &res)
		s.Run(2 * time.Minute)
		got, _ := nodes["b"].w.Disk.ReadFile(ShardFileName(testID, 0))
		if res.err != nil {
			return res.err.Error(), got
		}
		return "", got
	}
	err1, shard1 := run()
	err2, shard2 := run()
	if err1 != err2 || !bytes.Equal(shard1, shard2) {
		t.Fatalf("same seed, different run: %q vs %q, %d vs %d bytes", err1, err2, len(shard1), len(shard2))
	}
}

func ExampleService() {
	s := sim.New(42, sim.NetConfig{})
	nodes := world(s, "gateway", "storage")
	data := []byte("an erasure-coded shard, in spirit")
	sum := sha256.Sum256(data)

	nodes["gateway"].w.Loop.Post(func() {
		svc := nodes["gateway"].svc
		ws := svc.NewWrite("storage", testID, 0, nil, func(err error) {
			fmt.Println("write done, err:", err)
		})
		ws.Write(data)
		ws.Commit(int64(len(data)), sum[:])
	})
	s.Run(time.Minute)

	nodes["gateway"].w.Loop.Post(func() {
		nodes["gateway"].svc.Fetch("storage", testID, 0, 3, 14, func(b []byte, err error) {
			fmt.Printf("fetched %q, err: %v\n", b, err)
		})
	})
	s.Run(time.Minute)

	// Output:
	// write done, err: <nil>
	// fetched "erasure-coded ", err: <nil>
}
