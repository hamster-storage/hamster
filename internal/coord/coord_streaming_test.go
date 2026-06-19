package coord_test

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/hamster-storage/hamster/internal/coord"
	"github.com/hamster-storage/hamster/internal/meta"
	"github.com/hamster-storage/hamster/internal/sim"
)

// putStream drives one streaming PUT through the leader's coordinator: it feeds
// the body in chunkBytes-sized chunks via PutStream's backpressure callback,
// modelling the off-loop feeder as the in-sim driver. The body is in memory, so
// a chunk is handed over the moment the coordinator asks for it (a "fast
// feeder"); the seeded network decides when acks arrive, so the test exercises
// the want -> Feed -> step -> ack interleavings the real feeder will see. The
// coordinator never buffers more than its backpressure window regardless of
// object size.
func (c *cluster) putStream(key string, body []byte, chunkBytes int) (coord.PutResult, error) {
	c.t.Helper()
	id := c.leader()
	var (
		res     coord.PutResult
		perr    error
		done    bool
		h       *coord.PutHandle
		wants   int // want callbacks not yet serviced (++ on the loop, -- in the driver; sequential under the sim)
		nextOff int
		fedEOF  bool
	)
	c.worlds[id].Loop.Post(func() {
		h = c.nodes[id].co.PutStream(bucket, key, int64(len(body)), coord.PutOptions{},
			func() { wants++ },
			func(r coord.PutResult, e error) { res, perr, done = r, e, true })
	})
	for range 40000 {
		c.s.Run(tick)
		if done {
			return res, perr
		}
		// Service the coordinator's requests: one body chunk per want, posted
		// to the loop exactly as the real feeder's loop.Post(Feed) would, then
		// EOF once the body is exhausted.
		for wants > 0 && nextOff < len(body) {
			n := min(chunkBytes, len(body)-nextOff)
			chunk := append([]byte(nil), body[nextOff:nextOff+n]...)
			nextOff += n
			wants--
			c.worlds[id].Loop.Post(func() { h.Feed(chunk) })
		}
		if !fedEOF && wants > 0 && nextOff >= len(body) {
			fedEOF = true
			c.worlds[id].Loop.Post(func() { h.FeedEOF() })
		}
	}
	c.t.Fatal("streaming put never finished")
	return res, perr
}

// TestCoordinatorStreamingPut: a streamed PUT lands the same object as a
// buffered one. Across object sizes and feed-chunk granularities — including
// tiny chunks that stress the Feed/pending/step loop and a non-chunk-aligned
// tail — every stream commits the right parameters and decodes back
// bit-identically, the proof that the fed-with-backpressure path is equivalent
// to the whole-body path.
func TestCoordinatorStreamingPut(t *testing.T) {
	c := newCluster(t, 21, sim.NetConfig{}, 6, profile(t, "4+2"))
	c.propose(meta.CreateBucket{ProposedAtUnixMS: 1, Bucket: bucket})

	cases := []struct {
		size, chunk int
	}{
		{0, 1 << 20},           // empty object
		{100, 1 << 20},         // smaller than one chunk (and a k=1 small object)
		{64 << 10, 1 << 20},    // one whole chunk
		{1 << 20, 64 << 10},    // many chunks, aligned
		{5<<20 + 123, 1 << 20}, // multi-chunk with a non-aligned tail
		{4000, 100},            // tiny chunks
		{300, 1},               // one-byte chunks: maximal Feed/step churn
	}
	for i, tc := range cases {
		key := fmt.Sprintf("stream-%d", i)
		body := randomBody(uint64(i+100), tc.size)
		res, err := c.putStream(key, body, tc.chunk)
		if err != nil {
			t.Fatalf("%s (size %d chunk %d): %v", key, tc.size, tc.chunk, err)
		}
		wantK, wantM := c.profile.Params(int64(tc.size))
		if res.Durable != wantK+wantM {
			t.Errorf("%s: durable %d, want all %d", key, res.Durable, wantK+wantM)
		}
		e, ok := c.entry(key)
		if !ok || e.Size != int64(tc.size) || int(e.ECDataShards) != wantK || int(e.ECParityShards) != wantM {
			t.Fatalf("%s: entry size=%d k=%d m=%d ok=%v, want size=%d k=%d m=%d",
				key, e.Size, e.ECDataShards, e.ECParityShards, ok, tc.size, wantK, wantM)
		}
		got, err := c.readObject(key)
		if err != nil || !bytes.Equal(got, body) {
			t.Fatalf("%s: read back equal=%v err=%v", key, bytes.Equal(got, body), err)
		}
	}
}

// TestCoordinatorStreamingPutEquivalence: the same bytes streamed in chunks and
// written whole produce objects that read back identically — the wrapper and
// the streaming path are one path.
func TestCoordinatorStreamingPutEquivalence(t *testing.T) {
	c := newCluster(t, 22, sim.NetConfig{}, 6, profile(t, "4+2"))
	c.propose(meta.CreateBucket{ProposedAtUnixMS: 1, Bucket: bucket})

	body := randomBody(7, 2<<20+555)
	if _, err := c.putStream("via-stream", body, 96<<10); err != nil {
		t.Fatalf("streamed put: %v", err)
	}
	if _, err := c.put("via-buffer", body); err != nil {
		t.Fatalf("buffered put: %v", err)
	}
	streamed, err1 := c.readObject("via-stream")
	buffered, err2 := c.readObject("via-buffer")
	if err1 != nil || err2 != nil {
		t.Fatalf("read back: streamed %v, buffered %v", err1, err2)
	}
	if !bytes.Equal(streamed, body) || !bytes.Equal(buffered, body) {
		t.Fatal("streamed and buffered objects must both equal the source body")
	}
}

// TestCoordinatorStreamingPutFaultyNetwork: a streamed object rides drops,
// duplicates, and jitter on every shard stream — the backpressure pacing and
// the ack rule both survive an adversarial network, then decode back whole.
func TestCoordinatorStreamingPutFaultyNetwork(t *testing.T) {
	c := newCluster(t, 23, sim.NetConfig{
		MinLatency: time.Millisecond, MaxLatency: 15 * time.Millisecond,
		DropProb: 0.03, DuplicateProb: 0.03,
	}, 6, profile(t, "4+2"))
	c.propose(meta.CreateBucket{ProposedAtUnixMS: 1, Bucket: bucket})

	body := randomBody(9, 3<<20+1234)
	if _, err := c.putStream("faulty", body, 128<<10); err != nil {
		t.Fatalf("streamed put under faulty network: %v", err)
	}
	got, err := c.readObject("faulty")
	if err != nil || !bytes.Equal(got, body) {
		t.Fatalf("read back equal=%v err=%v", bytes.Equal(got, body), err)
	}
}

// TestPutWindowDefaults: the backpressure window honors the config knobs and
// falls back to the defaults when they are left zero.
func TestPutWindowDefaults(t *testing.T) {
	if got := coord.New(coord.Config{}).PutChunkSize(); got != 1<<20 {
		t.Fatalf("default PutChunkSize = %d, want %d", got, 1<<20)
	}
	if got := coord.New(coord.Config{PutChunkBytes: 7 << 10}).PutChunkSize(); got != 7<<10 {
		t.Fatalf("overridden PutChunkSize = %d, want %d", got, 7<<10)
	}
}
