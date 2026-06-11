package sim

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"slices"
	"testing"
	"time"

	"github.com/hamster-storage/hamster/internal/seam"
)

// pingNode forwards every message it receives to a random peer with a
// decremented hop count, recording each delivery. Under a jittery, lossy
// network this produces a trace whose exact shape depends on every PRNG
// choice the world makes — the right canary for seed determinism.
type pingNode struct {
	w     *World
	peers []seam.NodeID
	trace *[]string
}

func (n *pingNode) HandleMessage(from seam.NodeID, msg []byte) {
	*n.trace = append(*n.trace, fmt.Sprintf("%d %s->%s hops=%d",
		n.w.Clock.Now().UnixNano(), from, n.w.ID, msg[0]))
	if msg[0] == 0 {
		return
	}
	to := n.peers[n.w.Rand.IntN(len(n.peers))]
	n.w.Transport.Send(to, []byte{msg[0] - 1})
}

// pingTrace runs a three-node forwarding storm for one virtual minute and
// returns the delivery trace.
func pingTrace(seed uint64) []string {
	s := New(seed, NetConfig{
		MinLatency:    time.Millisecond,
		MaxLatency:    20 * time.Millisecond,
		DropProb:      0.1,
		DuplicateProb: 0.1,
	})
	ids := []seam.NodeID{"n1", "n2", "n3"}
	var trace []string
	for _, id := range ids {
		s.AddNode(id, func(w *World) seam.MessageHandler {
			var peers []seam.NodeID
			for _, p := range ids {
				if p != w.ID {
					peers = append(peers, p)
				}
			}
			n := &pingNode{w: w, peers: peers, trace: &trace}
			// Each node seeds the storm shortly after boot.
			w.Clock.AfterFunc(time.Duration(w.Rand.Int64N(int64(10*time.Millisecond))), func() {
				n.w.Transport.Send(peers[n.w.Rand.IntN(len(peers))], []byte{32})
			})
			return n
		})
	}
	s.Run(time.Minute)
	return trace
}

func TestSeedDeterminism(t *testing.T) {
	a := pingTrace(42)
	b := pingTrace(42)
	if len(a) == 0 {
		t.Fatal("trace is empty; the workload never ran")
	}
	if !slices.Equal(a, b) {
		t.Fatal("two runs with the same seed produced different traces")
	}
	c := pingTrace(43)
	if slices.Equal(a, c) {
		t.Fatal("different seeds produced identical traces; the PRNG is not driving the world")
	}
}

// crashWorld builds a one-node sim whose World is captured for direct
// driving by the test. The node's logic is inert; the test plays the role
// of the (single-threaded) event loop.
func crashWorld(seed uint64) (*Sim, **World) {
	s := New(seed, NetConfig{})
	w := new(*World)
	s.AddNode("n1", func(world *World) seam.MessageHandler {
		*w = world
		return inertNode{}
	})
	return s, w
}

type inertNode struct{}

func (inertNode) HandleMessage(seam.NodeID, []byte) {}

func TestCrashRespectsSync(t *testing.T) {
	synced := []byte("synced content")
	unsynced := []byte("unsynced content")
	var sawLost, sawSurvived bool

	for seed := range uint64(100) {
		s, w := crashWorld(seed)
		disk := (*w).Disk
		if err := disk.WriteFile("a", synced); err != nil {
			t.Fatal(err)
		}
		if err := disk.Sync("a"); err != nil {
			t.Fatal(err)
		}
		if err := disk.WriteFile("b", unsynced); err != nil {
			t.Fatal(err)
		}
		s.Crash("n1")
		s.Restart("n1")
		disk = (*w).Disk

		got, err := disk.ReadFile("a")
		if err != nil || !bytes.Equal(got, synced) {
			t.Fatalf("seed %d: synced file damaged by crash: %q, %v", seed, got, err)
		}
		got, err = disk.ReadFile("b")
		switch {
		case errors.Is(err, fs.ErrNotExist):
			sawLost = true
		case err != nil:
			t.Fatalf("seed %d: unexpected error reading unsynced file: %v", seed, err)
		case !bytes.HasPrefix(unsynced, got):
			t.Fatalf("seed %d: unsynced file is %q, neither lost nor a prefix of the write", seed, got)
		default:
			sawSurvived = true
		}
	}
	if !sawLost || !sawSurvived {
		t.Errorf("crash model is not exploring: lost=%v survived=%v over 100 seeds", sawLost, sawSurvived)
	}
}

func TestCrashRevertsUnsyncedRemove(t *testing.T) {
	content := []byte("durable")
	for seed := range uint64(20) {
		s, w := crashWorld(seed)
		disk := (*w).Disk
		for _, step := range []error{
			disk.WriteFile("f", content),
			disk.Sync("f"),
			disk.Remove("f"),
		} {
			if step != nil {
				t.Fatal(step)
			}
		}
		s.Crash("n1")
		s.Restart("n1")

		got, err := (*w).Disk.ReadFile("f")
		if err != nil || !bytes.Equal(got, content) {
			t.Fatalf("seed %d: unsynced remove became durable across crash: %q, %v", seed, got, err)
		}
	}
}

func TestSyncedRemoveIsDurable(t *testing.T) {
	s, w := crashWorld(1)
	disk := (*w).Disk
	for _, step := range []error{
		disk.WriteFile("f", []byte("x")),
		disk.Sync("f"),
		disk.Remove("f"),
		disk.Sync("f"),
	} {
		if step != nil {
			t.Fatal(step)
		}
	}
	s.Crash("n1")
	s.Restart("n1")
	if _, err := (*w).Disk.ReadFile("f"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("synced remove did not survive crash: err=%v", err)
	}
}

func TestPartitionBlocksDelivery(t *testing.T) {
	s := New(7, NetConfig{})
	var got []string
	worlds := map[seam.NodeID]**World{"a": new(*World), "b": new(*World)}
	for id, w := range worlds {
		s.AddNode(id, func(world *World) seam.MessageHandler {
			*w = world
			return recorderNode{world.ID, &got}
		})
	}
	send := func(msg string) {
		(*worlds["a"]).Transport.Send("b", []byte(msg))
		s.Run(time.Second)
	}

	s.Partition("a", "b")
	send("blocked")
	if len(got) != 0 {
		t.Fatalf("message crossed a partition: %v", got)
	}
	s.Heal("a", "b")
	send("after heal")
	want := []string{"a->b after heal"}
	if !slices.Equal(got, want) {
		t.Fatalf("after heal got %v, want %v", got, want)
	}
}

type recorderNode struct {
	id   seam.NodeID
	dest *[]string
}

func (n recorderNode) HandleMessage(from seam.NodeID, msg []byte) {
	*n.dest = append(*n.dest, fmt.Sprintf("%s->%s %s", from, n.id, msg))
}

func TestTimers(t *testing.T) {
	s, w := crashWorld(3)
	clock := (*w).Clock
	var fired []string
	clock.AfterFunc(2*time.Second, func() {
		fired = append(fired, fmt.Sprintf("late at %d", clock.Now().Unix()))
	})
	clock.AfterFunc(time.Second, func() {
		fired = append(fired, fmt.Sprintf("early at %d", clock.Now().Unix()))
	})
	stoppedTimer := clock.AfterFunc(time.Second, func() {
		fired = append(fired, "stopped")
	})
	if !stoppedTimer.Stop() {
		t.Fatal("Stop on a pending timer reported false")
	}
	if stoppedTimer.Stop() {
		t.Fatal("second Stop reported true")
	}
	s.Run(time.Minute)
	want := []string{"early at 1", "late at 2"}
	if !slices.Equal(fired, want) {
		t.Fatalf("timers fired %v, want %v", fired, want)
	}
}

func TestCrashKillsTimers(t *testing.T) {
	s, w := crashWorld(4)
	firedBeforeCrash := false
	(*w).Clock.AfterFunc(time.Second, func() { firedBeforeCrash = true })
	s.Crash("n1")
	s.Restart("n1")
	s.Run(time.Minute)
	if firedBeforeCrash {
		t.Fatal("a timer scheduled before a crash fired after restart")
	}
}

func TestDiskList(t *testing.T) {
	_, w := crashWorld(5)
	disk := (*w).Disk
	for _, step := range []error{
		disk.WriteFile("shards/2", []byte("b")),
		disk.WriteFile("shards/1", []byte("a")),
		disk.Sync("shards/1"),
		disk.WriteFile("gone", []byte("c")),
		disk.Sync("gone"),
		disk.Remove("gone"),
	} {
		if step != nil {
			t.Fatal(step)
		}
	}
	got, err := disk.List()
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"shards/1", "shards/2"}
	if !slices.Equal(got, want) {
		t.Fatalf("List() = %v, want %v", got, want)
	}
	if err := disk.WriteFile("../escape", nil); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("non-local name accepted: %v", err)
	}
}
