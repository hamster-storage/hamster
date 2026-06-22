// Package coord is the data-path coordinator ([ADR-0027] decision 4): the
// event-loop state machines that turn one S3 operation into placement,
// erasure coding, shard transfer, and a metadata commit. A PUT paces the
// body stripe-by-stripe through the pure stream→ec composition into
// datapath write streams, enforces the acknowledgment rule ([ADR-0015]:
// all k+m durable on the healthy path, a hard floor of k+1, refusal
// below), and only then proposes the PutObject record through Raft — the
// linearization point, and the only part of the object that ever touches
// the consensus log (critical invariant 1).
//
// Loop-owned, like everything that decides: Put runs on the node's event
// loop, its callbacks arrive on the same loop (shard acks via the
// datapath service, the commit via raftnode), and the pure engines it
// drives (internal/stream, internal/ec) never block on the network — all
// waiting is coordinator state.
//
// [ADR-0015]: ../../docs/adr/0015-storage-profiles.md
// [ADR-0027]: ../../docs/adr/0027-v03-distributed-data-path.md
// [ADR-0028]: ../../docs/adr/0028-stored-cluster-layout.md
package coord

import (
	"io"
	"math/rand/v2"

	"github.com/hamster-storage/hamster/internal/datapath"
	"github.com/hamster-storage/hamster/internal/keys"
	"github.com/hamster-storage/hamster/internal/meta"
	"github.com/hamster-storage/hamster/internal/place"
	"github.com/hamster-storage/hamster/internal/seam"
)

// Proposer is the node's metadata plane as the coordinator drives it: commit a
// proposal, read the store, check leadership. Propose is called on the loop and
// its callback fires on the loop. *raftnode.Node satisfies it directly (the
// simulator's path); the cluster wraps that with proposal forwarding (ADR-0037),
// where a commit prepared on a non-leader is sent to the leader off-loop and the
// callback posted back, so the loop never blocks on the hop.
type Proposer interface {
	Propose(p any, done func(any, error))
	Store() *meta.Store
	Leader() (uint64, bool)
}

// Config carries a Coordinator's world and cluster shape. Placement reads
// from the stored, versioned cluster layout ([ADR-0028]): the member set
// and partition count are a committed fact, resolved per operation, not
// recomputed from whoever is in the cluster at the moment ([ADR-0027]
// decision 2 described the v0.3 derived placement this replaces).
type Config struct {
	// Clock and Rand are the node's loop-owned time and randomness —
	// version IDs are minted from explicit inputs, never ambient ones.
	Clock seam.Clock
	Rand  *rand.Rand

	// Data is the node's data-plane endpoint.
	Data *datapath.Service
	// Raft is the node's metadata plane: proposals in, store out. An interface
	// so the cluster can wrap the local raftnode with proposal forwarding
	// (ADR-0037) — a PUT or UploadPart coordinated on a non-leader runs the data
	// plane here and commits via the leader. Under the simulator it is the local
	// *raftnode.Node directly. Propose is called on the loop and its callback
	// fires on the loop; a forwarding implementation does the off-loop hop and
	// posts the callback back, so the loop never blocks.
	Raft Proposer

	// Layout resolves the cluster's placement basis for one operation: the
	// stored, versioned cluster layout ([ADR-0028]), read once per op so an
	// object's partition and its node ranking share a single generation. ok
	// is false until the first layout is installed (the cluster is still
	// forming); the operation then refuses transiently. The active storage
	// profile follows the layout's member count (the auto ladder). Loop-safe
	// — called on the loop.
	Layout func() (place.Layout, bool)

	// Encryption resolves the cluster's encryption posture (ADR-0021): the
	// node's loaded KEK and whether new writes should be encrypted. A PUT
	// uses both — encrypt when the bool is set, wrapping the DEK under the
	// KEK. A GET uses only the KEK, and only for an object whose own record
	// says it is encrypted: reads are posture-free, since each version
	// records what it is. nil (or a zero KEK with the bool false) is an
	// unencrypted cluster — the common case. Loop-safe — called on the loop.
	Encryption func() (keys.KEK, bool)

	// Entropy is the source a PUT draws a new DEK from when encrypting
	// (ADR-0021): crypto/rand in production, a seeded deterministic reader
	// under the simulator, so the encrypted write path stays deterministic —
	// the DEK is its only random input. Read only when Encryption reports on.
	Entropy io.Reader

	// Keyring resolves the node's loaded KEKs by fingerprint (ADR-0032).
	// Normally the node holds one (its master key); during a master-key
	// rotation it holds two — the old, to unwrap each DEK, and the new, to
	// rewrap it. The rewrap sweep looks both up by the posture's current and
	// rotating-to fingerprints. nil (or a fingerprint the node does not hold)
	// reports not-found — an unencrypted cluster, and the default in tests.
	// Loop-safe — called on the loop.
	Keyring func(fingerprint uint64) (keys.KEK, bool)

	// PutChunkBytes and PutMaxOutstanding tune the streaming-PUT backpressure
	// window: the feeder reads PutChunkBytes-sized chunks, and the coordinator
	// keeps at most PutMaxOutstanding of them requested-but-not-yet-encoded, so
	// a streaming PUT buffers at most their product in memory regardless of
	// object size. Zero means the default (defaultPutChunkBytes /
	// defaultPutMaxOutstanding). Exposed so load testing can tune the window.
	PutChunkBytes     int
	PutMaxOutstanding int
}

// Coordinator runs data-path operations for one node.
type Coordinator struct {
	cfg      Config
	liveness *liveness
	// sweeping is the single-flight guard shared by every repair sweep — the
	// operator optimize, the transition migration, and the background scrubber —
	// so at most one runs at a time. Loop-owned, so no lock.
	sweeping bool
	scrub    *scrubber
}

// New returns a Coordinator over cfg.
func New(cfg Config) *Coordinator {
	return &Coordinator{cfg: cfg, liveness: newLiveness()}
}

// beginSweep claims the single-flight guard, returning false if a sweep is
// already running. Loop-owned. endSweep releases it.
func (c *Coordinator) beginSweep() bool {
	if c.sweeping {
		return false
	}
	c.sweeping = true
	return true
}

func (c *Coordinator) endSweep() { c.sweeping = false }

// encryption resolves the node's KEK and write-time posture, tolerating an
// unset accessor — an unencrypted cluster, and the default in tests.
func (c *Coordinator) encryption() (keys.KEK, bool) {
	if c.cfg.Encryption == nil {
		return keys.KEK{}, false
	}
	return c.cfg.Encryption()
}

// keyFor resolves a loaded KEK by fingerprint for the rewrap sweep, tolerating
// an unset keyring (no key machinery).
func (c *Coordinator) keyFor(fingerprint uint64) (keys.KEK, bool) {
	if c.cfg.Keyring == nil {
		return keys.KEK{}, false
	}
	return c.cfg.Keyring(fingerprint)
}

// unwrapKEK resolves the KEK that unwraps a version's DEK (ADR-0032): the
// keyring entry the version's fingerprint names, so an object reads under
// whichever key wrapped it even after a rotation. A zero fingerprint (a version
// wrapped under the founding KEK, before fingerprints existed) or one the
// keyring does not hold falls back to the node's current write KEK — which is
// the founding key for the legacy case.
func (c *Coordinator) unwrapKEK(fingerprint uint64) keys.KEK {
	if fingerprint != 0 {
		if k, ok := c.keyFor(fingerprint); ok && k.Loaded() {
			return k
		}
	}
	kek, _ := c.encryption()
	return kek
}

// wrapNonce derives a DEK-wrap nonce from a version ID: its first bytes.
// The DataID is globally unique and never bumped, so the (KEK, nonce) pair
// never repeats — see keys.KEK.Wrap. Reusing the public version ID as the
// (non-secret) GCM nonce is safe; GCM needs uniqueness, not secrecy.
func wrapNonce(vid meta.VersionID) []byte { return vid[:keys.WrapNonceLen] }

// DownNodes returns the nodes this coordinator currently considers down — its
// passive view from data-plane operation outcomes (a PUT skips these to avoid
// their write timeout). Loop-owned: call it on the node's loop. The view is
// local to this node and best-effort, not a committed cluster fact.
func (c *Coordinator) DownNodes() []seam.NodeID {
	return c.liveness.down(c.cfg.Clock.Now())
}

// PutResult is an acknowledged PUT: the committed version, the ETag, and
// how many shards were durable at ack — the object's immediate loss
// budget is Durable − k, never less than one (the floor), rising to m
// once repair restores the spread.
type PutResult struct {
	VersionID meta.VersionID
	ETag      []byte
	Durable   int
}

// PartResult is an acknowledged multipart UploadPart (ADR-0038): the part's
// data address (its minted DataID, where the k+m shards live), the part's
// MD5 ETag, and the durable shard count at ack. The gateway returns the ETag
// to the client and, at CompleteMultipartUpload, matches the client's part
// list against it.
type PartResult struct {
	DataID  meta.VersionID
	ETag    []byte
	Durable int
}
