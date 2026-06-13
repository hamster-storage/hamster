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
	"math/rand/v2"

	"github.com/hamster-storage/hamster/internal/datapath"
	"github.com/hamster-storage/hamster/internal/meta"
	"github.com/hamster-storage/hamster/internal/place"
	"github.com/hamster-storage/hamster/internal/raftnode"
	"github.com/hamster-storage/hamster/internal/seam"
)

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
	// Raft is the node's metadata plane: proposals in, store out.
	Raft *raftnode.Node

	// Layout resolves the cluster's placement basis for one operation: the
	// stored, versioned cluster layout ([ADR-0028]), read once per op so an
	// object's partition and its node ranking share a single generation. ok
	// is false until the first layout is installed (the cluster is still
	// forming); the operation then refuses transiently. The active storage
	// profile follows the layout's member count (the auto ladder). Loop-safe
	// — called on the loop.
	Layout func() (place.Layout, bool)
}

// Coordinator runs data-path operations for one node.
type Coordinator struct {
	cfg Config
}

// New returns a Coordinator over cfg.
func New(cfg Config) *Coordinator {
	return &Coordinator{cfg: cfg}
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
