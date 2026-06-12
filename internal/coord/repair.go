package coord

import (
	"bytes"
	"fmt"
	"io"

	"github.com/hamster-storage/hamster/internal/datapath"
	"github.com/hamster-storage/hamster/internal/ec"
	"github.com/hamster-storage/hamster/internal/meta"
	"github.com/hamster-storage/hamster/internal/place"
	"github.com/hamster-storage/hamster/internal/seam"
)

// The repair sweep (ERASURE-CODING.md "one system"): walk every version
// the metadata names, verify every shard against its replicated checksum
// — which is scrub: bitrot on a shard nobody reads is found here, not at
// some future GET — and rebuild what is missing or rotted from any k
// verified survivors. Rebuilt shards travel the same staged-then-marker
// commit as fresh writes, so a crash mid-repair leaves garbage, never a
// lie, and the next sweep converges.
//
// v0.3 shape, stated honestly: one sweep at a time, objects processed
// sequentially, survivor shards buffered whole during a rebuild (the
// streaming, stripe-windowed rebuild and the continuous queue with
// throttling arrive with the operational repair system). Multipart
// entries are skipped — they ride the v0.1 blob path until the gateway
// joins the cluster.

// RepairReport is one sweep's outcome.
type RepairReport struct {
	// Objects is how many version entries the sweep examined.
	Objects int
	// Healthy is how many needed nothing.
	Healthy int
	// RebuiltShards is how many shards were reconstructed and committed.
	RebuiltShards int
	// Unrepairable lists versions with fewer than k verified shards —
	// unreadable until nodes return; repair cannot invent the bytes.
	Unrepairable []string
	// Failed lists versions whose repair was attempted and did not
	// complete (a rebuild write failed, say); the next sweep retries.
	Failed []string
	// Skipped is entries outside the v0.3 data path (multipart parts).
	Skipped int
}

// RepairSweep runs one full verify-and-rebuild pass over every bucket.
// done fires exactly once on the loop. Only one sweep may run at a time
// per coordinator.
func (c *Coordinator) RepairSweep(done func(RepairReport, error)) {
	op := &sweepOp{c: c, done: done}
	store := c.cfg.Raft.Store()
	for _, b := range store.ListBuckets() {
		bucket := b.Name
		store.ScanVersions(bucket, func(key string, e meta.VersionEntry) bool {
			if e.Kind != meta.KindObject {
				return true // delete markers hold no shards
			}
			if len(e.Parts) > 0 {
				op.report.Skipped++
				return true
			}
			op.work = append(op.work, sweepItem{bucket: bucket, key: key, entry: e})
			return true
		})
	}
	op.nextItem()
}

type sweepItem struct {
	bucket, key string
	entry       meta.VersionEntry
}

type sweepOp struct {
	c      *Coordinator
	done   func(RepairReport, error)
	work   []sweepItem
	report RepairReport

	// Per-item state, reset by startItem.
	item    sweepItem
	nodes   []seam.NodeID
	width   int
	k       int
	results []shardState
	pending int
}

type shardState struct {
	verified datapath.VerifyResult
	err      error
	body     []byte // fetched survivor bytes (sources only)
}

func (op *sweepOp) nextItem() {
	if len(op.work) == 0 {
		op.done(op.report, nil)
		return
	}
	op.item = op.work[0]
	op.work = op.work[1:]
	op.report.Objects++

	e := op.item.entry
	op.k = int(e.ECDataShards)
	op.width = int(e.ECDataShards + e.ECParityShards)
	nodes, err := place.Nodes(e.Partition, op.c.cfg.Members, op.width)
	if err != nil {
		op.itemFailed(fmt.Errorf("placing: %w", err))
		return
	}
	op.nodes = nodes
	op.results = make([]shardState, op.width)

	// Phase 1 — scrub: every holder hashes its shard; the verdict is the
	// comparison against the entry's replicated checksums, made here.
	op.pending = op.width
	for i := range op.nodes {
		i := i
		op.c.cfg.Data.Verify(op.nodes[i], e.DataID, uint32(i), func(r datapath.VerifyResult, err error) {
			op.results[i] = shardState{verified: r, err: err}
			op.pending--
			if op.pending == 0 {
				op.classify()
			}
		})
	}
}

// good reports whether shard i verified clean against the metadata.
func (op *sweepOp) good(i int) bool {
	r := op.results[i]
	return r.err == nil && r.verified.Committed &&
		bytes.Equal(r.verified.Checksum, op.item.entry.ShardChecksums[i])
}

// rotted reports a shard that exists, answered, and does not match —
// bitrot or a torn leftover wearing a marker it should not have.
func (op *sweepOp) rotted(i int) bool {
	r := op.results[i]
	return r.err == nil && r.verified.Committed && !op.good(i)
}

// classify decides the item's fate after scrub: healthy, rebuildable, or
// beyond help.
func (op *sweepOp) classify() {
	goodCount := 0
	for i := range op.results {
		if op.good(i) {
			goodCount++
		}
	}
	switch {
	case goodCount == op.width:
		op.report.Healthy++
		op.nextItem()
	case goodCount < op.k:
		op.report.Unrepairable = append(op.report.Unrepairable,
			fmt.Sprintf("%s/%s: %d of %d shards verified, need %d", op.item.bucket, op.item.key, goodCount, op.width, op.k))
		op.nextItem()
	default:
		op.fetchSources()
	}
}

// fetchSources is phase 2: pull k verified shards whole, to feed the
// checksum-verifying reconstruction.
func (op *sweepOp) fetchSources() {
	sources := 0
	type piece struct {
		shard       int
		off, length int64
	}
	var pieces []piece
	for i := range op.results {
		if !op.good(i) || sources == op.k {
			op.results[i].body = nil
			continue
		}
		sources++
		length := op.results[i].verified.Length
		op.results[i].body = make([]byte, length)
		for o := int64(0); o < length; o += fetchPiece {
			pieces = append(pieces, piece{i, o, min(int64(fetchPiece), length-o)})
		}
	}

	op.pending = len(pieces)
	fail := false
	for _, p := range pieces {
		p := p
		op.c.cfg.Data.Fetch(op.nodes[p.shard], op.item.entry.DataID, uint32(p.shard), p.off, int(p.length), func(b []byte, err error) {
			if err != nil || int64(len(b)) != p.length {
				fail = true
			} else {
				// Completions reorder under retries; every piece lands at
				// its own offset.
				copy(op.results[p.shard].body[p.off:], b)
			}
			op.pending--
			if op.pending == 0 {
				if fail {
					op.itemFailed(fmt.Errorf("fetching survivors"))
					return
				}
				op.reconstruct()
			}
		})
	}
}

// reconstruct is phase 3: the pure rebuild, verifying sources and rebuilt
// shards against the replicated checksums — corruption is never laundered.
func (op *sweepOp) reconstruct() {
	e := op.item.entry
	shards := make([]io.ReaderAt, op.width)
	rebuild := make([]io.Writer, op.width)
	bufs := make(map[int]*bytes.Buffer)
	for i := range op.results {
		if op.results[i].body != nil {
			shards[i] = bytes.NewReader(op.results[i].body)
		} else if !op.good(i) {
			b := &bytes.Buffer{}
			bufs[i] = b
			rebuild[i] = b
		}
		// Good but unfetched shards stay out of the pass entirely.
	}
	if err := ec.Reconstruct(shards, e.ShardChecksums, rebuild); err != nil {
		op.itemFailed(fmt.Errorf("reconstructing: %w", err))
		return
	}
	op.installRebuilt(bufs)
}

// installRebuilt is phase 4: place each rebuilt shard on its holder. A
// rotted holder is deleted first — a committed shard is immutable, so the
// only way to replace a bad one is to remove the lie, then write the
// truth through the ordinary staged-then-marker commit.
func (op *sweepOp) installRebuilt(bufs map[int]*bytes.Buffer) {
	var targets []int
	for i := range op.results {
		if _, ok := bufs[i]; ok {
			targets = append(targets, i)
		}
	}
	var install func()
	install = func() {
		if len(targets) == 0 {
			op.nextItem()
			return
		}
		i := targets[0]
		targets = targets[1:]
		data := bufs[i].Bytes()
		write := func() {
			op.writeShard(i, data, func(err error) {
				if err != nil {
					op.itemFailed(fmt.Errorf("installing shard %d: %w", i, err))
					return
				}
				op.report.RebuiltShards++
				install()
			})
		}
		if op.rotted(i) {
			op.c.cfg.Data.Delete(op.nodes[i], op.item.entry.DataID, uint32(i), func(err error) {
				if err != nil {
					op.itemFailed(fmt.Errorf("removing rotted shard %d: %w", i, err))
					return
				}
				write()
			})
		} else {
			write()
		}
	}
	install()
}

// writeShard streams one rebuilt shard to its holder, paced by the window.
func (op *sweepOp) writeShard(i int, data []byte, done func(error)) {
	e := op.item.entry
	var ws *datapath.WriteStream
	pos := 0
	feed := func() {
		for pos < len(data) && ws.Window() > 0 {
			end := min(pos+ws.Window(), len(data))
			ws.Write(data[pos:end])
			pos = end
		}
		if pos == len(data) {
			ws.Commit(int64(len(data)), e.ShardChecksums[i])
		}
	}
	// onWindow never fires after Commit (the stream guards it), so feed
	// cannot double-commit.
	ws = op.c.cfg.Data.NewWrite(op.nodes[i], e.DataID, uint32(i), func() { feed() }, done)
	feed()
}

func (op *sweepOp) itemFailed(err error) {
	op.report.Failed = append(op.report.Failed,
		fmt.Sprintf("%s/%s: %v", op.item.bucket, op.item.key, err))
	op.nextItem()
}
