package coord

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/hamster-storage/hamster/internal/datapath"
	"github.com/hamster-storage/hamster/internal/ec"
	"github.com/hamster-storage/hamster/internal/meta"
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
	// MigratedShards is how many shards were copied from their old home to
	// their new one during a layout transition (ADR-0004) — a copy, not a
	// reconstruct, so the bytes are moved whole, not recomputed.
	MigratedShards int
	// ReEncoded is how many object versions were re-encoded to a smaller profile
	// because they no longer fit the active node count — the data step of a
	// downsize (ADR-0004, ADR-0015).
	ReEncoded int
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
	c.runSweep(false, done)
}

// Optimize runs a sweep that also re-encodes every object up to the active-count
// storage profile (ADR-0004, ADR-0031) — the explicit operator step that spreads
// existing data across a grown cluster. Otherwise identical to RepairSweep: it
// still scrubs, rebuilds, and (during a transition) migrates. One sweep widens
// every under-width object; a second re-encodes nothing.
func (c *Coordinator) Optimize(done func(RepairReport, error)) {
	c.runSweep(true, done)
}

func (c *Coordinator) runSweep(optimize bool, done func(RepairReport, error)) {
	if !c.beginSweep() {
		done(RepairReport{}, ErrSweepBusy)
		return
	}
	work, skipped := c.collectSweepWork()
	op := &sweepOp{
		c:        c,
		optimize: optimize,
		work:     work,
		done:     func(r RepairReport, e error) { c.endSweep(); done(r, e) },
	}
	op.report.Skipped = skipped
	op.nextItem()
}

// ErrSweepBusy is returned when a sweep is requested while another (an operator
// optimize, a transition migration, or the background scrub) already holds the
// single-flight guard. The caller retries.
var ErrSweepBusy = errors.New("coord: a repair sweep is already running")

// collectSweepWork snapshots every whole-object version the metadata names — the
// scrub/repair work list — and counts the multipart entries skipped (they ride
// the v0.1 blob path). Shared by runSweep and the background scrubber.
func (c *Coordinator) collectSweepWork() (work []sweepItem, skipped int) {
	store := c.cfg.Raft.Store()
	for _, b := range store.ListBuckets() {
		bucket := b.Name
		store.ScanVersions(bucket, func(key string, e meta.VersionEntry) bool {
			if e.Kind != meta.KindObject {
				return true // delete markers hold no shards
			}
			if len(e.Parts) > 0 {
				skipped++
				return true
			}
			work = append(work, sweepItem{bucket: bucket, key: key, entry: e})
			return true
		})
	}
	return work, skipped
}

type sweepItem struct {
	bucket, key string
	entry       meta.VersionEntry
}

// reEncode rewrites the current item to a new profile — down for a shrink that no
// longer fits, up for an optimize after growth — and advances. Both share
// ReEncode's physical re-representation: the bytes, and so the object's identity
// and lock, are unchanged.
func (op *sweepOp) reEncode(tk, tm int) {
	op.c.ReEncode(op.item.bucket, op.item.key, op.item.entry, tk, tm, func(err error) {
		if err != nil {
			op.report.Failed = append(op.report.Failed,
				fmt.Sprintf("%s/%s: re-encode: %v", op.item.bucket, op.item.key, err))
		} else {
			op.report.ReEncoded++
		}
		op.nextItem()
	})
}

type sweepOp struct {
	c        *Coordinator
	done     func(RepairReport, error)
	optimize bool // also re-encode under-width objects up to the active profile
	work     []sweepItem
	report   RepairReport

	// Per-item state, reset by startItem.
	item    sweepItem
	nodes   []seam.NodeID
	width   int
	k       int
	results []shardState
	pending int

	// Transition state (ADR-0004), populated only while a layout rebalance is
	// in flight (Locate returns an old placement). oldNodes is the prior
	// placement, oldResults the scrub of the shards not already at their new
	// home, and the two target lists partition those shards into the ones that
	// can be copied across whole (good at their old home) and the ones that
	// must be reconstructed (good at neither home). srcBodies buffers the k
	// survivors a reconstruct reads from. nil/empty in steady state.
	oldNodes           []seam.NodeID
	oldResults         []shardState
	copyTargets        []int
	reconstructTargets []int
	srcBodies          map[int][]byte
}

// srcFetch names one ranged read of a survivor shard during a transition
// reconstruct: the shard index, the node currently holding a good copy
// (its new home or, mid-migration, its old one), and the slice to pull.
type srcFetch struct {
	i           int
	node        seam.NodeID
	off, length int64
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
	layout, ok := op.c.cfg.Layout()
	if !ok {
		op.itemFailed(fmt.Errorf("placing: no cluster layout"))
		return
	}
	active := layout.ActiveCount()
	// Downsize (ADR-0004, ADR-0015): an object whose width no longer fits the
	// active node count is re-encoded down to the active-count profile — the
	// data step of a shrink. ReEncode moves it off the leaving node by writing
	// the narrower shards to the active set; healing/migrating it at the old
	// width would only chase a placement that cannot exist. Always — a shrink
	// must complete whether or not the sweep was asked to optimize.
	if op.width > active {
		tk, tm := ec.AutoProfile(active).Params(e.Size)
		op.reEncode(tk, tm)
		return
	}
	// Upsize (ADR-0004, ADR-0031): an object encoded narrower than the active
	// profile is widened to it — the data step of growing into a larger cluster.
	// Only on an explicit optimize, never automatic: growth keeps data readable at
	// its old width, so re-spreading it across the new nodes is the operator's
	// call (cluster optimize), not a side effect of every repair sweep.
	if op.optimize {
		if tk, tm := ec.AutoProfile(active).Params(e.Size); tk+tm > op.width {
			op.reEncode(tk, tm)
			return
		}
	}
	// Locate resolves the new placement and, while a layout transition is in
	// flight, the old one (ADR-0004). In steady state oldNodes is nil and the
	// sweep behaves exactly as before; during a transition the shards live at
	// their old home until repair migrates them to the new one.
	newNodes, oldNodes, err := layout.Locate(e.Partition, op.width)
	if err != nil {
		op.itemFailed(fmt.Errorf("placing: %w", err))
		return
	}
	op.nodes = newNodes
	op.oldNodes = oldNodes
	op.oldResults = nil
	op.results = make([]shardState, op.width)

	// Phase 1 — scrub: every holder hashes its shard; the verdict is the
	// comparison against the entry's replicated checksums, made here.
	op.pending = op.width
	for i := range op.nodes {
		i := i
		op.c.cfg.Data.Verify(op.nodes[i], e.DataID, uint32(i), func(r datapath.VerifyResult, err error) {
			// Scrub touches every holder, so its verify outcomes are the
			// repair path's liveness signal: a node that never answers is
			// down (a PUT will skip it), one that answers — even "I hold
			// nothing" — is up.
			op.c.observe(op.nodes[i], err)
			op.results[i] = shardState{verified: r, err: err}
			op.pending--
			if op.pending == 0 {
				op.afterScrubNew()
			}
		})
	}
}

// afterScrubNew routes the item after the new-home scrub: steady state falls
// straight into the existing classify/rebuild path; a transition first scrubs
// the old home of any shard not already at its new one, then migrates.
func (op *sweepOp) afterScrubNew() {
	if op.oldNodes == nil {
		op.classify()
		return
	}
	op.scrubOld()
}

// scrubOld verifies, at their old home, the shards not already good at their
// new one — the candidates for migration. Shards already at the new home need
// no old read. With nothing left to check, the item is fully at its new home.
func (op *sweepOp) scrubOld() {
	op.oldResults = make([]shardState, op.width)
	var idx []int
	for i := 0; i < op.width; i++ {
		if !op.good(i) {
			idx = append(idx, i)
		}
	}
	if len(idx) == 0 {
		op.migrate()
		return
	}
	op.pending = len(idx)
	for _, i := range idx {
		i := i
		op.c.cfg.Data.Verify(op.oldNodes[i], op.item.entry.DataID, uint32(i), func(r datapath.VerifyResult, err error) {
			op.c.observe(op.oldNodes[i], err)
			op.oldResults[i] = shardState{verified: r, err: err}
			op.pending--
			if op.pending == 0 {
				op.migrate()
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

// goodOld reports whether shard i verified clean at its old home — a
// migration source. Only meaningful during a transition (oldResults set).
func (op *sweepOp) goodOld(i int) bool {
	if op.oldResults == nil {
		return false
	}
	r := op.oldResults[i]
	return r.err == nil && r.verified.Committed &&
		bytes.Equal(r.verified.Checksum, op.item.entry.ShardChecksums[i])
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
			op.c.observe(op.nodes[p.shard], err)
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

// migrate is the transition counterpart of classify: it brings every shard of
// a version to its new home (ADR-0004). A shard already good at its new home is
// settled; one good only at its old home is copied across whole — migration is
// a copy, not a reconstruct, because during a transition the shard exists, just
// in the wrong place, so there is nothing to recompute; one good at neither
// home is reconstructed from any k survivors across old+new (the path for a
// shard damaged mid-move). Below k survivors total, the version is unrepairable.
func (op *sweepOp) migrate() {
	op.copyTargets = op.copyTargets[:0]
	op.reconstructTargets = op.reconstructTargets[:0]
	settled, sources := 0, 0
	for i := 0; i < op.width; i++ {
		switch {
		case op.good(i):
			settled++
			sources++
		case op.goodOld(i):
			op.copyTargets = append(op.copyTargets, i)
			sources++
		default:
			op.reconstructTargets = append(op.reconstructTargets, i)
		}
	}
	if settled == op.width {
		op.report.Healthy++
		op.nextItem()
		return
	}
	if len(op.reconstructTargets) > 0 && sources < op.k {
		op.report.Unrepairable = append(op.report.Unrepairable,
			fmt.Sprintf("%s/%s: %d of %d shards available across old+new, need %d",
				op.item.bucket, op.item.key, sources, op.width, op.k))
		op.nextItem()
		return
	}
	op.migrateCopies()
}

// migrateCopies streams each copyable shard from its old home to its new one,
// one at a time, then hands off to the reconstruct phase.
func (op *sweepOp) migrateCopies() {
	if len(op.copyTargets) == 0 {
		op.migrateReconstruct()
		return
	}
	i := op.copyTargets[0]
	op.copyTargets = op.copyTargets[1:]
	op.fetchWhole(op.oldNodes[i], i, op.oldResults[i].verified.Length, func(body []byte, err error) {
		if err != nil {
			op.itemFailed(fmt.Errorf("migrating shard %d: %w", i, err))
			return
		}
		op.installShard(i, body, func(err error) {
			if err != nil {
				op.itemFailed(fmt.Errorf("installing migrated shard %d: %w", i, err))
				return
			}
			op.report.MigratedShards++
			op.migrateCopies()
		})
	})
}

// migrateReconstruct rebuilds the shards good at neither home from any k
// survivors (fetched whole from wherever each is good), then installs them at
// their new home. Reached only for shards damaged mid-move; the common
// transition is pure copy and skips straight through.
func (op *sweepOp) migrateReconstruct() {
	if len(op.reconstructTargets) == 0 {
		op.nextItem()
		return
	}
	op.srcBodies = make(map[int][]byte)
	var fetches []srcFetch
	chosen := 0
	for i := 0; i < op.width && chosen < op.k; i++ {
		var node seam.NodeID
		var length int64
		switch {
		case op.good(i):
			node, length = op.nodes[i], op.results[i].verified.Length
		case op.goodOld(i):
			node, length = op.oldNodes[i], op.oldResults[i].verified.Length
		default:
			continue
		}
		chosen++
		op.srcBodies[i] = make([]byte, length)
		for o := int64(0); o < length; o += fetchPiece {
			fetches = append(fetches, srcFetch{i, node, o, min(int64(fetchPiece), length-o)})
		}
	}

	op.pending = len(fetches)
	fail := false
	for _, f := range fetches {
		f := f
		op.c.cfg.Data.Fetch(f.node, op.item.entry.DataID, uint32(f.i), f.off, int(f.length), func(b []byte, err error) {
			op.c.observe(f.node, err)
			if err != nil || int64(len(b)) != f.length {
				fail = true
			} else {
				copy(op.srcBodies[f.i][f.off:], b)
			}
			op.pending--
			if op.pending == 0 {
				if fail {
					op.itemFailed(fmt.Errorf("fetching survivors"))
					return
				}
				op.migrateRebuild()
			}
		})
	}
}

// migrateRebuild runs the pure reconstruction for the damaged-mid-move shards
// and installs each at its new home.
func (op *sweepOp) migrateRebuild() {
	e := op.item.entry
	shards := make([]io.ReaderAt, op.width)
	for i, b := range op.srcBodies {
		shards[i] = bytes.NewReader(b)
	}
	rebuild := make([]io.Writer, op.width)
	bufs := make(map[int]*bytes.Buffer)
	for _, i := range op.reconstructTargets {
		buf := &bytes.Buffer{}
		bufs[i] = buf
		rebuild[i] = buf
	}
	if err := ec.Reconstruct(shards, e.ShardChecksums, rebuild); err != nil {
		op.itemFailed(fmt.Errorf("reconstructing: %w", err))
		return
	}
	targets := append([]int(nil), op.reconstructTargets...)
	var install func()
	install = func() {
		if len(targets) == 0 {
			op.nextItem()
			return
		}
		i := targets[0]
		targets = targets[1:]
		op.installShard(i, bufs[i].Bytes(), func(err error) {
			if err != nil {
				op.itemFailed(fmt.Errorf("installing shard %d: %w", i, err))
				return
			}
			op.report.RebuiltShards++
			install()
		})
	}
	install()
}

// installShard writes data as shard i at its new home, deleting a rotted
// leftover there first — a committed shard is immutable, so a bad one is
// removed before the truth is written through the ordinary staged-then-marker
// commit. A merely-absent new home is written directly.
func (op *sweepOp) installShard(i int, data []byte, done func(error)) {
	write := func() { op.writeShard(i, data, done) }
	if op.rotted(i) {
		op.c.cfg.Data.Delete(op.nodes[i], op.item.entry.DataID, uint32(i), func(err error) {
			if err != nil {
				done(err)
				return
			}
			write()
		})
		return
	}
	write()
}

// fetchWhole pulls shard i whole from node, in window-sized pieces, and hands
// the assembled bytes to done. Used to move a survivor shard across during a
// transition.
func (op *sweepOp) fetchWhole(node seam.NodeID, i int, length int64, done func([]byte, error)) {
	body := make([]byte, length)
	if length == 0 {
		done(body, nil)
		return
	}
	pieces := 0
	for o := int64(0); o < length; o += fetchPiece {
		pieces++
	}
	op.pending = pieces
	fail := false
	for o := int64(0); o < length; o += fetchPiece {
		off, ln := o, min(int64(fetchPiece), length-o)
		op.c.cfg.Data.Fetch(node, op.item.entry.DataID, uint32(i), off, int(ln), func(b []byte, err error) {
			op.c.observe(node, err)
			if err != nil || int64(len(b)) != ln {
				fail = true
			} else {
				copy(body[off:], b)
			}
			op.pending--
			if op.pending == 0 {
				if fail {
					done(nil, fmt.Errorf("fetching shard %d", i))
					return
				}
				done(body, nil)
			}
		})
	}
}

func (op *sweepOp) itemFailed(err error) {
	op.report.Failed = append(op.report.Failed,
		fmt.Sprintf("%s/%s: %v", op.item.bucket, op.item.key, err))
	op.nextItem()
}
