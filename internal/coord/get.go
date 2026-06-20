package coord

import (
	"errors"
	"fmt"
	"io"
	"sort"

	"github.com/hamster-storage/hamster/internal/ec"
	"github.com/hamster-storage/hamster/internal/meta"
	"github.com/hamster-storage/hamster/internal/seam"
	"github.com/hamster-storage/hamster/internal/stream"
)

// headerPrefix is how much of each shard file the first phase fetches:
// enough for any shard header (tens of bytes, validated length field).
const headerPrefix = 512

// fetchPiece is the unit one slice fetch asks for — a slice, matching the
// datapath server's read cap comfortably.
const fetchPiece = ec.DefaultSliceSize

// ErrUnreadable is a GET finding fewer than k shards reachable: the
// object is intact on disk somewhere or repairable later, but this
// cluster, right now, cannot serve it.
var ErrUnreadable = errors.New("coord: too few shards reachable; object unreadable until nodes return or repair runs")

// Get reads the plaintext range [off, off+length) of the key's current
// version over the network ([ADR-0027] decision 4's read path): fetch the
// covering shard ranges — any k shards suffice — then decode and verify
// through the pure ec/stream readers. A negative length means the whole
// object. done fires exactly once on the loop with the verified bytes.
func (c *Coordinator) Get(bucket, key string, off, length int64, done func([]byte, error)) {
	store := c.cfg.Raft.Store()
	cur, found := store.Current(bucket, key)
	if !found {
		done(nil, fmt.Errorf("coord: no such key %s/%s", bucket, key))
		return
	}
	entry, found := store.GetVersion(bucket, key, cur.VersionID)
	if !found {
		done(nil, fmt.Errorf("coord: no such key %s/%s", bucket, key))
		return
	}
	c.GetEntry(entry, off, length, done)
}

// GetEntry serves the plaintext range [off, off+length) of a specific version
// entry over the network — the by-version read path. Get resolves a key's
// current version and delegates here; a versioned GET resolves the chosen
// version and calls this directly. The entry must be a stored object (a delete
// marker holds no shards). done fires exactly once on the loop.
func (c *Coordinator) GetEntry(entry meta.VersionEntry, off, length int64, done func([]byte, error)) {
	if off < 0 {
		off = 0
	}
	if length < 0 {
		length = entry.Size
	}
	end := min(off+length, entry.Size)
	if off > end {
		off = end
	}

	// A multipart object (ADR-0038) is stored as independently erasure-coded
	// parts, each with its own placement, geometry, and DEK. Map the requested
	// plaintext range onto the covering parts and read each as its own object,
	// stitching the results — a Range touches only the parts it overlaps.
	if len(entry.Parts) > 0 {
		c.getMultipart(entry, off, end, done)
		return
	}

	width := int(entry.ECDataShards + entry.ECParityShards)
	layout, ok := c.cfg.Layout()
	if !ok {
		done(nil, fmt.Errorf("coord: no cluster layout: %w", ErrUnreadable))
		return
	}
	nodes, oldNodes, err := layout.Locate(entry.Partition, width)
	if err != nil {
		done(nil, fmt.Errorf("coord: resolving partition %d: %w", entry.Partition, err))
		return
	}

	// Decrypt key resolution (ADR-0021, ADR-0032). An encrypted version records
	// its own algorithm and the fingerprint of the KEK that wrapped its DEK, so
	// reads are posture-free: unwrap under the key that fingerprint names,
	// regardless of whether the cluster still writes encrypted or has since
	// rotated. A node holding both keys mid-rotation reads objects on either; a
	// loaded key is required — a node that cannot produce it refuses the read
	// loudly rather than serving ciphertext.
	var dekBytes []byte
	if entry.EncAlgorithm != meta.EncNone {
		if entry.EncAlgorithm != meta.EncAES256GCM {
			done(nil, fmt.Errorf("coord: object uses unknown encryption algorithm %d", entry.EncAlgorithm))
			return
		}
		kek := c.unwrapKEK(entry.KEKFingerprint)
		if !kek.Loaded() {
			done(nil, fmt.Errorf("coord: object is encrypted but its KEK %016x is not loaded: %w", entry.KEKFingerprint, ErrUnreadable))
			return
		}
		dek, err := kek.Unwrap(entry.WrappedDEK)
		if err != nil {
			done(nil, fmt.Errorf("coord: unwrapping object key: %w", err))
			return
		}
		dekBytes = dek.Bytes()
	}

	op := &getOp{
		c: c, done: done,
		entry: entry, nodes: nodes, oldNodes: oldNodes,
		k: int(entry.ECDataShards), width: width,
		off: off, length: end - off,
		dek:      dekBytes,
		prefixes: make([][]byte, width),
		extents:  make([][]extent, width),
		source:   make([]seam.NodeID, width),
		failed:   make([]bool, width),
	}
	op.fetchHeaders()
}

// extent is one prefetched byte range of one shard file.
type extent struct {
	off  int64
	data []byte
}

type getOp struct {
	c    *Coordinator
	done func([]byte, error)

	entry    meta.VersionEntry
	nodes    []seam.NodeID // new (target) placement, per shard index
	oldNodes []seam.NodeID // prior placement during a transition, else nil
	dek      []byte        // unwrapped DEK for an encrypted object, else nil
	k        int
	width    int

	off, length int64

	prefixes [][]byte      // phase A: shard file fronts
	extents  [][]extent    // phase B: covering slice ranges
	source   []seam.NodeID // the node a shard's header actually came from
	failed   []bool
	pending  int
	planned  bool // phase B started; straggling header answers ignored
	finished bool
}

// candidates lists where shard i may live: its new home, and — during a
// transition — its old home when that differs. A shard mid-migration sits at
// exactly one of them (the Fetch is keyed by (DataID, index), so a node answers
// only if it physically holds that shard), so the first non-empty answer wins.
func (op *getOp) candidates(i int) []seam.NodeID {
	cands := []seam.NodeID{op.nodes[i]}
	if op.oldNodes != nil && op.oldNodes[i] != op.nodes[i] {
		cands = append(cands, op.oldNodes[i])
	}
	return cands
}

// fetchHeaders is phase A: the file front of every shard, in parallel.
// Phase B starts as soon as k shards have answered — a down node must
// cost a degraded read nothing, not a timeout — abandoning stragglers
// (their late answers are ignored). The trade, accepted: a slice fetch
// failing on one of the chosen k refuses a read that a straggler could
// have served; the client's retry chooses fresh responders.
func (op *getOp) fetchHeaders() {
	// One fetch per (shard index, candidate node): during a transition a shard
	// is probed at both its new and old home.
	type probe struct {
		i    int
		node seam.NodeID
	}
	var probes []probe
	for i := range op.nodes {
		for _, node := range op.candidates(i) {
			probes = append(probes, probe{i, node})
		}
	}
	op.pending = len(probes)
	for _, p := range probes {
		p := p
		op.c.cfg.Data.Fetch(p.node, op.entry.DataID, uint32(p.i), 0, headerPrefix, func(b []byte, err error) {
			// Feed liveness before the straggler guard: this phase touches
			// every shard's holder, so a header fetch that times out is the
			// cleanest down signal a read can give — and it lands after k have
			// already answered, exactly when the guard below would drop it.
			op.c.observe(p.node, err)
			if op.finished || op.planned {
				return
			}
			// First non-empty answer for a shard index wins and pins the node
			// the slice phase will read from (its new home, or its old one).
			if err == nil && len(b) > 0 && op.prefixes[p.i] == nil {
				op.prefixes[p.i] = b
				op.source[p.i] = p.node
			}
			op.pending--
			have := 0
			for j := range op.prefixes {
				if op.prefixes[j] != nil {
					have++
				}
			}
			if op.pending == 0 || have >= op.k {
				op.planned = true
				for j := range op.prefixes {
					if op.prefixes[j] == nil {
						op.failed[j] = true // unresolved stragglers: absent
					}
				}
				op.planSlices()
			}
		})
	}
}

// planSlices is phase B's setup: decode the headers that answered, take
// the recorded geometry, and compute each shard's covering slice ranges
// for the requested plaintext range.
func (op *getOp) planSlices() {
	var geo ec.Header
	have := 0
	for i, p := range op.prefixes {
		if op.failed[i] || p == nil {
			continue
		}
		h, err := ec.ReadHeader(io.ReaderAt(byteReaderAt(p)))
		if err != nil || h.ID != op.entry.DataID || h.Index != i {
			op.failed[i] = true // not a shard of this object: treat as absent
			op.prefixes[i] = nil
			continue
		}
		geo = h
		have++
	}
	if have < op.k {
		op.fail(fmt.Errorf("%w (%d of %d shards answered, need %d)", ErrUnreadable, have, op.width, op.k))
		return
	}

	// Frame ranges the stream reader will touch, mapped to stripes.
	cover := stream.Cover(op.entry.Size, stream.DefaultChunkSize, op.off, op.length, op.dek != nil)
	stripeBytes := int64(op.k) * geo.SliceSize
	type span struct{ first, last int64 }
	var spans []span
	for _, r := range cover {
		if r.Len == 0 {
			continue
		}
		spans = append(spans, span{r.Off / stripeBytes, (r.Off + r.Len - 1) / stripeBytes})
	}
	sort.Slice(spans, func(a, b int) bool { return spans[a].first < spans[b].first })
	var merged []span
	for _, s := range spans {
		if len(merged) > 0 && s.first <= merged[len(merged)-1].last+1 {
			merged[len(merged)-1].last = max(merged[len(merged)-1].last, s.last)
			continue
		}
		merged = append(merged, s)
	}

	// Issue the slice fetches: every merged stripe span, on every present
	// shard, in fetchPiece-sized requests. Reconstruction reads the same
	// ranges from whichever k shards answer, so degraded needs nothing
	// extra prefetched.
	type req struct {
		shard       int
		off, length int64
	}
	var reqs []req
	for i := range op.nodes {
		if op.failed[i] {
			continue
		}
		for _, s := range merged {
			start := geo.PayloadOffset + s.first*geo.SliceSize
			end := geo.PayloadOffset + (s.last+1)*geo.SliceSize
			for o := start; o < end; o += fetchPiece {
				reqs = append(reqs, req{i, o, min(int64(fetchPiece), end-o)})
			}
		}
	}
	if len(reqs) == 0 {
		op.assemble() // zero-byte body: the prefixes already cover it
		return
	}
	op.pending = len(reqs)
	for _, r := range reqs {
		r := r
		op.c.cfg.Data.Fetch(op.source[r.shard], op.entry.DataID, uint32(r.shard), r.off, int(r.length), func(b []byte, err error) {
			op.c.observe(op.source[r.shard], err)
			if op.finished {
				return
			}
			if err != nil {
				op.failed[r.shard] = true // one lost piece loses the shard
			} else if len(b) > 0 {
				op.extents[r.shard] = append(op.extents[r.shard], extent{off: r.off, data: b})
			}
			op.pending--
			if op.pending == 0 {
				op.assemble()
			}
		})
	}
}

// assemble is phase C: build sparse readers over what arrived and run the
// pure, verifying decode — reconstruction from any k if shards dropped
// out, every chunk CRC checked before a byte is returned.
func (op *getOp) assemble() {
	shards := make([]io.ReaderAt, op.width)
	present := 0
	for i := range shards {
		if op.failed[i] || op.prefixes[i] == nil {
			continue
		}
		s := &sparse{}
		s.add(extent{off: 0, data: op.prefixes[i]})
		for _, e := range op.extents[i] {
			s.add(e)
		}
		shards[i] = s
		present++
	}
	if present < op.k {
		op.fail(fmt.Errorf("%w (%d of %d shards complete, need %d)", ErrUnreadable, present, op.width, op.k))
		return
	}
	er, err := ec.NewReader(shards)
	if err != nil {
		op.fail(fmt.Errorf("coord: opening shards: %w", err))
		return
	}
	sr, err := stream.NewReader(er, er.FrameSize(), op.dek)
	if err != nil {
		op.fail(fmt.Errorf("coord: opening frame: %w", err))
		return
	}
	out := make([]byte, op.length)
	if op.length > 0 {
		if _, err := sr.ReadAt(out, op.off); err != nil {
			op.fail(fmt.Errorf("coord: decoding object: %w", err))
			return
		}
	}
	op.finished = true
	op.done(out, nil)
}

func (op *getOp) fail(err error) {
	op.finished = true
	op.done(nil, err)
}

// byteReaderAt adapts a prefix slice for header decoding.
type byteReaderAt []byte

func (b byteReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(b)) {
		return 0, io.EOF
	}
	n := copy(p, b[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

// sparse serves ReadAt from prefetched extents. Overlapping extents are
// fine (the header prefix overlaps the first slices of small shards);
// gaps inside a request are an error — the coordinator prefetched the
// wrong ranges, which must fail loudly, never quietly zero-fill.
type sparse struct {
	extents []extent // sorted by off
}

func (s *sparse) add(e extent) {
	i := sort.Search(len(s.extents), func(i int) bool { return s.extents[i].off > e.off })
	s.extents = append(s.extents, extent{})
	copy(s.extents[i+1:], s.extents[i:])
	s.extents[i] = e
}

func (s *sparse) ReadAt(p []byte, off int64) (int, error) {
	n := 0
	for n < len(p) {
		at := off + int64(n)
		covered := false
		for _, e := range s.extents {
			if e.off <= at && at < e.off+int64(len(e.data)) {
				n += copy(p[n:], e.data[at-e.off:])
				covered = true
				break
			}
		}
		if !covered {
			// Past every extent is end-of-data (short final slices end
			// exactly where the file does); a hole before one is a bug.
			for _, e := range s.extents {
				if e.off+int64(len(e.data)) > at {
					return n, fmt.Errorf("coord: read at %d not prefetched", at)
				}
			}
			return n, io.EOF
		}
	}
	return n, nil
}

// DeleteShards best-effort removes a version's shards from its holders —
// the reclaim after a metadata delete displaced the version. Outcomes are
// ignored: anything that survives is an orphan a future scan collects,
// unreadable as an object because no metadata names it.
func (c *Coordinator) DeleteShards(e meta.VersionEntry) {
	width := int(e.ECDataShards + e.ECParityShards)
	if width == 0 {
		return // a v0.1 whole-blob entry; not this data path's to reclaim
	}
	layout, ok := c.cfg.Layout()
	if !ok {
		return // no layout to resolve holders against; orphans await GC
	}
	nodes, err := layout.Nodes(e.Partition, width)
	if err != nil {
		return
	}
	for _, id := range e.DataIDs() {
		for i, n := range nodes {
			c.cfg.Data.Delete(n, id, uint32(i), func(error) {})
		}
	}
}
