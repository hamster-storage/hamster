package coord

import (
	"errors"
	"fmt"
	"io"
	"sort"

	"github.com/hamster-storage/hamster/internal/ec"
	"github.com/hamster-storage/hamster/internal/meta"
	"github.com/hamster-storage/hamster/internal/place"
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

	width := int(entry.ECDataShards + entry.ECParityShards)
	nodes, err := place.Nodes(entry.Partition, c.cfg.Members, width)
	if err != nil {
		done(nil, fmt.Errorf("coord: resolving partition %d: %w", entry.Partition, err))
		return
	}

	op := &getOp{
		c: c, done: done,
		entry: entry, nodes: nodes,
		k: int(entry.ECDataShards), width: width,
		off: off, length: end - off,
		prefixes: make([][]byte, width),
		extents:  make([][]extent, width),
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

	entry meta.VersionEntry
	nodes []seam.NodeID
	k     int
	width int

	off, length int64

	prefixes [][]byte   // phase A: shard file fronts
	extents  [][]extent // phase B: covering slice ranges
	failed   []bool
	pending  int
	finished bool
}

// fetchHeaders is phase A: the file front of every shard, in parallel.
// Every fetch resolves (success, error, or timeout) before phase B, which
// needs the recorded slice geometry to know what to ask for.
func (op *getOp) fetchHeaders() {
	op.pending = op.width
	for i := range op.nodes {
		i := i
		op.c.cfg.Data.Fetch(op.nodes[i], op.entry.DataID, uint32(i), 0, headerPrefix, func(b []byte, err error) {
			if op.finished {
				return
			}
			if err != nil || len(b) == 0 {
				op.failed[i] = true
			} else {
				op.prefixes[i] = b
			}
			op.pending--
			if op.pending == 0 {
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
	cover := stream.Cover(op.entry.Size, stream.DefaultChunkSize, op.off, op.length)
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
		op.c.cfg.Data.Fetch(op.nodes[r.shard], op.entry.DataID, uint32(r.shard), r.off, int(r.length), func(b []byte, err error) {
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
	sr, err := stream.NewReader(er, er.FrameSize())
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
