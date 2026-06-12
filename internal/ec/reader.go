package ec

import (
	"bytes"
	"fmt"
	"io"
	"sync"

	"github.com/klauspost/reedsolomon"

	"github.com/hamster-storage/hamster/internal/meta"
)

// Reader reassembles a framed object stream from its shards, as an
// io.ReaderAt over the frame's bytes — the shape stream.NewReader wants.
// Any k of the k+m shards suffice: with every data shard present, a read
// touches only the data slices the range covers; with data shards
// missing, the touched stripes are reconstructed from whichever k shards
// survive. More than m missing is unreadable, loudly.
type Reader struct {
	geo    geometry
	id     meta.VersionID
	rs     reedsolomon.Encoder
	shards []io.ReaderAt
	offs   []int64 // per-shard payload offset; -1 when the shard is absent

	mu        sync.Mutex
	cachedIdx int64 // single-stripe cache: sequential reads are the hot path
	cached    []byte
}

// NewReader opens a shard set. shards is indexed by shard number with nil
// for missing; at least k must be present, and every present header must
// agree on what is being read.
func NewReader(shards []io.ReaderAt) (*Reader, error) {
	r := &Reader{shards: shards, cachedIdx: -1}
	present := 0
	for i, s := range shards {
		if s == nil {
			continue
		}
		h, off, err := decodeShard(s)
		if err != nil {
			return nil, fmt.Errorf("ec: shard %d: %w", i, err)
		}
		if h.index != i {
			return nil, fmt.Errorf("ec: shard %d carries header index %d: misplaced file", i, h.index)
		}
		g := geometry{k: h.k, m: h.m, sliceSize: h.sliceSize, frameSize: h.frameSize}
		if present == 0 {
			r.geo, r.id = g, h.id
			r.offs = make([]int64, h.k+h.m)
			for j := range r.offs {
				r.offs[j] = -1
			}
			if len(shards) != h.k+h.m {
				return nil, fmt.Errorf("ec: %d shard slots for a %d+%d object", len(shards), h.k, h.m)
			}
		} else if g != r.geo || h.id != r.id {
			return nil, fmt.Errorf("ec: shard %d disagrees with its siblings: mixed shard sets", i)
		}
		r.offs[i] = off
		present++
	}
	if present == 0 {
		return nil, fmt.Errorf("ec: no shards")
	}
	if present < r.geo.k {
		return nil, fmt.Errorf("ec: %d of %d+%d shards present, need %d: object unreadable until repaired",
			present, r.geo.k, r.geo.m, r.geo.k)
	}
	if r.geo.m > 0 {
		rs, err := reedsolomon.New(r.geo.k, r.geo.m)
		if err != nil {
			return nil, fmt.Errorf("ec: %d+%d decoder: %w", r.geo.k, r.geo.m, err)
		}
		r.rs = rs
	}
	return r, nil
}

// FrameSize is the framed stream's total size — what stream.NewReader
// needs alongside this Reader.
func (r *Reader) FrameSize() int64 { return r.geo.frameSize }

// DataID is the object data address every shard header named.
func (r *Reader) DataID() meta.VersionID { return r.id }

// ReadAt reads frame bytes at off, per io.ReaderAt.
func (r *Reader) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("ec: negative read offset %d", off)
	}
	stripeBytes := int64(r.geo.k) * r.geo.sliceSize
	var n int
	for n < len(p) && off < r.geo.frameSize {
		si := off / stripeBytes
		data, err := r.stripeData(si)
		if err != nil {
			return n, err
		}
		c := copy(p[n:], data[off-si*stripeBytes:])
		n += c
		off += int64(c)
	}
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

// stripeData returns stripe si's frame bytes, reading the data slices
// directly when they are all present and reconstructing from any k
// otherwise. A single-stripe cache absorbs the chunk-not-aligned-to-
// stripe rereads of sequential GETs.
func (r *Reader) stripeData(si int64) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if si == r.cachedIdx {
		return r.cached, nil
	}
	l, sliceOff := r.geo.stripeSlice(si)

	slices := make([][]byte, r.geo.k+r.geo.m)
	missingData := false
	for i := range r.geo.k {
		if r.offs[i] < 0 {
			missingData = true
			break
		}
	}
	read := func(i int) error {
		b := make([]byte, l)
		if err := readFull(r.shards[i], b, r.offs[i]+sliceOff, fmt.Sprintf("shard %d stripe %d", i, si)); err != nil {
			return err
		}
		slices[i] = b
		return nil
	}
	if !missingData {
		for i := range r.geo.k {
			if err := read(i); err != nil {
				return nil, err
			}
		}
	} else {
		// Gather any k surviving slices and rebuild the data ones.
		got := 0
		for i := range slices {
			if r.offs[i] < 0 || got == r.geo.k {
				continue
			}
			if err := read(i); err != nil {
				return nil, err
			}
			got++
		}
		if err := r.rs.ReconstructData(slices); err != nil {
			return nil, fmt.Errorf("ec: reconstructing stripe %d: %w", si, err)
		}
	}

	data := bytes.Join(slices[:r.geo.k], nil)
	data = data[:r.geo.stripeData(si)] // drop the final stripe's padding
	r.cachedIdx, r.cached = si, data
	return data, nil
}
