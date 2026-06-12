package ec

import (
	"crypto/sha256"
	"fmt"
	"hash"
	"io"

	"github.com/klauspost/reedsolomon"

	"github.com/hamster-storage/hamster/internal/meta"
)

// DefaultSliceSize is the payload bytes each shard receives per stripe
// ([ADR-0026]): large enough that shard I/O is sequential, small enough
// that one stripe (k+m slices) keeps the write path's memory bounded.
// Every shard records its own slice size, so retuning changes new writes
// only.
//
// [ADR-0026]: ../../docs/adr/0026-stripe-and-shard-layout.md
const DefaultSliceSize = 256 << 10

// sliceSize is what new shards are written with — a var so tests can
// shrink it to exercise many stripes cheaply. Read paths always take the
// slice size from the shard header, never from here.
var sliceSize = int64(DefaultSliceSize)

// Writer erasure-codes one framed object stream into k+m shard files,
// stripe by stripe: at most one stripe (k data slices) plus its parity is
// in memory. The caller declares the frame size up front — the frame
// layout makes it computable from the plaintext size (stream.FrameSize) —
// because every shard's header states it.
type Writer struct {
	geo     geometry
	rs      reedsolomon.Encoder // nil for 1+0: a single whole copy
	sinks   []io.Writer
	hashes  []hash.Hash
	stripe  []byte
	parity  []byte
	written int64
	closed  bool
}

// NewWriter starts encoding a frame of frameSize bytes for object data id
// into len(shards) = k+m sinks, writing each shard's header immediately.
// Shard i's bytes go to shards[i]: 0..k-1 data, k..k+m-1 parity.
func NewWriter(id meta.VersionID, k, m int, frameSize int64, shards []io.Writer) (*Writer, error) {
	if err := validateParams(k, m); err != nil {
		return nil, err
	}
	if frameSize <= 0 {
		return nil, fmt.Errorf("ec: frame size %d; frames are never empty", frameSize)
	}
	if len(shards) != k+m {
		return nil, fmt.Errorf("ec: %d sinks for %d+%d shards", len(shards), k, m)
	}
	w := &Writer{
		geo:    geometry{k: k, m: m, sliceSize: sliceSize, frameSize: frameSize},
		sinks:  make([]io.Writer, k+m),
		hashes: make([]hash.Hash, k+m),
		stripe: make([]byte, 0, int64(k)*sliceSize),
		parity: make([]byte, int64(m)*sliceSize),
	}
	if m > 0 {
		rs, err := reedsolomon.New(k, m)
		if err != nil {
			return nil, fmt.Errorf("ec: %d+%d encoder: %w", k, m, err)
		}
		w.rs = rs
	}
	for i := range shards {
		w.hashes[i] = sha256.New()
		w.sinks[i] = io.MultiWriter(shards[i], w.hashes[i])
		hdr := encodeShard(shardHeader{
			id: id, index: i, k: k, m: m,
			sliceSize: sliceSize, frameSize: frameSize,
		})
		if _, err := w.sinks[i].Write(hdr); err != nil {
			return nil, fmt.Errorf("ec: writing shard %d header: %w", i, err)
		}
	}
	return w, nil
}

// Write buffers frame bytes, encoding and emitting each stripe as it
// fills. Writing past the declared frame size is refused.
func (w *Writer) Write(p []byte) (int, error) {
	if w.closed {
		return 0, fmt.Errorf("ec: write after Close")
	}
	if w.written+int64(len(p)) > w.geo.frameSize {
		return 0, fmt.Errorf("ec: frame exceeds its declared %d bytes", w.geo.frameSize)
	}
	full := int(w.geo.k) * int(w.geo.sliceSize)
	var n int
	for len(p) > 0 {
		take := full - len(w.stripe)
		if take > len(p) {
			take = len(p)
		}
		w.stripe = append(w.stripe, p[:take]...)
		p = p[take:]
		n += take
		w.written += int64(take)
		if len(w.stripe) == full {
			if err := w.encodeStripe(w.geo.sliceSize); err != nil {
				return n, err
			}
		}
	}
	return n, nil
}

// Close encodes the final short stripe (zero-padded to k equal slices —
// the frame knows its own end, so the padding is dead weight, never
// data). It does not close the underlying sinks.
func (w *Writer) Close() error {
	if w.closed {
		return fmt.Errorf("ec: Close called twice")
	}
	w.closed = true
	if w.written != w.geo.frameSize {
		return fmt.Errorf("ec: encoded %d of the declared %d frame bytes", w.written, w.geo.frameSize)
	}
	if len(w.stripe) > 0 {
		l := w.geo.lastLen()
		for int64(len(w.stripe)) < int64(w.geo.k)*l {
			w.stripe = append(w.stripe, 0)
		}
		if err := w.encodeStripe(l); err != nil {
			return err
		}
	}
	return nil
}

// encodeStripe splits the buffered stripe into k contiguous slices of
// length l, computes the m parity slices, and appends slice i to sink i.
func (w *Writer) encodeStripe(l int64) error {
	bufs := make([][]byte, w.geo.k+w.geo.m)
	for i := range w.geo.k {
		bufs[i] = w.stripe[int64(i)*l : int64(i+1)*l]
	}
	for i := range w.geo.m {
		bufs[w.geo.k+i] = w.parity[int64(i)*l : int64(i+1)*l]
	}
	if w.rs != nil {
		if err := w.rs.Encode(bufs); err != nil {
			return fmt.Errorf("ec: encoding stripe: %w", err)
		}
	}
	for i, b := range bufs {
		if _, err := w.sinks[i].Write(b); err != nil {
			return fmt.Errorf("ec: writing shard %d: %w", i, err)
		}
	}
	w.stripe = w.stripe[:0]
	return nil
}

// Checksums returns each shard's SHA-256, header included — the values
// the metadata commit records as VersionEntry.ShardChecksums. Valid after
// Close.
func (w *Writer) Checksums() [][]byte {
	sums := make([][]byte, len(w.hashes))
	for i, h := range w.hashes {
		sums[i] = h.Sum(nil)
	}
	return sums
}
