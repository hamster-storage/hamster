// Package stream implements the framed object stream of
// docs/DATA-STREAM.md: the byte format between the S3 gateway and erasure
// coding. Object bytes are split into fixed-size chunks, each chunk is
// (optionally) transformed, and the chunks are framed with a header and a
// trailer index, so a read can seek to any byte range and touch only the
// chunks the range covers.
//
// Two frame shapes ship today: the identity frame (chunking, framing, and
// per-chunk CRC-32C integrity) and the encrypted frame (each chunk sealed
// with AES-256-GCM under a caller-supplied per-object DEK, the GCM tag
// joining the CRC, ADR-0021). The compression transform is still reserved
// as a header flag but not wired — a frame using it is refused. The frame
// is always present even with no transforms: one read path, no special
// cases.
//
// Framing is pure computation — no clocks, no randomness, no I/O of its
// own — so it runs under the simulation harness with no seam.
package stream

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math"
)

// DefaultChunkSize is the nominal plaintext bytes per chunk. Every frame
// records its own chunk size in the header, so retuning the default
// changes new writes only and confuses nothing.
const DefaultChunkSize = 1 << 20

// formatVersion is the frame format this code writes. Readers accept
// exactly this version today; future versions are additive per the
// versioned-formats invariant, and a frame from a newer binary is refused
// rather than misread.
const formatVersion = 1

// Header flags. flagEncrypted is wired (AES-256-GCM per chunk);
// flagCompressed is designed (docs/DATA-STREAM.md) but not yet
// implemented, so a frame setting it is refused. supportedFlags is the
// set this binary can read — any bit outside it means a frame from a
// newer hamster, refused rather than misread.
const (
	flagCompressed = 1 << 0
	flagEncrypted  = 1 << 1

	supportedFlags = flagEncrypted
)

var magic = [4]byte{'H', 'M', 'F', '1'}

// crcTable is CRC-32C (Castagnoli), the per-chunk integrity checksum.
var crcTable = crc32.MakeTable(crc32.Castagnoli)

// maxHeaderLen bounds a version-1 header: the magic and four varints.
const maxHeaderLen = 4 + 4*binary.MaxVarintLen64

// header is the parsed fixed front of a frame.
type header struct {
	flags         uint64
	chunkSize     int64
	plaintextSize int64
	len           int64 // encoded bytes the header occupied
}

// appendHeader encodes a version-1 header. flags is 0 for an identity
// frame or flagEncrypted for an AES-256-GCM frame.
func appendHeader(b []byte, flags uint64, chunkSize, plaintextSize int64) []byte {
	b = append(b, magic[:]...)
	b = binary.AppendUvarint(b, formatVersion)
	b = binary.AppendUvarint(b, flags)
	b = binary.AppendUvarint(b, uint64(chunkSize))
	b = binary.AppendUvarint(b, uint64(plaintextSize))
	return b
}

// parseHeader decodes and validates a frame's front bytes.
func parseHeader(b []byte) (header, error) {
	var h header
	if len(b) < len(magic) || [4]byte(b[:4]) != magic {
		return h, fmt.Errorf("stream: bad magic: not a framed object")
	}
	rest := b[len(magic):]
	next := func(what string) (uint64, error) {
		v, n := binary.Uvarint(rest)
		if n <= 0 {
			return 0, fmt.Errorf("stream: corrupt header: bad %s", what)
		}
		rest = rest[n:]
		return v, nil
	}
	version, err := next("format version")
	if err != nil {
		return h, err
	}
	if version != formatVersion {
		return h, fmt.Errorf("stream: frame format version %d (this binary reads %d): written by a newer hamster", version, formatVersion)
	}
	if h.flags, err = next("flags"); err != nil {
		return h, err
	}
	if h.flags&^supportedFlags != 0 {
		return h, fmt.Errorf("stream: frame uses transforms this binary does not support (flags %#x)", h.flags)
	}
	chunkSize, err := next("chunk size")
	if err != nil {
		return h, err
	}
	plaintextSize, err := next("plaintext size")
	if err != nil {
		return h, err
	}
	if chunkSize > math.MaxInt64 || plaintextSize > math.MaxInt64 {
		return h, fmt.Errorf("stream: corrupt header: implausible sizes")
	}
	h.chunkSize, h.plaintextSize = int64(chunkSize), int64(plaintextSize)
	if h.chunkSize == 0 && h.plaintextSize > 0 {
		return h, fmt.Errorf("stream: corrupt header: zero chunk size with %d plaintext bytes", h.plaintextSize)
	}
	h.len = int64(len(b) - len(rest))
	return h, nil
}

// FrameSize is the exact framed size of a plaintextSize-byte object at
// chunkSize, computable before any byte streams: an identity or encrypted
// frame is fully determined by its dimensions. The erasure-coded write
// path needs it up front, because every shard's header states the frame
// size. An encrypted frame adds one GCM tag (gcmTagLen) of stored bytes
// per chunk; pass encrypted=true for it. (A compressed frame's size is not
// knowable in advance; that is the compression transform's problem when it
// arrives.)
func FrameSize(plaintextSize int64, chunkSize int, encrypted bool) int64 {
	tag, flags := transformOverhead(encrypted)
	size := int64(len(appendHeader(make([]byte, 0, maxHeaderLen), flags, int64(chunkSize), plaintextSize)))
	n := chunkCount(plaintextSize, int64(chunkSize))
	size += plaintextSize + tag*n
	if n > 0 {
		last := plaintextSize - (n-1)*int64(chunkSize)
		size += (n - 1) * int64(uvarintLen(uint64(int64(chunkSize)+tag)))
		size += int64(uvarintLen(uint64(last + tag)))
	}
	return size + 4*n + 4
}

// transformOverhead reports a frame's per-chunk stored overhead and its
// header flags: zero and identity, or one GCM tag and flagEncrypted.
func transformOverhead(encrypted bool) (tag int64, flags uint64) {
	if encrypted {
		return gcmTagLen, flagEncrypted
	}
	return 0, 0
}

// A Range is a byte range within a frame: [Off, Off+Len).
type Range struct {
	Off, Len int64
}

// Cover reports which frame byte ranges a Reader touches to serve the
// plaintext range [off, off+length): the header, the covering chunks, and
// the trailer. Like FrameSize, it is exact arithmetic over the frame's
// dimensions — a network read coordinator prefetches exactly these ranges
// and the Reader finds every byte it asks for. Pass encrypted=true to
// match an AES-256-GCM frame, whose stored chunks each carry a GCM tag.
// Ranges are sorted, non-overlapping, and merged when adjacent; an empty
// or out-of-bounds request still returns the header and trailer, which the
// Reader always reads.
func Cover(plaintextSize int64, chunkSize int, off, length int64, encrypted bool) []Range {
	tag, flags := transformOverhead(encrypted)
	stride := int64(chunkSize) + tag // stored bytes per full chunk
	n := chunkCount(plaintextSize, int64(chunkSize))
	headerLen := int64(len(appendHeader(make([]byte, 0, maxHeaderLen), flags, int64(chunkSize), plaintextSize)))
	frameSize := FrameSize(plaintextSize, chunkSize, encrypted)
	trailerStart := headerLen + plaintextSize + tag*n
	// The reader's header read is up to maxHeaderLen, not the exact
	// header; cover what it reads, not what is strictly there.
	headRead := min(int64(maxHeaderLen), frameSize)

	// Clamp to the plaintext; a degenerate request covers no body bytes.
	if off < 0 {
		off = 0
	}
	end := min(off+length, plaintextSize)

	out := []Range{{Off: 0, Len: headRead}}
	if off < end {
		first := off / int64(chunkSize)
		last := (end - 1) / int64(chunkSize)
		bodyStart := headerLen + first*stride
		bodyEnd := min(headerLen+(last+1)*stride, trailerStart)
		out = appendRange(out, Range{Off: bodyStart, Len: bodyEnd - bodyStart})
	}
	return appendRange(out, Range{Off: trailerStart, Len: frameSize - trailerStart})
}

// appendRange appends r, merging it into the previous range when they
// touch or overlap.
func appendRange(rs []Range, r Range) []Range {
	prev := &rs[len(rs)-1]
	if r.Off <= prev.Off+prev.Len {
		prev.Len = max(prev.Len, r.Off+r.Len-prev.Off)
		return rs
	}
	return append(rs, r)
}

// uvarintLen is the encoded size of v as a uvarint.
func uvarintLen(v uint64) int {
	n := 1
	for v >= 0x80 {
		v >>= 7
		n++
	}
	return n
}

// chunkCount is how many chunks a plaintext of size bytes splits into.
func chunkCount(size, chunkSize int64) int64 {
	if size == 0 {
		return 0
	}
	return (size-1)/chunkSize + 1
}

// readAtFull reads exactly len(p) bytes at off, treating a short read as
// the error it is for a frame whose size the caller declared.
func readAtFull(r io.ReaderAt, p []byte, off int64, what string) error {
	if n, err := r.ReadAt(p, off); n < len(p) {
		if err == nil || err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return fmt.Errorf("stream: reading %s: %w", what, err)
	}
	return nil
}
