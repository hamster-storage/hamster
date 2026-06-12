package stream

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
)

// Reader gives CRC-verified random access to one object's plaintext. The
// frame arrives through an io.ReaderAt and its total size — the data path
// knows both — and Reader does not care whether the bytes live in one
// file or are reassembled from erasure-coded shards.
type Reader struct {
	r       io.ReaderAt
	size    int64   // plaintext bytes
	chunk   int64   // plaintext bytes per chunk (last may be short)
	offsets []int64 // offsets[i] is chunk i's frame offset; offsets[n] the end
	crcs    []uint32
}

// NewReader parses and cross-checks a frame's header and trailer. Every
// structural lie a corrupt frame can tell — wrong magic, impossible
// sizes, an index that does not match the bytes — fails here; chunk
// content corruption fails at read time via the per-chunk CRC.
func NewReader(r io.ReaderAt, frameSize int64) (*Reader, error) {
	hdrLen := int64(maxHeaderLen)
	if frameSize < hdrLen {
		hdrLen = frameSize
	}
	front := make([]byte, hdrLen)
	if err := readAtFull(r, front, 0, "header"); err != nil {
		return nil, err
	}
	h, err := parseHeader(front)
	if err != nil {
		return nil, err
	}

	// The trailer: found from the end, sized to the chunk count the
	// header implies. Validate the count against the real trailer length
	// before allocating anything sized by it.
	n := chunkCount(h.plaintextSize, h.chunkSize)
	var lenField [4]byte
	if frameSize < h.len+4 {
		return nil, fmt.Errorf("stream: frame of %d bytes is too short for its own header", frameSize)
	}
	if err := readAtFull(r, lenField[:], frameSize-4, "trailer length"); err != nil {
		return nil, err
	}
	trailerLen := int64(binary.LittleEndian.Uint32(lenField[:]))
	if h.len+trailerLen+4 > frameSize {
		return nil, fmt.Errorf("stream: corrupt frame: %d-byte trailer does not fit", trailerLen)
	}
	if n > trailerLen/5 { // each chunk costs ≥ 1 length byte + 4 CRC bytes
		return nil, fmt.Errorf("stream: corrupt frame: trailer of %d bytes cannot index %d chunks", trailerLen, n)
	}
	trailer := make([]byte, trailerLen)
	if err := readAtFull(r, trailer, frameSize-4-trailerLen, "trailer"); err != nil {
		return nil, err
	}

	fr := &Reader{
		r:       r,
		size:    h.plaintextSize,
		chunk:   h.chunkSize,
		offsets: make([]int64, n+1),
		crcs:    make([]uint32, n),
	}
	fr.offsets[0] = h.len
	for i := int64(0); i < n; i++ {
		stored, used := binary.Uvarint(trailer)
		if used <= 0 {
			return nil, fmt.Errorf("stream: corrupt trailer: bad length for chunk %d", i)
		}
		trailer = trailer[used:]
		// Identity frames only (flags are zero, enforced by parseHeader):
		// every stored chunk is exactly its plaintext length.
		if int64(stored) != fr.plainLen(i) {
			return nil, fmt.Errorf("stream: corrupt frame: chunk %d stores %d bytes, want %d", i, stored, fr.plainLen(i))
		}
		fr.offsets[i+1] = fr.offsets[i] + int64(stored)
	}
	if int64(len(trailer)) != 4*n {
		return nil, fmt.Errorf("stream: corrupt trailer: %d bytes left for %d chunk CRCs", len(trailer), n)
	}
	for i := int64(0); i < n; i++ {
		fr.crcs[i] = binary.LittleEndian.Uint32(trailer[4*i:])
	}
	if fr.offsets[n] != frameSize-4-trailerLen {
		return nil, fmt.Errorf("stream: corrupt frame: chunks end at %d, trailer starts at %d", fr.offsets[n], frameSize-4-trailerLen)
	}
	return fr, nil
}

// Size reports the object's plaintext size.
func (fr *Reader) Size() int64 { return fr.size }

// plainLen is chunk i's plaintext length: the chunk size, short for the
// last chunk.
func (fr *Reader) plainLen(i int64) int64 {
	if rest := fr.size - i*fr.chunk; rest < fr.chunk {
		return rest
	}
	return fr.chunk
}

// ReadAt reads plaintext bytes at off, per io.ReaderAt. Only the chunks
// the range touches are read, each verified against its CRC-32C — a
// corrupted chunk fails the read loudly rather than serving garbage.
func (fr *Reader) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("stream: negative read offset %d", off)
	}
	var n int
	for n < len(p) && off < fr.size {
		i := off / fr.chunk
		chunk, err := fr.readChunk(i)
		if err != nil {
			return n, err
		}
		c := copy(p[n:], chunk[off-i*fr.chunk:])
		n += c
		off += int64(c)
	}
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

// readChunk reads stored chunk i and verifies its CRC. With no transforms
// the stored bytes are the plaintext.
func (fr *Reader) readChunk(i int64) ([]byte, error) {
	stored := make([]byte, fr.offsets[i+1]-fr.offsets[i])
	if err := readAtFull(fr.r, stored, fr.offsets[i], fmt.Sprintf("chunk %d", i)); err != nil {
		return nil, err
	}
	if crc32.Checksum(stored, crcTable) != fr.crcs[i] {
		return nil, fmt.Errorf("stream: chunk %d failed its CRC: frame corrupted", i)
	}
	return stored, nil
}
