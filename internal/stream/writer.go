package stream

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math"
)

// Writer frames one object's bytes onto an underlying writer. The caller
// declares the plaintext size up front (S3 PUTs always carry it — see
// docs/S3-API.md), writes exactly that many bytes, and Closes to emit the
// trailer. Memory stays bounded: at most one chunk is buffered, plus the
// trailer index (a few bytes per chunk).
type Writer struct {
	w         io.Writer
	chunkSize int64
	declared  int64
	written   int64
	buf       []byte
	sealBuf   []byte // scratch for one chunk's ciphertext (encrypted frames)
	crypter   *chunkCrypter
	lengths   []uint64
	crcs      []uint32
	frameSize int64
	closed    bool
}

// NewWriter starts a frame for plaintextSize object bytes, writing the
// header immediately. chunkSize is recorded in the header; use
// DefaultChunkSize unless measuring says otherwise. dek selects the
// transform: nil writes an identity frame, a 32-byte ([DEKLen]) key writes
// an AES-256-GCM frame. The DEK must be unique to this object (ADR-0021) —
// the chunk index is the nonce, so reusing a DEK reuses nonces.
func NewWriter(w io.Writer, plaintextSize int64, chunkSize int, dek []byte) (*Writer, error) {
	if plaintextSize < 0 {
		return nil, fmt.Errorf("stream: negative plaintext size %d", plaintextSize)
	}
	if chunkSize <= 0 {
		return nil, fmt.Errorf("stream: chunk size must be positive, not %d", chunkSize)
	}
	var crypter *chunkCrypter
	flags := uint64(0)
	if dek != nil {
		var err error
		if crypter, err = newChunkCrypter(dek); err != nil {
			return nil, err
		}
		flags = flagEncrypted
	}
	hdr := appendHeader(make([]byte, 0, maxHeaderLen), flags, int64(chunkSize), plaintextSize)
	if _, err := w.Write(hdr); err != nil {
		return nil, fmt.Errorf("stream: writing header: %w", err)
	}
	bufCap := int64(chunkSize)
	if plaintextSize < bufCap {
		bufCap = plaintextSize
	}
	wr := &Writer{
		w:         w,
		chunkSize: int64(chunkSize),
		declared:  plaintextSize,
		buf:       make([]byte, 0, bufCap),
		crypter:   crypter,
		frameSize: int64(len(hdr)),
	}
	if crypter != nil {
		wr.sealBuf = make([]byte, 0, bufCap+gcmTagLen)
	}
	return wr, nil
}

// Write buffers p into chunks, emitting each chunk as it fills. Writing
// past the declared plaintext size is refused: the header already
// promised the size, and a frame must never lie about its contents.
func (w *Writer) Write(p []byte) (int, error) {
	if w.closed {
		return 0, fmt.Errorf("stream: write after Close")
	}
	if w.written+int64(len(p)) > w.declared {
		return 0, fmt.Errorf("stream: object exceeds its declared %d bytes", w.declared)
	}
	var n int
	for len(p) > 0 {
		take := int(w.chunkSize) - len(w.buf)
		if take > len(p) {
			take = len(p)
		}
		w.buf = append(w.buf, p[:take]...)
		p = p[take:]
		n += take
		w.written += int64(take)
		if int64(len(w.buf)) == w.chunkSize {
			if err := w.flush(); err != nil {
				return n, err
			}
		}
	}
	return n, nil
}

// flush emits the buffered chunk: its stored bytes now (the plaintext for
// an identity frame, the AES-256-GCM ciphertext-plus-tag for an encrypted
// one), its stored length and CRC-32C remembered for the trailer. The CRC
// covers the stored bytes — what lands on disk — so it guards the
// ciphertext; the GCM tag adds authentication on top.
func (w *Writer) flush() error {
	i := int64(len(w.lengths))
	stored := w.buf
	if w.crypter != nil {
		w.sealBuf = w.crypter.seal(w.sealBuf[:0], w.buf, i)
		stored = w.sealBuf
	}
	if _, err := w.w.Write(stored); err != nil {
		return fmt.Errorf("stream: writing chunk %d: %w", i, err)
	}
	w.lengths = append(w.lengths, uint64(len(stored)))
	w.crcs = append(w.crcs, crc32.Checksum(stored, crcTable))
	w.frameSize += int64(len(stored))
	w.buf = w.buf[:0]
	return nil
}

// Close flushes the final chunk and writes the trailer. It does not close
// the underlying writer. Close fails if fewer bytes arrived than were
// declared — the header already promised them.
func (w *Writer) Close() error {
	if w.closed {
		return fmt.Errorf("stream: Close called twice")
	}
	w.closed = true
	if w.written != w.declared {
		return fmt.Errorf("stream: wrote %d of the declared %d bytes", w.written, w.declared)
	}
	if len(w.buf) > 0 {
		if err := w.flush(); err != nil {
			return err
		}
	}
	trailer := make([]byte, 0, len(w.lengths)*(binary.MaxVarintLen64+4)+4)
	for _, l := range w.lengths {
		trailer = binary.AppendUvarint(trailer, l)
	}
	for _, c := range w.crcs {
		trailer = binary.LittleEndian.AppendUint32(trailer, c)
	}
	if len(trailer) > math.MaxUint32 {
		return fmt.Errorf("stream: trailer of %d bytes overflows its length field", len(trailer))
	}
	trailer = binary.LittleEndian.AppendUint32(trailer, uint32(len(trailer)))
	if _, err := w.w.Write(trailer); err != nil {
		return fmt.Errorf("stream: writing trailer: %w", err)
	}
	w.frameSize += int64(len(trailer))
	return nil
}

// FrameSize reports the total framed bytes emitted. Valid after Close;
// the data path records it so reads know the frame's extent.
func (w *Writer) FrameSize() int64 { return w.frameSize }
