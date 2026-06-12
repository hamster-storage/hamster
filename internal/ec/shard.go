package ec

import (
	"encoding/binary"
	"fmt"
	"io"

	"google.golang.org/protobuf/encoding/protowire"

	"github.com/hamster-storage/hamster/internal/meta"
)

// A shard file is self-describing ([ADR-0026]): magic, a 4-byte
// little-endian header length, the versioned protobuf header, then the
// payload — one slice per stripe, appended in stripe order. Everything a
// reader needs to interpret the payload (and a scrubber needs to identify
// a stray file) is in the header; the shard's checksum lives in the
// object's metadata, never mirrored here.
//
// [ADR-0026]: ../../docs/adr/0026-stripe-and-shard-layout.md

var shardMagic = [4]byte{'H', 'M', 'S', '1'}

const shardFormatVersion = 1

// shardHeader describes one shard file.
//
// Protobuf fields (additive forever, per invariant 2):
//
//	1 format_version varint
//	2 data_id        bytes  the object version's data address
//	3 shard_index    varint 0..k-1 data, k..k+m-1 parity
//	4 data_shards    varint k
//	5 parity_shards  varint m
//	6 slice_size     varint payload bytes per stripe in this shard
//	7 frame_size     varint total framed bytes across the k data shards
type shardHeader struct {
	id        meta.VersionID
	index     int
	k, m      int
	sliceSize int64
	frameSize int64
}

// encodeShard renders the complete file front: magic, length, header.
func encodeShard(h shardHeader) []byte {
	var p []byte
	p = protowire.AppendTag(p, 1, protowire.VarintType)
	p = protowire.AppendVarint(p, shardFormatVersion)
	p = protowire.AppendTag(p, 2, protowire.BytesType)
	p = protowire.AppendBytes(p, h.id[:])
	p = protowire.AppendTag(p, 3, protowire.VarintType)
	p = protowire.AppendVarint(p, uint64(h.index))
	p = protowire.AppendTag(p, 4, protowire.VarintType)
	p = protowire.AppendVarint(p, uint64(h.k))
	p = protowire.AppendTag(p, 5, protowire.VarintType)
	p = protowire.AppendVarint(p, uint64(h.m))
	p = protowire.AppendTag(p, 6, protowire.VarintType)
	p = protowire.AppendVarint(p, uint64(h.sliceSize))
	p = protowire.AppendTag(p, 7, protowire.VarintType)
	p = protowire.AppendVarint(p, uint64(h.frameSize))

	out := make([]byte, 0, len(shardMagic)+4+len(p))
	out = append(out, shardMagic[:]...)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(p)))
	return append(out, p...)
}

// decodeShard parses a file front read from r, returning the header and
// the payload offset. Unknown protobuf fields are skipped: new fields are
// only ever added, and old readers must keep reading.
func decodeShard(r io.ReaderAt) (shardHeader, int64, error) {
	var h shardHeader
	var front [8]byte
	if err := readFull(r, front[:], 0, "shard front"); err != nil {
		return h, 0, err
	}
	if [4]byte(front[:4]) != shardMagic {
		return h, 0, fmt.Errorf("ec: bad magic: not a shard file")
	}
	hdrLen := int64(binary.LittleEndian.Uint32(front[4:]))
	if hdrLen > 1<<20 {
		return h, 0, fmt.Errorf("ec: corrupt shard: implausible %d-byte header", hdrLen)
	}
	p := make([]byte, hdrLen)
	if err := readFull(r, p, 8, "shard header"); err != nil {
		return h, 0, err
	}
	version := uint64(0)
	for len(p) > 0 {
		num, typ, n := protowire.ConsumeTag(p)
		if n < 0 {
			return h, 0, fmt.Errorf("ec: corrupt shard header: bad tag")
		}
		p = p[n:]
		switch {
		case num == 2 && typ == protowire.BytesType:
			b, n := protowire.ConsumeBytes(p)
			if n < 0 || len(b) != len(h.id) {
				return h, 0, fmt.Errorf("ec: corrupt shard header: bad data id")
			}
			h.id = meta.VersionID(b)
			p = p[n:]
		case typ == protowire.VarintType:
			v, n := protowire.ConsumeVarint(p)
			if n < 0 {
				return h, 0, fmt.Errorf("ec: corrupt shard header: bad field %d", num)
			}
			p = p[n:]
			switch num {
			case 1:
				version = v
			case 3:
				h.index = int(v)
			case 4:
				h.k = int(v)
			case 5:
				h.m = int(v)
			case 6:
				h.sliceSize = int64(v)
			case 7:
				h.frameSize = int64(v)
			}
		default:
			n := protowire.ConsumeFieldValue(num, typ, p)
			if n < 0 {
				return h, 0, fmt.Errorf("ec: corrupt shard header: bad field %d", num)
			}
			p = p[n:]
		}
	}
	if version == 0 {
		return h, 0, fmt.Errorf("ec: corrupt shard header: no format version")
	}
	if err := validateParams(h.k, h.m); err != nil {
		return h, 0, err
	}
	if h.index < 0 || h.index >= h.k+h.m || h.sliceSize <= 0 || h.frameSize <= 0 {
		return h, 0, fmt.Errorf("ec: corrupt shard header: shard %d of %d+%d, slice %d, frame %d",
			h.index, h.k, h.m, h.sliceSize, h.frameSize)
	}
	return h, 8 + hdrLen, nil
}

// validateParams checks a k+m pair against what Hamster writes: at least
// one data shard, parity only alongside k=1 striping or real spread, and
// the library's 256-shard ceiling. m=0 is the 1+0 profile — a single
// whole copy — never parity-less striping.
func validateParams(k, m int) error {
	if k < 1 || m < 0 || k+m > 256 {
		return fmt.Errorf("ec: invalid parameters %d+%d", k, m)
	}
	if m == 0 && k != 1 {
		return fmt.Errorf("ec: %d+0 is striping without parity; hamster does not write it", k)
	}
	return nil
}

// geometry is the stripe arithmetic shared by every path: how a frame of
// frameSize bytes lays out as stripes of k slices.
type geometry struct {
	k, m      int
	sliceSize int64
	frameSize int64
}

// fullStripes is how many stripes carry sliceSize-sized slices.
func (g geometry) fullStripes() int64 { return g.frameSize / (int64(g.k) * g.sliceSize) }

// lastLen is the final short stripe's slice length, 0 when the frame
// divides evenly.
func (g geometry) lastLen() int64 {
	rem := g.frameSize % (int64(g.k) * g.sliceSize)
	if rem == 0 {
		return 0
	}
	return (rem-1)/int64(g.k) + 1
}

// stripes is the total stripe count.
func (g geometry) stripes() int64 {
	n := g.fullStripes()
	if g.lastLen() > 0 {
		n++
	}
	return n
}

// stripeSlice is stripe si's slice length and its offset within a
// shard's payload.
func (g geometry) stripeSlice(si int64) (length, offset int64) {
	if si < g.fullStripes() {
		return g.sliceSize, si * g.sliceSize
	}
	return g.lastLen(), g.fullStripes() * g.sliceSize
}

// stripeData is how many frame bytes stripe si carries (the last stripe
// carries the remainder, less than its padded k×lastLen).
func (g geometry) stripeData(si int64) int64 {
	if si < g.fullStripes() {
		return int64(g.k) * g.sliceSize
	}
	return g.frameSize - g.fullStripes()*int64(g.k)*g.sliceSize
}

// readFull reads exactly len(p) bytes at off, treating a short read as
// the error it is.
func readFull(r io.ReaderAt, p []byte, off int64, what string) error {
	if n, err := r.ReadAt(p, off); n < len(p) {
		if err == nil || err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return fmt.Errorf("ec: reading %s: %w", what, err)
	}
	return nil
}

// Header is a shard file's self-description (ADR-0026 decision 3),
// exported for callers that locate shard bytes before they can build a
// Reader — the network read coordinator decodes headers first to learn
// the geometry its slice fetches must follow.
type Header struct {
	ID            meta.VersionID
	Index         int
	Data, Parity  int
	SliceSize     int64
	FrameSize     int64
	PayloadOffset int64 // where the first slice begins
}

// ReadHeader decodes a shard file's front. A 512-byte prefix is always
// sufficient (headers are tens of bytes; the length field is validated).
func ReadHeader(r io.ReaderAt) (Header, error) {
	h, payload, err := decodeShard(r)
	if err != nil {
		return Header{}, err
	}
	return Header{
		ID: h.id, Index: h.index, Data: h.k, Parity: h.m,
		SliceSize: h.sliceSize, FrameSize: h.frameSize,
		PayloadOffset: payload,
	}, nil
}
