package ec

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"hash"
	"io"
)

// Reconstruct rebuilds missing shards from the survivors — repair's
// rebuild task (docs/ERASURE-CODING.md). shards holds the survivors (nil
// where a shard is gone); rebuild names the targets (a non-nil writer at
// every index to rebuild, which must be nil in shards — a corrupt shard
// is treated as missing). checksums, when non-nil, are the expected
// SHA-256 of every shard (VersionEntry.ShardChecksums): each survivor
// used is verified against its entry as it is read, and each rebuilt
// shard against its own when one is recorded — repair must never launder
// corruption into fresh shards.
//
// Outputs stream as stripes are rebuilt, but a source's checksum can only
// fail at the end of the pass: the caller must stage rebuilt shards and
// commit them only after Reconstruct returns nil.
func Reconstruct(shards []io.ReaderAt, checksums [][]byte, rebuild []io.Writer) error {
	if len(rebuild) != len(shards) {
		return fmt.Errorf("ec: %d rebuild slots for %d shards", len(rebuild), len(shards))
	}
	if checksums != nil && len(checksums) != len(shards) {
		return fmt.Errorf("ec: %d checksums for %d shards", len(checksums), len(shards))
	}
	for i := range shards {
		if rebuild[i] != nil && shards[i] != nil {
			return fmt.Errorf("ec: shard %d is both source and rebuild target; treat a bad shard as missing", i)
		}
	}

	// Open the survivors exactly as a reader would: parse, cross-check,
	// demand k of them.
	r, err := NewReader(shards)
	if err != nil {
		return err
	}
	if r.rs == nil {
		return fmt.Errorf("ec: a 1+0 object has no redundancy to rebuild from")
	}
	geo := r.geo

	// Hash every survivor we read and every shard we rebuild; headers
	// are part of the shard file and its checksum.
	hashes := make([]hash.Hash, len(shards))
	sinks := make([]io.Writer, len(shards))
	for i, w := range rebuild {
		if shards[i] != nil {
			hashes[i] = sha256.New()
			hdr := make([]byte, r.offs[i])
			if err := readFull(shards[i], hdr, 0, fmt.Sprintf("shard %d header", i)); err != nil {
				return err
			}
			hashes[i].Write(hdr)
			continue
		}
		if w == nil {
			continue
		}
		hashes[i] = sha256.New()
		sinks[i] = io.MultiWriter(w, hashes[i])
		hdr := encodeShard(shardHeader{
			id: r.id, index: i, k: geo.k, m: geo.m,
			sliceSize: geo.sliceSize, frameSize: geo.frameSize,
		})
		if _, err := sinks[i].Write(hdr); err != nil {
			return fmt.Errorf("ec: writing rebuilt shard %d header: %w", i, err)
		}
	}

	// Stripe by stripe: read every survivor's slice (the whole file gets
	// hashed for its checksum anyway), reconstruct the rest, emit the
	// targets.
	for si := range geo.stripes() {
		l, sliceOff := geo.stripeSlice(si)
		slices := make([][]byte, len(shards))
		for i, s := range shards {
			if s == nil {
				continue
			}
			b := make([]byte, l)
			if err := readFull(s, b, r.offs[i]+sliceOff, fmt.Sprintf("shard %d stripe %d", i, si)); err != nil {
				return err
			}
			hashes[i].Write(b)
			slices[i] = b
		}
		if err := r.rs.Reconstruct(slices); err != nil {
			return fmt.Errorf("ec: reconstructing stripe %d: %w", si, err)
		}
		for i, sink := range sinks {
			if sink == nil {
				continue
			}
			if _, err := sink.Write(slices[i]); err != nil {
				return fmt.Errorf("ec: writing rebuilt shard %d: %w", i, err)
			}
		}
	}

	// The verdict: every survivor read and every shard rebuilt must match
	// the checksum metadata recorded at write time.
	if checksums != nil {
		for i, h := range hashes {
			if h == nil || len(checksums[i]) == 0 {
				continue
			}
			if !bytes.Equal(h.Sum(nil), checksums[i]) {
				if shards[i] != nil {
					return fmt.Errorf("ec: shard %d failed its checksum: corrupt source, rebuild from others", i)
				}
				return fmt.Errorf("ec: rebuilt shard %d failed its checksum", i)
			}
		}
	}
	return nil
}
