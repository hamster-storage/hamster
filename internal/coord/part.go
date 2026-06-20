package coord

import (
	"fmt"

	"github.com/hamster-storage/hamster/internal/meta"
)

// Multipart on the erasure-coded data path (ADR-0038). Each part is encoded
// independently — its own placement, its own k+m profile chosen for the part's
// size, its own per-part DEK — and made durable as k+m shards exactly like a
// whole PUT, then committed as an UploadPart row under the upload. A part is
// durable and independently readable the moment its upload acknowledges, before
// CompleteMultipartUpload assembles the parts into a version. This reuses the
// whole-PUT streaming machinery wholesale: only the metadata proposal at the
// linearization point (UploadPart, not PutObject) and the acknowledged result
// (a part's data address, not a committed version) differ.

// PutPart stores one whole in-memory part — the convenience entry point over
// the streaming machinery for callers holding the part bytes. done fires once
// on the loop. The body slice must not be mutated until done fires.
func (c *Coordinator) PutPart(bucket, key string, uploadID meta.VersionID, partNumber uint32, body []byte, done func(PartResult, error)) {
	op := c.beginPart(bucket, key, uploadID, partNumber, int64(len(body)), nil, done)
	if op == nil {
		return
	}
	op.Feed(body)
	op.FeedEOF()
}

// PutPartStream begins a streaming UploadPart of size bytes, fed under the same
// backpressure window as a streaming PUT. want fires on the loop when the
// coordinator can accept another chunk; done fires once on the loop. A nil
// handle means setup failed and done has already fired.
func (c *Coordinator) PutPartStream(bucket, key string, uploadID meta.VersionID, partNumber uint32, size int64, want func(), done func(PartResult, error)) *PutHandle {
	op := c.beginPart(bucket, key, uploadID, partNumber, size, want, done)
	if op == nil {
		return nil
	}
	op.replenish()
	return &PutHandle{op}
}

// beginPart opens a part write over the whole-PUT machinery (beginPut), then
// swaps the commit strategy to UploadPart. The PartResult callback is adapted
// onto beginPut's PutResult callback: a part's data address is the minted ID
// the shards were written under (PutResult.VersionID).
func (c *Coordinator) beginPart(bucket, key string, uploadID meta.VersionID, partNumber uint32, size int64, want func(), done func(PartResult, error)) *putOp {
	adapt := func(r PutResult, err error) {
		done(PartResult{DataID: r.VersionID, ETag: r.ETag, Durable: r.Durable}, err)
	}
	// A part carries no object-lock or content-type metadata of its own — those
	// belong to the completed object, captured at CreateMultipartUpload.
	op := c.beginPut(bucket, key, size, PutOptions{}, want, adapt)
	if op == nil {
		return nil
	}
	op.commit = func() { op.commitUploadPart(uploadID, partNumber) }
	return op
}

// commitUploadPart proposes the part's UploadPart row — the linearization point
// for a part — and acknowledges. The part's data address is its minted DataID
// (op.vid), the same ID the shards were written under and the DEK-wrap nonce;
// the EC geometry committed here is what a GET later uses to fetch and decode
// the part from its PartRef. A re-uploaded part number displaces the prior
// part's shards, which become reclaimable orphans (same posture as a replaced
// whole object on this path).
func (op *putOp) commitUploadPart(uploadID meta.VersionID, partNumber uint32) {
	op.c.cfg.Raft.Propose(meta.UploadPart{
		ProposedAtUnixMS: op.atMS,
		Bucket:           op.bucket,
		Key:              op.key,
		UploadID:         uploadID,
		PartNumber:       partNumber,
		DataID:           op.vid,
		Size:             op.size,
		ETag:             op.etag,
		Checksum:         op.objSum,
		Partition:        op.partition,
		ECDataShards:     uint32(op.k),
		ECParityShards:   uint32(op.m),
		ShardChecksums:   op.ecw.Checksums(),
		EncAlgorithm:     op.encAlg,
		WrappedDEK:       op.wrappedDEK,
		KEKFingerprint:   op.kekFP,
	}, func(_ any, err error) {
		if err != nil {
			// Durable shards without a committed part row are orphans; reclaim
			// what answers, the rest is scan-discoverable garbage.
			op.cleanup()
			op.done(PutResult{}, fmt.Errorf("coord: upload-part commit: %w", err))
			return
		}
		op.done(PutResult{VersionID: op.vid, ETag: op.etag, Durable: op.successes}, nil)
	})
}

// partEntry synthesizes a single-object VersionEntry from a part's PartRef so
// the part can be read through the ordinary whole-object GET path (getOp): each
// part has its own data address, placement, geometry, and DEK, exactly the
// facts GetEntry needs. The synthetic entry carries no Parts, so reading it
// takes the single-object branch — no recursion.
func partEntry(p meta.PartRef) meta.VersionEntry {
	return meta.VersionEntry{
		Kind:           meta.KindObject,
		DataID:         p.DataID,
		Size:           p.Size,
		Partition:      p.Partition,
		ECDataShards:   p.ECDataShards,
		ECParityShards: p.ECParityShards,
		ShardChecksums: p.ShardChecksums,
		EncAlgorithm:   p.EncAlgorithm,
		WrappedDEK:     p.WrappedDEK,
		KEKFingerprint: p.KEKFingerprint,
	}
}

// getMultipart serves the plaintext range [off, end) of a multipart object by
// reading each covering part's sub-range and concatenating them in order. Parts
// are laid out contiguously in object-byte order, so the range maps to a
// contiguous run of part sub-ranges; a Range read fetches only the parts it
// overlaps. Reads run one part at a time on the loop — deterministic, and a part
// stream's window bounds memory to a single part's covering slices, never the
// whole object. done fires once on the loop.
func (c *Coordinator) getMultipart(entry meta.VersionEntry, off, end int64, done func([]byte, error)) {
	type seg struct {
		idx         int
		off, length int64 // sub-range within part idx
	}
	var segs []seg
	var partStart int64
	for i, p := range entry.Parts {
		ps, pe := partStart, partStart+p.Size
		partStart = pe
		lo, hi := max(off, ps), min(end, pe)
		if lo < hi {
			segs = append(segs, seg{idx: i, off: lo - ps, length: hi - lo})
		}
	}
	out := make([]byte, 0, end-off)
	if len(segs) == 0 {
		done(out, nil)
		return
	}
	var readNext func(j int)
	readNext = func(j int) {
		if j == len(segs) {
			done(out, nil)
			return
		}
		s := segs[j]
		c.GetEntry(partEntry(entry.Parts[s.idx]), s.off, s.length, func(b []byte, err error) {
			if err != nil {
				done(nil, err)
				return
			}
			out = append(out, b...)
			readNext(j + 1)
		})
	}
	readNext(0)
}
