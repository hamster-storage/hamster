package meta

import (
	"bytes"
	"maps"
	"slices"
)

// Multipart uploads (docs/METADATA.md, docs/S3-API.md). Upload state lives
// under the u/ prefix: one UploadRecord per upload, one PartRecord per
// uploaded part. Parts are data-plane facts like any PUT — the bytes are
// durable before the row commits. CompleteMultipartUpload is one
// transaction: it validates the client's part list against the stored
// rows, commits the assembled version entry exactly like a PUT (same
// monotonicity bump, same null-version replacement, same c/ row), and
// deletes the upload state. Until then nothing under u/ is visible to any
// object read or listing.

// CreateMultipartUpload starts an upload. The ID is minted by the gateway,
// like a version ID; the content type and user metadata are captured now
// and applied to the completed object.
type CreateMultipartUpload struct {
	ProposedAtUnixMS int64
	Bucket           string
	Key              string
	UploadID         VersionID
	ContentType      string
	UserMetadata     map[string]string
}

// UploadPart commits one part's metadata. The part's data is already
// durable under DataID when this proposal is made (the metadata commit is
// the linearization point, exactly as for PutObject).
type UploadPart struct {
	ProposedAtUnixMS int64
	Bucket           string
	Key              string
	UploadID         VersionID
	PartNumber       uint32
	DataID           VersionID
	Size             int64
	ETag             []byte
	Checksum         []byte
}

// UploadPartResult reports the data address displaced when a part number
// is uploaded twice — the caller reclaims that blob. Zero on first upload.
type UploadPartResult struct {
	ReplacedDataID VersionID
}

// CompletedPart is one row of the client's CompleteMultipartUpload part
// list: the part number and the ETag the client believes it uploaded.
type CompletedPart struct {
	PartNumber uint32
	ETag       []byte
}

// CompleteMultipartUpload assembles an upload's parts into a committed
// object version. Parts is the client's list, in the client's order —
// apply validates it (ascending, every part present with a matching ETag,
// minimum size for all but the last) rather than trusting it. ETag is the
// composite MD5 the gateway computed from that list (a data-plane fact,
// like every PUT ETag); VersionID is freshly minted and subject to the
// same ordering bump as any PUT.
type CompleteMultipartUpload struct {
	ProposedAtUnixMS int64
	Bucket           string
	Key              string
	UploadID         VersionID
	VersionID        VersionID
	ETag             []byte
	Parts            []CompletedPart
}

// CompleteResult reports the committed version and the data addresses of
// uploaded parts the completion did not use — the caller reclaims those.
type CompleteResult struct {
	VersionID        VersionID
	DiscardedDataIDs []VersionID
}

// AbortMultipartUpload discards an upload and all of its parts.
type AbortMultipartUpload struct {
	ProposedAtUnixMS int64
	Bucket           string
	Key              string
	UploadID         VersionID
}

// AbortResult carries the parts' data addresses for the caller to reclaim.
type AbortResult struct {
	PartDataIDs []VersionID
}

// ApplyCreateMultipartUpload starts a multipart upload.
func (s *Store) ApplyCreateMultipartUpload(p CreateMultipartUpload) (err error) {
	defer s.txn(&err)()
	if _, ok := s.GetBucket(p.Bucket); !ok {
		return ErrNoSuchBucket
	}
	if err := validateObjectKey(p.Key); err != nil {
		return err
	}
	row := uploadRowKey(p.Bucket, p.Key, p.UploadID)
	if _, ok := s.kv.get(row); ok {
		return ErrUploadExists // a minted-ID collision; effectively unreachable
	}
	s.kv.set(row, UploadRecord{
		FormatVersion: currentFormatVersion,
		UploadID:      p.UploadID,
		CreatedUnixMS: p.ProposedAtUnixMS,
		ContentType:   p.ContentType,
		UserMetadata:  maps.Clone(p.UserMetadata),
	})
	return nil
}

// ApplyUploadPart commits one part row, replacing any prior upload of the
// same part number.
func (s *Store) ApplyUploadPart(p UploadPart) (res UploadPartResult, err error) {
	defer s.txn(&err)()
	if _, ok := s.GetBucket(p.Bucket); !ok {
		return UploadPartResult{}, ErrNoSuchBucket
	}
	if err := validateObjectKey(p.Key); err != nil {
		return UploadPartResult{}, err
	}
	if p.PartNumber < 1 || p.PartNumber > MaxPartNumber {
		return UploadPartResult{}, ErrInvalidPartNumber
	}
	if _, ok := s.kv.get(uploadRowKey(p.Bucket, p.Key, p.UploadID)); !ok {
		return UploadPartResult{}, ErrNoSuchUpload
	}
	row := partRowKey(p.Bucket, p.Key, p.UploadID, p.PartNumber)
	if prior, ok := s.kv.get(row); ok {
		res.ReplacedDataID = prior.(PartRecord).DataID
	}
	s.kv.set(row, PartRecord{
		FormatVersion:  currentFormatVersion,
		PartNumber:     p.PartNumber,
		DataID:         p.DataID,
		Size:           p.Size,
		ETag:           slices.Clone(p.ETag),
		Checksum:       slices.Clone(p.Checksum),
		UploadedUnixMS: p.ProposedAtUnixMS,
	})
	return res, nil
}

// ApplyCompleteMultipartUpload validates the part list, commits the
// assembled object version, and deletes the upload state — one transaction.
func (s *Store) ApplyCompleteMultipartUpload(p CompleteMultipartUpload) (res CompleteResult, err error) {
	defer s.txn(&err)()
	cfg, ok := s.GetBucket(p.Bucket)
	if !ok {
		return CompleteResult{}, ErrNoSuchBucket
	}
	if err := validateObjectKey(p.Key); err != nil {
		return CompleteResult{}, err
	}
	uploadRow := uploadRowKey(p.Bucket, p.Key, p.UploadID)
	rec, ok := s.kv.get(uploadRow)
	if !ok {
		return CompleteResult{}, ErrNoSuchUpload
	}
	upload := rec.(UploadRecord)
	if len(p.Parts) == 0 {
		return CompleteResult{}, ErrInvalidPart
	}

	// Stored parts arrive in part-number order — the scan order. Keeping
	// the slice (not just the lookup map) keeps every derived list, the
	// discarded set included, deterministically ordered.
	var storedParts []PartRecord
	stored := make(map[uint32]PartRecord)
	s.kv.scan(uploadRow, func(k string, v any) bool {
		if !hasPrefix(k, uploadRow) {
			return false
		}
		if pr, ok := v.(PartRecord); ok {
			storedParts = append(storedParts, pr)
			stored[pr.PartNumber] = pr
		}
		return true
	})

	// Validate the client's list against the stored rows. Ordering is a
	// property of the list itself and is checked first, in full; then each
	// listed part must exist with the ETag the client claims, and meet
	// S3's minimum size unless it is the last.
	for i := 1; i < len(p.Parts); i++ {
		if p.Parts[i].PartNumber <= p.Parts[i-1].PartNumber {
			return CompleteResult{}, ErrInvalidPartOrder
		}
	}
	var size int64
	parts := make([]PartRef, len(p.Parts))
	used := make(map[uint32]bool, len(p.Parts))
	for i, cp := range p.Parts {
		pr, ok := stored[cp.PartNumber]
		if !ok || !bytes.Equal(pr.ETag, cp.ETag) {
			return CompleteResult{}, ErrInvalidPart
		}
		if i < len(p.Parts)-1 && pr.Size < MinPartSize {
			return CompleteResult{}, ErrPartTooSmall
		}
		size += pr.Size
		parts[i] = PartRef{DataID: pr.DataID, Size: pr.Size, Checksum: pr.Checksum}
		used[cp.PartNumber] = true
	}

	// The same commit path as ApplyPutObject: bump for ordering, replace
	// the null version when versioning is not enabled, upsert c/.
	vid := p.VersionID
	if newest, ok := s.newestVersion(p.Bucket, p.Key); ok && vid.Compare(newest.VersionID) <= 0 {
		vid = newest.VersionID.Next()
	}
	entry := VersionEntry{
		FormatVersion: currentFormatVersion,
		VersionID:     vid,
		Kind:          KindObject,
		Size:          size,
		CreatedUnixMS: p.ProposedAtUnixMS,
		ETag:          p.ETag,
		ContentType:   upload.ContentType,
		UserMetadata:  upload.UserMetadata,
		Parts:         parts,
		NullVersion:   cfg.Versioning != VersioningEnabled,
	}.clone()

	if entry.NullVersion {
		if err := s.removeNullVersion(p.Bucket, p.Key, p.ProposedAtUnixMS); err != nil {
			return CompleteResult{}, err
		}
	}
	s.kv.set(versionRowKey(p.Bucket, p.Key, vid), entry)
	s.kv.set(currentRowKey(p.Bucket, p.Key), currentRecordFor(entry))

	res = CompleteResult{VersionID: vid}
	s.kv.delete(uploadRow)
	for _, pr := range storedParts {
		if !used[pr.PartNumber] {
			res.DiscardedDataIDs = append(res.DiscardedDataIDs, pr.DataID)
		}
		s.kv.delete(partRowKey(p.Bucket, p.Key, p.UploadID, pr.PartNumber))
	}
	return res, nil
}

// ApplyAbortMultipartUpload discards an upload and its part rows.
func (s *Store) ApplyAbortMultipartUpload(p AbortMultipartUpload) (res AbortResult, err error) {
	defer s.txn(&err)()
	if _, ok := s.GetBucket(p.Bucket); !ok {
		return AbortResult{}, ErrNoSuchBucket
	}
	if err := validateObjectKey(p.Key); err != nil {
		return AbortResult{}, err
	}
	uploadRow := uploadRowKey(p.Bucket, p.Key, p.UploadID)
	if _, ok := s.kv.get(uploadRow); !ok {
		return AbortResult{}, ErrNoSuchUpload
	}
	var rows []string
	s.kv.scan(uploadRow, func(k string, v any) bool {
		if !hasPrefix(k, uploadRow) {
			return false
		}
		if pr, ok := v.(PartRecord); ok {
			res.PartDataIDs = append(res.PartDataIDs, pr.DataID)
		}
		rows = append(rows, k)
		return true
	})
	for _, k := range rows {
		s.kv.delete(k)
	}
	return res, nil
}

// GetUpload returns an in-progress upload's record.
func (s *Store) GetUpload(bucket, key string, uid VersionID) (UploadRecord, bool) {
	v, ok := s.kv.get(uploadRowKey(bucket, key, uid))
	if !ok {
		return UploadRecord{}, false
	}
	rec := v.(UploadRecord)
	rec.UserMetadata = maps.Clone(rec.UserMetadata)
	return rec, true
}

// UploadListing is one ListMultipartUploads result row.
type UploadListing struct {
	Key    string
	Upload UploadRecord
}

// ListUploads returns up to max in-progress uploads in a bucket whose keys
// start with prefix, ordered by key then initiation (upload ID). The
// marker pair is exclusive, with S3's semantics: a zero uploadMarker means
// keys strictly after keyMarker only; a nonzero one admits keyMarker's own
// later uploads.
func (s *Store) ListUploads(bucket, prefix, keyMarker string, uploadMarker VersionID, max int) []UploadListing {
	var out []UploadListing
	if max <= 0 {
		return out
	}
	scanPrefix := uploadsScanPrefix(bucket)
	from := scanPrefix + prefix
	if keyMarker != "" {
		if f := scanPrefix + keyMarker; f > from {
			from = f
		}
	}
	s.kv.scan(from, func(k string, v any) bool {
		if !hasPrefix(k, scanPrefix) {
			return false
		}
		key, uid, _, isPart := uploadFromRow(k, bucket)
		if isPart {
			return true
		}
		if !hasPrefix(key, prefix) {
			return false // keys are scan-ordered; past the prefix means done
		}
		if keyMarker != "" {
			if uploadMarker.IsZero() {
				if key <= keyMarker {
					return true
				}
			} else if key < keyMarker || (key == keyMarker && uid.Compare(uploadMarker) <= 0) {
				return true
			}
		}
		if len(out) == max {
			return false
		}
		out = append(out, UploadListing{Key: key, Upload: v.(UploadRecord)})
		return true
	})
	return out
}

// ListUploadParts returns up to max of an upload's parts with numbers
// strictly greater than afterPart, in part-number order. The bool reports
// whether the upload exists at all.
func (s *Store) ListUploadParts(bucket, key string, uid VersionID, afterPart uint32, max int) ([]PartRecord, bool) {
	if _, ok := s.kv.get(uploadRowKey(bucket, key, uid)); !ok {
		return nil, false
	}
	var out []PartRecord
	if max <= 0 {
		return out, true
	}
	scanPrefix := uploadRowKey(bucket, key, uid)
	s.kv.scan(partRowKey(bucket, key, uid, afterPart+1), func(k string, v any) bool {
		if !hasPrefix(k, scanPrefix) || len(out) == max {
			return false
		}
		pr := v.(PartRecord)
		pr.ETag = slices.Clone(pr.ETag)
		pr.Checksum = slices.Clone(pr.Checksum)
		out = append(out, pr)
		return true
	})
	return out, true
}
