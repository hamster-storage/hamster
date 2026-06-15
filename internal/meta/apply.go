package meta

// The apply functions. Each is one S3 mutation as one transaction
// (METADATA.md principle 3), and each is a pure function of the proposal
// and existing state: no clock, no randomness, no I/O. Validation here is
// authoritative — the API layer validates too, but apply is the layer no
// caller can bypass.

// ApplyCreateBucket creates a bucket. Object-lock-enabled buckets start
// with versioning enabled (S3: lock requires versioning).
func (s *Store) ApplyCreateBucket(p CreateBucket) (err error) {
	defer s.txn(&err)()
	if err := validateBucketName(p.Bucket); err != nil {
		return err
	}
	if _, ok := s.kv.get(bucketRowKey(p.Bucket)); ok {
		return ErrBucketExists
	}
	cfg := BucketConfig{
		FormatVersion:     currentFormatVersion,
		Name:              p.Bucket,
		CreatedUnixMS:     p.ProposedAtUnixMS,
		Versioning:        Unversioned,
		ObjectLockEnabled: p.ObjectLockEnabled,
	}
	if p.ObjectLockEnabled {
		cfg.Versioning = VersioningEnabled
	}
	s.kv.set(bucketRowKey(p.Bucket), cfg)
	return nil
}

// ApplyDeleteBucket deletes a bucket if it holds no version rows and no
// in-progress multipart uploads — one prefix seek each, inside the
// transaction (METADATA.md). Delete markers count: a bucket with history
// is not empty. Uploads count too (S3 parity): deleting the bucket from
// under them would orphan their u/ rows and part data.
func (s *Store) ApplyDeleteBucket(p DeleteBucket) (err error) {
	defer s.txn(&err)()
	if _, ok := s.kv.get(bucketRowKey(p.Bucket)); !ok {
		return ErrNoSuchBucket
	}
	for _, prefix := range []string{bucketVersionsScanPrefix(p.Bucket), uploadsScanPrefix(p.Bucket)} {
		empty := true
		s.kv.scan(prefix, func(k string, _ any) bool {
			empty = !hasPrefix(k, prefix)
			return false
		})
		if !empty {
			return ErrBucketNotEmpty
		}
	}
	s.kv.delete(bucketRowKey(p.Bucket))
	return nil
}

// ApplySetBucketVersioning moves a bucket between Enabled and Suspended.
// Lock-enabled buckets can never suspend — suspension is the only path by
// which a later write could displace a version, and lock forbids it.
func (s *Store) ApplySetBucketVersioning(p SetBucketVersioning) (err error) {
	defer s.txn(&err)()
	cfg, ok := s.GetBucket(p.Bucket)
	if !ok {
		return ErrNoSuchBucket
	}
	if p.State != VersioningEnabled && p.State != VersioningSuspended {
		return ErrInvalidVersioningState
	}
	if p.State == VersioningSuspended && cfg.ObjectLockEnabled {
		return ErrInvalidVersioningState
	}
	cfg.Versioning = p.State
	s.kv.set(bucketRowKey(p.Bucket), cfg)
	return nil
}

// ApplyPutObject commits one object version: insert the v/ row, replace
// the prior null version when versioning is not enabled, upsert the c/
// row — one transaction.
func (s *Store) ApplyPutObject(p PutObject) (res PutResult, err error) {
	defer s.txn(&err)()
	cfg, ok := s.GetBucket(p.Bucket)
	if !ok {
		return PutResult{}, ErrNoSuchBucket
	}
	if err := validateObjectKey(p.Key); err != nil {
		return PutResult{}, err
	}
	if (p.RetentionMode != RetentionNone || p.LegalHold) && !cfg.ObjectLockEnabled {
		return PutResult{}, ErrInvalidRetention
	}
	if p.RetentionMode != RetentionNone && p.RetainUntilUnixMS <= p.ProposedAtUnixMS {
		return PutResult{}, ErrInvalidRetention
	}

	// Commit order beats clock order (METADATA.md): if the minted ID does
	// not sort after the key's newest version, bump it just past — every
	// replica computes the identical bump, so version lists stay
	// append-ordered by commit regardless of any node's clock.
	vid := p.VersionID
	newest, hasNewest := s.newestVersion(p.Bucket, p.Key)
	if hasNewest && vid.Compare(newest.VersionID) <= 0 {
		vid = newest.VersionID.Next()
	}

	entry := VersionEntry{
		FormatVersion:     currentFormatVersion,
		VersionID:         vid,
		DataID:            p.VersionID, // the minted ID the data was written under, pre-bump
		Kind:              KindObject,
		Size:              p.Size,
		CreatedUnixMS:     p.ProposedAtUnixMS,
		ETag:              p.ETag,
		ContentType:       p.ContentType,
		UserMetadata:      p.UserMetadata,
		Partition:         p.Partition,
		ECDataShards:      p.ECDataShards,
		ECParityShards:    p.ECParityShards,
		ObjectChecksum:    p.ObjectChecksum,
		ShardChecksums:    p.ShardChecksums,
		RetentionMode:     p.RetentionMode,
		RetainUntilUnixMS: p.RetainUntilUnixMS,
		LegalHold:         p.LegalHold,
		NullVersion:       cfg.Versioning != VersioningEnabled,
	}.clone() // own every reference field; the proposer may reuse its buffers

	var replaced []VersionID
	if entry.NullVersion {
		var err error
		if replaced, err = s.removeNullVersion(p.Bucket, p.Key, p.ProposedAtUnixMS); err != nil {
			return PutResult{}, err
		}
	}
	s.kv.set(versionRowKey(p.Bucket, p.Key, vid), entry)
	// The bump made vid the key's newest, so the current row is always
	// this entry.
	s.kv.set(currentRowKey(p.Bucket, p.Key), currentRecordFor(entry))
	return PutResult{VersionID: vid, ReplacedDataIDs: replaced}, nil
}

// ApplyDeleteObject is DELETE without a version ID.
func (s *Store) ApplyDeleteObject(p DeleteObject) (res DeleteObjectResult, err error) {
	defer s.txn(&err)()
	cfg, ok := s.GetBucket(p.Bucket)
	if !ok {
		return DeleteObjectResult{}, ErrNoSuchBucket
	}
	if err := validateObjectKey(p.Key); err != nil {
		return DeleteObjectResult{}, err
	}

	if cfg.Versioning == Unversioned {
		// The list holds one entry; remove it and the current row.
		newest, ok := s.newestVersion(p.Bucket, p.Key)
		if !ok {
			return DeleteObjectResult{}, nil // S3 DELETE is idempotent
		}
		// Unversioned buckets cannot hold locks; checked anyway, because
		// no code path may destroy a locked version (CLAUDE.md inv. 4).
		if newest.lockedAt(p.ProposedAtUnixMS, false) {
			return DeleteObjectResult{}, ErrObjectLocked
		}
		s.kv.delete(versionRowKey(p.Bucket, p.Key, newest.VersionID))
		s.kv.delete(currentRowKey(p.Bucket, p.Key))
		return DeleteObjectResult{Removed: true, RemovedDataIDs: newest.DataIDs()}, nil
	}

	// Versioned bucket: insert a delete marker. Under suspension the
	// marker is the null version and replaces the prior null entry,
	// exactly like a suspended PUT (S3 semantics).
	vid := p.VersionID
	if newest, ok := s.newestVersion(p.Bucket, p.Key); ok && vid.Compare(newest.VersionID) <= 0 {
		vid = newest.VersionID.Next()
	}
	marker := VersionEntry{
		FormatVersion: currentFormatVersion,
		VersionID:     vid,
		Kind:          KindDeleteMarker,
		CreatedUnixMS: p.ProposedAtUnixMS,
		NullVersion:   cfg.Versioning == VersioningSuspended,
	}
	var replaced []VersionID
	if marker.NullVersion {
		var err error
		if replaced, err = s.removeNullVersion(p.Bucket, p.Key, p.ProposedAtUnixMS); err != nil {
			return DeleteObjectResult{}, err
		}
	}
	s.kv.set(versionRowKey(p.Bucket, p.Key, vid), marker)
	s.kv.delete(currentRowKey(p.Bucket, p.Key)) // newest is now a marker
	return DeleteObjectResult{MarkerCreated: true, MarkerID: vid, RemovedDataIDs: replaced}, nil
}

// ApplyDeleteVersion is DELETE with a version ID — the one operation that
// destroys a version row, and therefore where the lock check lives: inside
// deterministic apply, against replicated state, with no time-of-check gap.
// There is no input that overrides COMPLIANCE retention or a legal hold.
func (s *Store) ApplyDeleteVersion(p DeleteVersion) (res DeleteVersionResult, err error) {
	defer s.txn(&err)()
	if _, ok := s.GetBucket(p.Bucket); !ok {
		return DeleteVersionResult{}, ErrNoSuchBucket
	}
	if err := validateObjectKey(p.Key); err != nil {
		return DeleteVersionResult{}, err
	}
	row, ok := s.kv.get(versionRowKey(p.Bucket, p.Key, p.VersionID))
	if !ok {
		return DeleteVersionResult{}, nil // idempotent
	}
	entry := row.(VersionEntry)
	if entry.lockedAt(p.ProposedAtUnixMS, p.BypassGovernance) {
		return DeleteVersionResult{}, ErrObjectLocked
	}
	newest, _ := s.newestVersion(p.Bucket, p.Key)
	wasNewest := newest.VersionID == entry.VersionID
	s.kv.delete(versionRowKey(p.Bucket, p.Key, entry.VersionID))
	if wasNewest {
		// Recompute the derived current row from the next-newest entry.
		if next, ok := s.newestVersion(p.Bucket, p.Key); ok && next.Kind == KindObject {
			s.kv.set(currentRowKey(p.Bucket, p.Key), currentRecordFor(next))
		} else {
			s.kv.delete(currentRowKey(p.Bucket, p.Key))
		}
	}
	return DeleteVersionResult{Removed: true}, nil
}

// ApplyUpdateRetention rewrites a version's retention fields — the only
// mutation a committed entry sees besides legal holds, and strengthen-only
// under COMPLIANCE (METADATA.md). Legal holds are independent of retention
// and do not block it.
func (s *Store) ApplyUpdateRetention(p UpdateRetention) (err error) {
	defer s.txn(&err)()
	entry, err := s.lockTarget(p.Bucket, p.Key, p.VersionID)
	if err != nil {
		return err
	}
	if p.Mode != RetentionNone && p.RetainUntilUnixMS <= p.ProposedAtUnixMS {
		return ErrInvalidRetention // retain-until must be in the proposal's future
	}

	// Expired retention behaves as no retention.
	curMode := entry.RetentionMode
	if curMode != RetentionNone && entry.RetainUntilUnixMS <= p.ProposedAtUnixMS {
		curMode = RetentionNone
	}
	switch curMode {
	case RetentionCompliance:
		// Strengthen-only, no override: stay COMPLIANCE, never earlier.
		if p.Mode != RetentionCompliance || p.RetainUntilUnixMS < entry.RetainUntilUnixMS {
			return ErrObjectLocked
		}
	case RetentionGovernance:
		// Without bypass, GOVERNANCE may only strengthen: removal and
		// earlier dates need the bypass.
		if !p.BypassGovernance &&
			(p.Mode == RetentionNone || p.RetainUntilUnixMS < entry.RetainUntilUnixMS) {
			return ErrObjectLocked
		}
	}

	entry.RetentionMode = p.Mode
	entry.RetainUntilUnixMS = p.RetainUntilUnixMS
	if p.Mode == RetentionNone {
		entry.RetainUntilUnixMS = 0
	}
	s.kv.set(versionRowKey(p.Bucket, p.Key, entry.VersionID), entry)
	return nil
}

// ApplyUpdateLegalHold sets or clears a version's legal hold.
func (s *Store) ApplyUpdateLegalHold(p UpdateLegalHold) (err error) {
	defer s.txn(&err)()
	entry, err := s.lockTarget(p.Bucket, p.Key, p.VersionID)
	if err != nil {
		return err
	}
	entry.LegalHold = p.Hold
	s.kv.set(versionRowKey(p.Bucket, p.Key, entry.VersionID), entry)
	return nil
}

// ApplyReEncodeObject rewrites a version's EC layout to a new profile (ADR-0004,
// ADR-0015) — a physical re-representation, not a content edit. Only the data-
// addressing and EC fields move; Size, ETag, ObjectChecksum, and the object-lock
// fields are left exactly as they are, so it is COMPLIANCE-safe (it can run on a
// locked version because it neither deletes the object nor shortens retention).
func (s *Store) ApplyReEncodeObject(p ReEncodeObject) (err error) {
	defer s.txn(&err)()
	row, ok := s.kv.get(versionRowKey(p.Bucket, p.Key, p.VersionID))
	if !ok {
		return ErrNoSuchVersion
	}
	entry := row.(VersionEntry)
	if entry.Kind != KindObject || len(entry.Parts) > 0 {
		// Delete markers and multipart objects have no single whole-object EC
		// layout to rewrite.
		return ErrInvalidReEncode
	}
	if p.ECDataShards == 0 || int(p.ECDataShards+p.ECParityShards) != len(p.ShardChecksums) {
		return ErrInvalidReEncode
	}
	entry.DataID = p.DataID
	entry.ECDataShards = p.ECDataShards
	entry.ECParityShards = p.ECParityShards
	sc := make([][]byte, len(p.ShardChecksums))
	for i, c := range p.ShardChecksums {
		sc[i] = append([]byte(nil), c...)
	}
	entry.ShardChecksums = sc
	s.kv.set(versionRowKey(p.Bucket, p.Key, p.VersionID), entry)
	return nil
}

// lockTarget validates and fetches the version a lock mutation aims at:
// the bucket must be lock-enabled and the target a real object, not a
// delete marker.
func (s *Store) lockTarget(bucket, key string, vid VersionID) (VersionEntry, error) {
	cfg, ok := s.GetBucket(bucket)
	if !ok {
		return VersionEntry{}, ErrNoSuchBucket
	}
	if err := validateObjectKey(key); err != nil {
		return VersionEntry{}, err
	}
	if !cfg.ObjectLockEnabled {
		return VersionEntry{}, ErrInvalidRetention
	}
	entry, ok := s.GetVersion(bucket, key, vid)
	if !ok {
		return VersionEntry{}, ErrNoSuchVersion
	}
	if entry.Kind != KindObject {
		return VersionEntry{}, ErrInvalidRetention
	}
	return entry, nil
}

// ApplySetClusterLayout installs a new cluster-layout generation (ADR-0028):
// the replicated placement basis, committed like any other proposal but
// naming nodes, not objects. Compare-and-set on Version — the first layout
// must be Version 1 and each later one exactly the stored Version plus one —
// so a reconciling leader that retransmits, or two proposals that race, is
// refused deterministically (ErrStaleLayout) instead of overwriting a newer
// layout; every replica converges to the same generation. The partition
// count is fixed at the first install and may never change (ADR-0004: never
// resized); a later layout disagreeing on it is refused. A layout with no
// members or a zero partition count is invalid.
func (s *Store) ApplySetClusterLayout(p SetClusterLayout) (err error) {
	defer s.txn(&err)()
	want := uint64(1)
	if prev, ok := s.kv.get(clusterLayoutKey); ok {
		cur := prev.(ClusterLayout)
		want = cur.Version + 1
		if p.PartitionCount != cur.PartitionCount {
			return ErrInvalidLayout
		}
	}
	if p.Version != want {
		return ErrStaleLayout
	}
	if p.PartitionCount == 0 || (len(p.Members) == 0 && len(p.Nodes) == 0) {
		return ErrInvalidLayout
	}
	s.kv.set(clusterLayoutKey, ClusterLayout{
		FormatVersion:  currentFormatVersion,
		Version:        p.Version,
		PartitionCount: p.PartitionCount,
		Members:        append([]string(nil), p.Members...),
		Nodes:          append([]LayoutNode(nil), p.Nodes...),
		Previous:       append([]LayoutNode(nil), p.Previous...),
	})
	return nil
}

// ApplyRegisterNode upserts a member's registration row (ADR-0016, ADR-0004):
// the replicated registry the layout reconcile composes a labeled layout
// from. Idempotent by node ID — a re-registration replaces the row — so a
// reconciling leader that retransmits converges every replica deterministically.
// An empty node ID is invalid.
func (s *Store) ApplyRegisterNode(p RegisterNode) (err error) {
	defer s.txn(&err)()
	if p.NodeID == "" {
		return ErrInvalidNode
	}
	s.kv.set(nodeRowKey(p.NodeID), NodeRecord{
		FormatVersion: currentFormatVersion,
		NodeID:        p.NodeID,
		Host:          p.Host,
		Zone:          p.Zone,
		Capacity:      p.Capacity,
	})
	return nil
}

// ApplySetNodeDraining flips a registered member's drain flag (ADR-0004),
// leaving its labels, capacity, and unknown fields intact. Idempotent;
// refuses an unknown node.
func (s *Store) ApplySetNodeDraining(p SetNodeDraining) (err error) {
	defer s.txn(&err)()
	if p.NodeID == "" {
		return ErrInvalidNode
	}
	v, ok := s.kv.get(nodeRowKey(p.NodeID))
	if !ok {
		return ErrInvalidNode
	}
	rec := v.(NodeRecord)
	rec.Draining = p.Draining
	s.kv.set(nodeRowKey(p.NodeID), rec)
	return nil
}

// ApplySetNodeReplacedBy records the node taking p.NodeID's place (ADR-0004),
// or clears the pairing when p.ReplacedBy is empty. Leaves the node's other
// fields intact. Idempotent; refuses an unknown node.
func (s *Store) ApplySetNodeReplacedBy(p SetNodeReplacedBy) (err error) {
	defer s.txn(&err)()
	if p.NodeID == "" {
		return ErrInvalidNode
	}
	v, ok := s.kv.get(nodeRowKey(p.NodeID))
	if !ok {
		return ErrInvalidNode
	}
	rec := v.(NodeRecord)
	rec.ReplacedBy = p.ReplacedBy
	s.kv.set(nodeRowKey(p.NodeID), rec)
	return nil
}

// removeNullVersion deletes the key's null-version entry if one exists, as
// part of an unversioned or suspended write. The lock check is defense in
// depth: lock-enabled buckets can never reach this path, but no code path
// may destroy a locked version, full stop.
func (s *Store) removeNullVersion(bucket, key string, atUnixMS int64) ([]VersionID, error) {
	prior, ok := s.nullVersion(bucket, key)
	if !ok {
		return nil, nil
	}
	if prior.lockedAt(atUnixMS, false) {
		return nil, ErrObjectLocked
	}
	s.kv.delete(versionRowKey(bucket, key, prior.VersionID))
	return prior.DataIDs(), nil
}
