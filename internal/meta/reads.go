package meta

import (
	"slices"
	"strings"
)

// Reads. These are local and require no proposal; strong consistency comes
// from the Raft layer above (read-index), per ADR-0005. All results are
// copies — callers can hold them freely.

// GetBucket returns a bucket's configuration.
func (s *Store) GetBucket(name string) (BucketConfig, bool) {
	v, ok := s.kv.get(bucketRowKey(name))
	if !ok {
		return BucketConfig{}, false
	}
	return v.(BucketConfig), true
}

// ListBuckets returns all buckets in name order.
func (s *Store) ListBuckets() []BucketConfig {
	var out []BucketConfig
	s.kv.scan(bucketScanPrefix, func(k string, v any) bool {
		if !hasPrefix(k, bucketScanPrefix) {
			return false
		}
		out = append(out, v.(BucketConfig))
		return true
	})
	return out
}

// ClusterLayout returns the installed cluster layout (ADR-0028), present
// once the first generation has been committed. Placement resolves against
// it rather than the live member set. The returned Members slice is a copy
// the caller may hold and mutate freely.
func (s *Store) ClusterLayout() (ClusterLayout, bool) {
	v, ok := s.kv.get(clusterLayoutKey)
	if !ok {
		return ClusterLayout{}, false
	}
	l := v.(ClusterLayout)
	l.Members = slices.Clone(l.Members)
	l.Nodes = slices.Clone(l.Nodes)
	return l, true
}

// EncryptionAlgorithm returns the cluster's encryption-at-rest posture
// (ADR-0021): the algorithm new writes use, or EncNone when the cluster does
// not encrypt (including before any posture is committed). Reads never
// consult this — each version records its own algorithm — so it governs only
// what new writes do.
func (s *Store) EncryptionAlgorithm() EncAlgorithm {
	v, ok := s.kv.get(encryptionPostureKey)
	if !ok {
		return EncNone
	}
	return v.(EncryptionPosture).Algorithm
}

// EncryptionPosture returns the cluster's full encryption-at-rest posture
// (ADR-0021, ADR-0032): the algorithm plus the current and rotating-to KEK
// fingerprints. The zero value (Algorithm EncNone, both fingerprints zero) is
// returned before any posture is committed. The coordinator reads it to stamp
// new writes with the current fingerprint and to drive a rotation; status
// reports it.
func (s *Store) EncryptionPosture() EncryptionPosture {
	v, ok := s.kv.get(encryptionPostureKey)
	if !ok {
		return EncryptionPosture{}
	}
	return v.(EncryptionPosture)
}

// TrustBundle returns the cluster's CA trust bundle (ADR-0033): the set of
// trusted CA certificates and the issuing CA, present once the first generation
// is committed. A node builds its mTLS trust pool from it. The returned slices
// are copies the caller may hold.
func (s *Store) TrustBundle() (TrustBundle, bool) {
	v, ok := s.kv.get(trustBundleKey)
	if !ok {
		return TrustBundle{}, false
	}
	t := v.(TrustBundle)
	cas := make([]TrustedCA, len(t.CAs))
	for i, c := range t.CAs {
		cas[i] = TrustedCA{Fingerprint: c.Fingerprint, CertPEM: slices.Clone(c.CertPEM)}
	}
	t.CAs = cas
	return t, true
}

// Node returns a member's registration row (ADR-0016, ADR-0004), present
// once the issuer has committed it through RegisterNode.
func (s *Store) Node(id string) (NodeRecord, bool) {
	v, ok := s.kv.get(nodeRowKey(id))
	if !ok {
		return NodeRecord{}, false
	}
	return v.(NodeRecord), true
}

// Nodes returns every registered member in node-ID order — the replicated
// registry the layout reconcile composes a labeled layout from, so any
// leader builds the same one.
func (s *Store) Nodes() []NodeRecord {
	var out []NodeRecord
	s.kv.scan(nodeScanPrefix, func(k string, v any) bool {
		if !hasPrefix(k, nodeScanPrefix) {
			return false
		}
		out = append(out, v.(NodeRecord))
		return true
	})
	return out
}

// Current returns the derived current-version record for a key: present
// if and only if the key's newest version is a live object.
func (s *Store) Current(bucket, key string) (CurrentRecord, bool) {
	v, ok := s.kv.get(currentRowKey(bucket, key))
	if !ok {
		return CurrentRecord{}, false
	}
	rec := v.(CurrentRecord)
	rec.ETag = slices.Clone(rec.ETag)
	return rec, true
}

// GetVersion returns one version entry by ID — the GET-with-versionId
// path: one direct read.
func (s *Store) GetVersion(bucket, key string, vid VersionID) (VersionEntry, bool) {
	v, ok := s.kv.get(versionRowKey(bucket, key, vid))
	if !ok {
		return VersionEntry{}, false
	}
	return v.(VersionEntry).clone(), true
}

// ListVersions returns a key's full version list, newest first — the
// order the keyspace stores it in (ADR-0014).
func (s *Store) ListVersions(bucket, key string) []VersionEntry {
	prefix := versionScanPrefix(bucket, key)
	var out []VersionEntry
	s.kv.scan(prefix, func(k string, v any) bool {
		if !hasPrefix(k, prefix) {
			return false
		}
		out = append(out, v.(VersionEntry).clone())
		return true
	})
	return out
}

// ObjectListing is one ListObjects result row.
type ObjectListing struct {
	Key     string
	Current CurrentRecord
}

// ListObjects scans the derived c/ index: every row is a live current
// object, already in S3 listing order (METADATA.md). startAfter is
// exclusive; max <= 0 means unlimited. Delimiter grouping is the API
// layer's job.
func (s *Store) ListObjects(bucket, prefix, startAfter string, max int) []ObjectListing {
	scanPrefix := currentScanPrefix(bucket) + prefix
	from := scanPrefix
	if startAfter != "" {
		// Object keys cannot contain NUL, so rowKey+NUL is the smallest
		// encoded key strictly after startAfter's row.
		if after := currentRowKey(bucket, startAfter) + nul; after > from {
			from = after
		}
	}
	var out []ObjectListing
	s.kv.scan(from, func(k string, v any) bool {
		if !hasPrefix(k, scanPrefix) {
			return false
		}
		if max > 0 && len(out) == max {
			return false
		}
		rec := v.(CurrentRecord)
		rec.ETag = slices.Clone(rec.ETag)
		out = append(out, ObjectListing{Key: objectKeyFromCurrentRow(k, bucket), Current: rec})
		return true
	})
	return out
}

// VersionListing is one ListObjectVersions result row: a version (or delete
// marker) and whether it is its key's current version.
type VersionListing struct {
	Key      string
	Entry    VersionEntry
	IsLatest bool
}

// ListObjectVersions returns a page of versions and delete markers across keys,
// in S3 ListObjectVersions order — key ascending, newest version first within a
// key. It resumes strictly after (keyMarker, versionIDMarker): a zero
// versionIDMarker means "after the whole keyMarker key", a set one means "after
// that version of keyMarker". prefix filters keys; delimiter grouping is the API
// layer's job. Up to max rows are returned; truncated reports whether more
// remain. IsLatest marks each key's newest version — false for a page that
// resumes into the middle of keyMarker's versions, since that key's newest was
// on the prior page.
func (s *Store) ListObjectVersions(bucket, prefix, keyMarker string, versionIDMarker VersionID, max int) (out []VersionListing, truncated bool) {
	if max <= 0 {
		return nil, false
	}
	bucketPrefix := bucketVersionsScanPrefix(bucket)
	from := bucketPrefix + prefix
	resumedMidKey := keyMarker != "" && versionIDMarker != (VersionID{})
	if keyMarker != "" {
		// versionRowKey with a zero ID complements to all-0xff — the largest
		// (oldest) tail — so +nul lands just past every version of keyMarker; a
		// set ID lands just past that one version.
		if cur := versionRowKey(bucket, keyMarker, versionIDMarker) + nul; cur > from {
			from = cur
		}
	}
	prevKey := ""
	havePrev := false
	s.kv.scan(from, func(k string, v any) bool {
		if !hasPrefix(k, bucketPrefix) {
			return false // left this bucket's version rows
		}
		key, _ := keyAndVersionFromVersionRow(k, bucket)
		if !hasPrefix(key, prefix) {
			return false // past the prefix range (keys sharing it are contiguous)
		}
		if len(out) == max {
			truncated = true
			return false
		}
		isFirstOfKey := !havePrev || key != prevKey
		isLatest := isFirstOfKey && !(resumedMidKey && key == keyMarker)
		out = append(out, VersionListing{Key: key, Entry: v.(VersionEntry).clone(), IsLatest: isLatest})
		prevKey, havePrev = key, true
		return true
	})
	return out, truncated
}

// newestVersion returns a key's newest version entry: the first row under
// its v/ prefix, by the complement encoding. Internal — the returned entry
// shares reference fields with the stored row and must not be mutated.
func (s *Store) newestVersion(bucket, key string) (VersionEntry, bool) {
	prefix := versionScanPrefix(bucket, key)
	var entry VersionEntry
	found := false
	s.kv.scan(prefix, func(k string, v any) bool {
		if hasPrefix(k, prefix) {
			entry, found = v.(VersionEntry), true
		}
		return false
	})
	return entry, found
}

// nullVersion finds a key's null-version entry — at most one exists.
func (s *Store) nullVersion(bucket, key string) (VersionEntry, bool) {
	prefix := versionScanPrefix(bucket, key)
	var entry VersionEntry
	found := false
	s.kv.scan(prefix, func(k string, v any) bool {
		if !hasPrefix(k, prefix) {
			return false
		}
		if e := v.(VersionEntry); e.NullVersion {
			entry, found = e, true
			return false
		}
		return true
	})
	return entry, found
}

func hasPrefix(s, prefix string) bool { return strings.HasPrefix(s, prefix) }

// ScanVersions visits every version entry in a bucket, in keyspace order,
// until fn returns false. This is repair's and scrub's view of the world:
// unlike ListObjects (the derived current index), it sees every version —
// non-current versions and keys whose current is a delete marker still
// hold shards that must stay healthy. Entries share no storage with the
// caller (cloned, like every read).
func (s *Store) ScanVersions(bucket string, fn func(key string, e VersionEntry) bool) {
	prefix := bucketVersionsScanPrefix(bucket)
	s.kv.scan(prefix, func(k string, v any) bool {
		if !hasPrefix(k, prefix) {
			return false
		}
		key, _ := keyAndVersionFromVersionRow(k, bucket)
		return fn(key, v.(VersionEntry).clone())
	})
}
