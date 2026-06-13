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
