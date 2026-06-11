package meta

import "strings"

// Keyspace encoding, exactly as designed in docs/METADATA.md and ADR-0014:
//
//	b/<bucket>                                BucketConfig
//	v/<bucket>\x00<key>\x00<~version-id>      VersionEntry — the truth
//	c/<bucket>\x00<key>                       CurrentRecord — derived
//
// Components are NUL-delimited: bucket names are NUL-safe by their charset,
// and object keys reject the literal NUL byte (validateObjectKey). The
// version component is the bitwise complement of the ID, so the first row
// under a key's prefix is always its newest version. Prefixes s/ (system),
// u/ (multipart), and g/ (GC) are reserved.

const nul = "\x00"

const bucketScanPrefix = "b/"

func bucketRowKey(bucket string) string { return "b/" + bucket }

func currentRowKey(bucket, key string) string { return "c/" + bucket + nul + key }

func currentScanPrefix(bucket string) string { return "c/" + bucket + nul }

func versionRowKey(bucket, key string, vid VersionID) string {
	c := complement(vid)
	return "v/" + bucket + nul + key + nul + string(c[:])
}

func versionScanPrefix(bucket, key string) string { return "v/" + bucket + nul + key + nul }

func bucketVersionsScanPrefix(bucket string) string { return "v/" + bucket + nul }

// complement flips every bit. UUIDv7 sorts oldest-first; the complement
// sorts newest-first, which is the order version lists are read in.
func complement(vid VersionID) VersionID {
	for i := range vid {
		vid[i] = ^vid[i]
	}
	return vid
}

// objectKeyFromCurrentRow recovers the object key from an encoded c/ row
// key belonging to bucket.
func objectKeyFromCurrentRow(rowKey, bucket string) string {
	return rowKey[len(currentScanPrefix(bucket)):]
}

// keyAndVersionFromVersionRow recovers the object key and version ID from
// an encoded v/ row key belonging to bucket. The complemented ID is the
// trailing 16 bytes (which may themselves contain NUL); the split is
// unambiguous because object keys cannot contain NUL.
func keyAndVersionFromVersionRow(rowKey, bucket string) (string, VersionID) {
	rest := rowKey[len(bucketVersionsScanPrefix(bucket)):]
	key := rest[:len(rest)-17]
	var c VersionID
	copy(c[:], rest[len(rest)-16:])
	return key, complement(c)
}

// validateObjectKey enforces the apply-layer key rules: 1–1024 bytes with
// no literal NUL — a stored NUL would corrupt the keyspace (METADATA.md).
// The S3 layer rejects NUL too; this check is the independent second layer
// that no caller, including a buggy internal one, can bypass.
func validateObjectKey(key string) error {
	if len(key) == 0 || len(key) > 1024 || strings.IndexByte(key, 0) >= 0 {
		return ErrInvalidObjectKey
	}
	return nil
}

// validateBucketName enforces the S3 shape (docs/S3-API.md): 3–63
// characters of [a-z0-9.-], starting and ending alphanumeric.
func validateBucketName(name string) error {
	if len(name) < 3 || len(name) > 63 {
		return ErrInvalidBucketName
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		alnum := c >= 'a' && c <= 'z' || c >= '0' && c <= '9'
		if (i == 0 || i == len(name)-1) && !alnum {
			return ErrInvalidBucketName
		}
		if !alnum && c != '.' && c != '-' {
			return ErrInvalidBucketName
		}
	}
	return nil
}
