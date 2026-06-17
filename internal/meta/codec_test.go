package meta

import (
	"encoding/hex"
	"reflect"
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
)

// fullVersionEntry populates every field, the worst case for round-trips.
func fullVersionEntry() VersionEntry {
	return VersionEntry{
		FormatVersion:     1,
		VersionID:         VersionID{0x01, 0x02, 0x03, 0xFF, 0x00, 0x10},
		DataID:            VersionID{0xAA, 0x00, 0xBB},
		Kind:              KindObject,
		Size:              5 << 30,
		CreatedUnixMS:     1765432101234,
		ETag:              []byte{0xDE, 0xAD, 0xBE, 0xEF},
		ContentType:       "application/octet-stream",
		UserMetadata:      map[string]string{"b-key": "two", "a-key": "one", "empty": ""},
		Partition:         42,
		ECDataShards:      4,
		ECParityShards:    2,
		ObjectChecksum:    []byte{0x11, 0x22},
		ShardChecksums:    [][]byte{{0x01}, {0x02}, {0x03}},
		RetentionMode:     RetentionCompliance,
		RetainUntilUnixMS: 1899999999999,
		LegalHold:         true,
		EncAlgorithm:      EncAES256GCM,
		WrappedDEK:        []byte{0x77, 0x88, 0x99, 0xAB},
		KEKFingerprint:    0xA1A2A3A4A5A6A7A8,
		NullVersion:       true,
		Parts: []PartRef{
			{DataID: VersionID{0x01}, Size: 5 << 20, Checksum: []byte{0xCA, 0xFE}},
			{DataID: VersionID{0x02}, Size: 7, Checksum: []byte{0xF0}},
		},
	}
}

func TestCodecRoundTrip(t *testing.T) {
	t.Run("VersionEntry", func(t *testing.T) {
		in := fullVersionEntry()
		out, err := unmarshalVersionEntry(marshalVersionEntry(in))
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(in, out) {
			t.Fatalf("round trip:\n in: %+v\nout: %+v", in, out)
		}
	})
	t.Run("VersionEntry_zero", func(t *testing.T) {
		out, err := unmarshalVersionEntry(marshalVersionEntry(VersionEntry{}))
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(VersionEntry{}, out) {
			t.Fatalf("zero entry round trip: %+v", out)
		}
		if len(marshalVersionEntry(VersionEntry{})) != 0 {
			t.Fatal("zero entry should encode to zero bytes (proto3 zero omission)")
		}
	})
	t.Run("BucketConfig", func(t *testing.T) {
		in := BucketConfig{FormatVersion: 1, Name: "docs", CreatedUnixMS: 17, Versioning: VersioningSuspended, ObjectLockEnabled: true}
		out, err := unmarshalBucketConfig(marshalBucketConfig(in))
		if err != nil || !reflect.DeepEqual(in, out) {
			t.Fatalf("round trip: %+v, %v", out, err)
		}
	})
	t.Run("CurrentRecord", func(t *testing.T) {
		in := CurrentRecord{FormatVersion: 1, VersionID: VersionID{9}, Size: 3, ETag: []byte{1, 2}, CreatedUnixMS: 5, PartCount: 4}
		out, err := unmarshalCurrentRecord(marshalCurrentRecord(in))
		if err != nil || !reflect.DeepEqual(in, out) {
			t.Fatalf("round trip: %+v, %v", out, err)
		}
	})
	t.Run("UploadRecord", func(t *testing.T) {
		in := UploadRecord{FormatVersion: 1, UploadID: VersionID{7, 0, 8}, CreatedUnixMS: 12, ContentType: "text/plain", UserMetadata: map[string]string{"k": "v"}}
		out, err := unmarshalUploadRecord(marshalUploadRecord(in))
		if err != nil || !reflect.DeepEqual(in, out) {
			t.Fatalf("round trip: %+v, %v", out, err)
		}
	})
	t.Run("PartRecord", func(t *testing.T) {
		in := PartRecord{FormatVersion: 1, PartNumber: 10000, DataID: VersionID{1}, Size: 5 << 20, ETag: []byte{3}, Checksum: []byte{4}, UploadedUnixMS: 99}
		out, err := unmarshalPartRecord(marshalPartRecord(in))
		if err != nil || !reflect.DeepEqual(in, out) {
			t.Fatalf("round trip: %+v, %v", out, err)
		}
	})
	t.Run("TrustBundle", func(t *testing.T) {
		in := TrustBundle{FormatVersion: 1, Version: 3, IssuerFingerprint: 0xBEEF,
			CAs: []TrustedCA{{Fingerprint: 0xCAFE, CertPEM: []byte("-old-")}, {Fingerprint: 0xBEEF, CertPEM: []byte("-new-")}}}
		out, err := unmarshalTrustBundle(marshalTrustBundle(in))
		if err != nil || !reflect.DeepEqual(in, out) {
			t.Fatalf("round trip: %+v, %v", out, err)
		}
	})
	t.Run("NodeRecord", func(t *testing.T) {
		in := NodeRecord{FormatVersion: 1, NodeID: "n1", Host: "h", Zone: "z", Capacity: 2, ReplacedBy: "n9", LeafCAFingerprint: 0xCA11}
		out, err := unmarshalNodeRecord(marshalNodeRecord(in))
		if err != nil || !reflect.DeepEqual(in, out) {
			t.Fatalf("round trip: %+v, %v", out, err)
		}
	})
	t.Run("EncryptionPosture", func(t *testing.T) {
		// A posture mid-rotation: both fingerprints set (ADR-0032).
		in := EncryptionPosture{FormatVersion: 1, Algorithm: EncAES256GCM, CurrentKEKFingerprint: 0x1111, RotatingToKEKFingerprint: 0x2222}
		out, err := unmarshalEncryptionPosture(marshalEncryptionPosture(in))
		if err != nil || !reflect.DeepEqual(in, out) {
			t.Fatalf("round trip: %+v, %v", out, err)
		}
		// A pre-rotation posture (no fingerprints) still round-trips, and a
		// record from before the fields existed decodes to zero fingerprints.
		bare := EncryptionPosture{FormatVersion: 1, Algorithm: EncAES256GCM}
		out2, err := unmarshalEncryptionPosture(marshalEncryptionPosture(bare))
		if err != nil || !reflect.DeepEqual(bare, out2) {
			t.Fatalf("bare round trip: %+v, %v", out2, err)
		}
	})
}

// TestCodecGolden pins the exact bytes. A failure here means the on-disk
// format changed: deliberate only during v0 (ROADMAP.md), never after v1.
func TestCodecGolden(t *testing.T) {
	cases := []struct {
		name string
		got  []byte
		want string
	}{
		{"BucketConfig", marshalBucketConfig(BucketConfig{FormatVersion: 1, Name: "docs", CreatedUnixMS: 17, Versioning: VersioningEnabled, ObjectLockEnabled: true}),
			"08011204646f6373181120012801"},
		{"CurrentRecord", marshalCurrentRecord(CurrentRecord{FormatVersion: 1, VersionID: VersionID{9}, Size: 3, ETag: []byte{1, 2}, CreatedUnixMS: 5, PartCount: 4}),
			"080112100900000000000000000000000000000018032202010228053004"},
		{"VersionEntry", marshalVersionEntry(fullVersionEntry()),
			"08011210010203ff00100000000000000000000020808080801428f292d0dfb0333204deadbeef3a186170706c69636174696f6e2f6f637465742d73747265616d420c0a05612d6b657912036f6e65420c0a05622d6b6579120374776f42070a05656d707479482a50045802620211226a01016a01026a0103700278ffefcc86a637800101880101920110aa00bb000000000000000000000000009a011b0a1001000000000000000000000000000000108080c0021a02cafe9a01170a100200000000000000000000000000000010071a01f0a00101aa0104778899abb001a8cf9aadcaf4a8d1a101"},
		// EncryptionPosture mid-rotation (ADR-0032): field 3 current = 0x1111, field 4 rotating-to = 0x2222.
		{"EncryptionPosture", marshalEncryptionPosture(EncryptionPosture{FormatVersion: 1, Algorithm: EncAES256GCM, CurrentKEKFingerprint: 0x1111, RotatingToKEKFingerprint: 0x2222}),
			"0801100118912220a244"},
	}
	for _, c := range cases {
		if got := hex.EncodeToString(c.got); got != c.want {
			t.Errorf("%s encoding changed:\n got %s\nwant %s", c.name, got, c.want)
		}
	}
}

// Encoding must be deterministic: same record, same bytes, every time —
// the property replicated state machines compare on. Maps are the only
// iteration-order hazard.
func TestCodecDeterministic(t *testing.T) {
	e := fullVersionEntry()
	first := marshalVersionEntry(e)
	for i := 0; i < 32; i++ {
		if string(marshalVersionEntry(e)) != string(first) {
			t.Fatal("encoding is not deterministic across calls")
		}
	}
}

// A decoder must keep fields it does not know and re-emit them: an old
// node rewriting a record (a retention update) must never shed a newer
// writer's fields (ADR-0008).
func TestCodecUnknownFieldsSurvive(t *testing.T) {
	b := marshalVersionEntry(fullVersionEntry())
	// A future writer appends field 90 (varint) and field 91 (bytes).
	b = protowire.AppendTag(b, 90, protowire.VarintType)
	b = protowire.AppendVarint(b, 12345)
	b = protowire.AppendTag(b, 91, protowire.BytesType)
	b = protowire.AppendBytes(b, []byte("future"))

	e, err := unmarshalVersionEntry(b)
	if err != nil {
		t.Fatal(err)
	}
	if len(e.unknown) == 0 {
		t.Fatal("unknown fields were dropped at decode")
	}
	// The rewrite an old node would do: mutate a known field, re-encode.
	e.RetainUntilUnixMS++
	again, err := unmarshalVersionEntry(marshalVersionEntry(e))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(e, again) {
		t.Fatalf("unknown fields lost across rewrite:\n%+v\n%+v", e, again)
	}
	if !strings.Contains(string(marshalVersionEntry(again)), "future") {
		t.Fatal("re-encoded record no longer carries the future field's bytes")
	}
}

func TestCodecDecodeErrors(t *testing.T) {
	if _, err := unmarshalVersionEntry([]byte{0xFF}); err == nil {
		t.Fatal("truncated input decoded without error")
	}
	// Field 2 (version_id) with the wrong wire type: varint, not bytes.
	bad := protowire.AppendTag(nil, 2, protowire.VarintType)
	bad = protowire.AppendVarint(bad, 7)
	if _, err := unmarshalVersionEntry(bad); err == nil {
		t.Fatal("wire-type mismatch decoded without error")
	}
	// Field 2 with a 3-byte ID: IDs are exactly 16 bytes.
	bad = protowire.AppendTag(nil, 2, protowire.BytesType)
	bad = protowire.AppendBytes(bad, []byte{1, 2, 3})
	if _, err := unmarshalVersionEntry(bad); err == nil {
		t.Fatal("short ID decoded without error")
	}
}

func TestDecodeRowDispatch(t *testing.T) {
	uid := VersionID{0x00, 0xFF, 0x00, 0xFF} // NUL-bearing ID, the hostile case
	rows := []struct {
		key  string
		val  []byte
		want any
	}{
		{bucketRowKey("bkt"), marshalBucketConfig(BucketConfig{FormatVersion: 1, Name: "bkt"}), BucketConfig{FormatVersion: 1, Name: "bkt"}},
		{currentRowKey("bkt", "k"), marshalCurrentRecord(CurrentRecord{FormatVersion: 1, Size: 9}), CurrentRecord{FormatVersion: 1, Size: 9}},
		{versionRowKey("bkt", "k", uid), marshalVersionEntry(VersionEntry{FormatVersion: 1, VersionID: uid}), VersionEntry{FormatVersion: 1, VersionID: uid}},
		{uploadRowKey("bkt", "k", uid), marshalUploadRecord(UploadRecord{FormatVersion: 1, UploadID: uid}), UploadRecord{FormatVersion: 1, UploadID: uid}},
		{partRowKey("bkt", "k", uid, 7), marshalPartRecord(PartRecord{FormatVersion: 1, PartNumber: 7}), PartRecord{FormatVersion: 1, PartNumber: 7}},
	}
	for _, r := range rows {
		got, err := decodeRow(r.key, r.val)
		if err != nil {
			t.Fatalf("decodeRow(%q): %v", r.key, err)
		}
		if !reflect.DeepEqual(got, r.want) {
			t.Fatalf("decodeRow(%q):\n got %+v\nwant %+v", r.key, got, r.want)
		}
	}
	if _, err := decodeRow("x/whatever", nil); err == nil {
		t.Fatal("unknown prefix decoded without error")
	}
	if _, err := decodeRow("u/bkt\x00key\x00short", nil); err == nil {
		t.Fatal("malformed u/ tail decoded without error")
	}
}
