package meta

import (
	"bytes"
	"encoding/hex"
	"reflect"
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
)

// fullProposals returns one of each proposal type with every field set —
// the round-trip corpus. Hold the slice order stable: the golden test
// indexes into it.
func fullProposals() []any {
	uid := VersionID{0xAA, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	vid := VersionID{0xBB, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	did := VersionID{0xCC, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	return []any{
		CreateBucket{ProposedAtUnixMS: 1700000000001, Bucket: "docs", ObjectLockEnabled: true},
		DeleteBucket{ProposedAtUnixMS: 1700000000002, Bucket: "docs"},
		SetBucketVersioning{ProposedAtUnixMS: 1700000000003, Bucket: "docs", State: VersioningSuspended},
		SetObjectLockConfiguration{ProposedAtUnixMS: 1700000000018, Bucket: "docs",
			DefaultRetentionMode: RetentionGovernance, DefaultRetentionDays: 30},
		PutObject{
			ProposedAtUnixMS: 1700000000004, Bucket: "docs", Key: "dir/report.pdf",
			VersionID: vid, Size: 12345, ETag: []byte{0xE1, 0xE2}, ContentType: "application/pdf",
			UserMetadata: map[string]string{"b": "2", "a": "1"},
			Partition:    77, ECDataShards: 4, ECParityShards: 2,
			ObjectChecksum: []byte{0xC1}, ShardChecksums: [][]byte{{0x51}, {0x52}},
			RetentionMode: RetentionCompliance, RetainUntilUnixMS: 1800000000000, LegalHold: true,
			EncAlgorithm: EncAES256GCM, WrappedDEK: []byte{0x60, 0x61, 0x62}, KEKFingerprint: 0x1122334455667788,
		},
		DeleteObject{ProposedAtUnixMS: 1700000000005, Bucket: "docs", Key: "k", VersionID: vid},
		DeleteVersion{ProposedAtUnixMS: 1700000000006, Bucket: "docs", Key: "k", VersionID: vid, BypassGovernance: true},
		UpdateRetention{ProposedAtUnixMS: 1700000000007, Bucket: "docs", Key: "k", VersionID: vid,
			Mode: RetentionGovernance, RetainUntilUnixMS: 1800000000001, BypassGovernance: true},
		UpdateLegalHold{ProposedAtUnixMS: 1700000000008, Bucket: "docs", Key: "k", VersionID: vid, Hold: true},
		CreateMultipartUpload{ProposedAtUnixMS: 1700000000009, Bucket: "docs", Key: "k", UploadID: uid,
			ContentType: "video/mp4", UserMetadata: map[string]string{"x": "y"}},
		UploadPart{ProposedAtUnixMS: 1700000000010, Bucket: "docs", Key: "k", UploadID: uid,
			PartNumber: 3, DataID: did, Size: MinPartSize, ETag: []byte{0xE3}, Checksum: []byte{0xC3},
			Partition: 88, ECDataShards: 4, ECParityShards: 2,
			ShardChecksums: [][]byte{{0x61}, {0x62}, {0x63}, {0x64}, {0x65}, {0x66}},
			EncAlgorithm:   EncAES256GCM, WrappedDEK: []byte{0x70, 0x71}, KEKFingerprint: 0x99},
		CompleteMultipartUpload{ProposedAtUnixMS: 1700000000011, Bucket: "docs", Key: "k", UploadID: uid,
			VersionID: vid, ETag: []byte{0xE4},
			Parts: []CompletedPart{{PartNumber: 1, ETag: []byte{0x51}}, {PartNumber: 2, ETag: []byte{0x52}}}},
		AbortMultipartUpload{ProposedAtUnixMS: 1700000000012, Bucket: "docs", Key: "k", UploadID: uid},
		SetClusterLayout{ProposedAtUnixMS: 1700000000013, Version: 3, PartitionCount: 4096,
			Nodes:    []LayoutNode{{ID: "n1", Host: "h1", Zone: "z1"}, {ID: "n2", Host: "h2", Zone: "z2"}},
			Previous: []LayoutNode{{ID: "n1", Host: "h1", Zone: "z1"}, {ID: "n2", Host: "h2", Zone: "z2"}, {ID: "n3", Host: "h3", Zone: "z3"}}},
		RegisterNode{ProposedAtUnixMS: 1700000000014, NodeID: "n1", Host: "boxA", Zone: "z1", Capacity: 4, LeafCAFingerprint: 0xCA0001},
		SetNodeDraining{ProposedAtUnixMS: 1700000000015, NodeID: "n1", Draining: true},
		SetNodeReplacedBy{ProposedAtUnixMS: 1700000000016, NodeID: "n1", ReplacedBy: "n7"},
		ReEncodeObject{ProposedAtUnixMS: 1700000000017, Bucket: "docs", Key: "dir/report.pdf",
			VersionID: vid, DataID: did, ECDataShards: 3, ECParityShards: 2,
			ShardChecksums: [][]byte{{0x51}, {0x52}, {0x53}, {0x54}, {0x55}}},
		SetEncryptionPosture{ProposedAtUnixMS: 1700000000018, Algorithm: EncAES256GCM, KEKFingerprint: 0x0102030405060708},
		BeginKEKRotation{ProposedAtUnixMS: 1700000000019, FromFingerprint: 0x0102030405060708, ToFingerprint: 0x1112131415161718},
		RewrapDEK{ProposedAtUnixMS: 1700000000020, Bucket: "docs", Key: "dir/report.pdf",
			VersionID: vid, WrappedDEK: []byte{0x70, 0x71, 0x72}, KEKFingerprint: 0x1112131415161718},
		CompleteKEKRotation{ProposedAtUnixMS: 1700000000021, ToFingerprint: 0x1112131415161718},
		SetNodeLeafCA{ProposedAtUnixMS: 1700000000022, NodeID: "n1", LeafCAFingerprint: 0xCA0002},
		SetTrustBundle{ProposedAtUnixMS: 1700000000023, Version: 2, IssuerFingerprint: 0xCA0002,
			CAs: []TrustedCA{{Fingerprint: 0xCA0001, CertPEM: []byte("-old-")}, {Fingerprint: 0xCA0002, CertPEM: []byte("-new-")}}},
		SetNodeVersion{ProposedAtUnixMS: 1700000000024, NodeID: "n1", BinaryVersion: "v0.11.0-rc.1", Generation: 2},
	}
}

func TestProposalRoundTrip(t *testing.T) {
	for _, p := range fullProposals() {
		got, err := DecodeProposal(EncodeProposal(p))
		if err != nil {
			t.Fatalf("%T: decode: %v", p, err)
		}
		if !reflect.DeepEqual(got, p) {
			t.Fatalf("%T: round trip diverged:\n got %+v\nwant %+v", p, got, p)
		}
	}
}

// TestProposalGolden pins exact bytes for a hand-verifiable proposal. A
// change here is a wire format change: deliberate in v0, a bug after v1.
func TestProposalGolden(t *testing.T) {
	// CreateBucket{at: 1700000000001, "docs", lock}, verified by hand:
	//   0801                field 1 varint 1   (format_version)
	//   10 81d095ffbc31     field 2 varint 1700000000001 (proposed_at)
	//   1a08                field 3 bytes len 8 (create_bucket)
	//     0a04 646f6373       bucket "docs"
	//     1001                object_lock_enabled true
	const want = "08011081d095ffbc311a080a04646f63731001"
	got := hex.EncodeToString(EncodeProposal(fullProposals()[0]))
	if got != want {
		t.Fatalf("CreateBucket bytes changed:\n got %s\nwant %s", got, want)
	}
}

func TestProposalDeterministic(t *testing.T) {
	p := fullProposals()[3] // PutObject, the one with a map
	first := EncodeProposal(p)
	for range 32 {
		if !bytes.Equal(EncodeProposal(p), first) {
			t.Fatal("encoding is not deterministic")
		}
	}
}

// Unknown fields inside a known command are additive evolution: skipped.
func TestProposalUnknownCommandFieldSkipped(t *testing.T) {
	// Hand-build a CreateBucket whose command carries future field 90.
	var cmd []byte
	cmd = putString(cmd, 1, "docs")
	cmd = putUvarint(cmd, 90, 7)
	var b []byte
	b = putUvarint(b, 1, proposalFormatVersion)
	b = putUvarint(b, propAt, 42)
	b = protowire.AppendTag(b, propCreateBucket, protowire.BytesType)
	b = protowire.AppendBytes(b, cmd)

	got, err := DecodeProposal(b)
	if err != nil {
		t.Fatal(err)
	}
	want := CreateBucket{ProposedAtUnixMS: 42, Bucket: "docs"}
	if got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestProposalDecodeErrors(t *testing.T) {
	encode := func(fields ...func([]byte) []byte) []byte {
		var b []byte
		for _, f := range fields {
			b = f(b)
		}
		return b
	}
	envelope := func(num protowire.Number) func([]byte) []byte {
		return func(b []byte) []byte {
			b = protowire.AppendTag(b, num, protowire.BytesType)
			return protowire.AppendBytes(b, nil)
		}
	}
	cases := map[string][]byte{
		"empty":      {},
		"no_command": encode(func(b []byte) []byte { return putUvarint(b, propAt, 1) }),
		// Field 28 is the next unassigned command slot — a newer node's
		// command this build does not know, which must refuse, not half-apply.
		"unknown_command": encode(envelope(28)),
		"two_commands":    encode(envelope(propCreateBucket), envelope(propDeleteBucket)),
		"unknown_envelope_field": encode(envelope(propCreateBucket),
			func(b []byte) []byte { return putUvarint(b, 90, 1) }),
		"truncated": EncodeProposal(fullProposals()[3])[:11],
	}
	for name, b := range cases {
		if _, err := DecodeProposal(b); err == nil {
			t.Errorf("%s: decoded without error", name)
		}
	}
	// The upgrade-hint error message matters: it is what an operator sees.
	_, err := DecodeProposal(encode(envelope(28)))
	if err == nil || !strings.Contains(err.Error(), "upgrade") {
		t.Fatalf("unknown command error should hint at upgrading: %v", err)
	}
}
