package meta

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"google.golang.org/protobuf/encoding/protowire"
)

// Record encoding: the protobuf wire format, hand-written over protowire
// (ADR-0023). Field numbers are the contract — they match the message
// sketches in docs/METADATA.md exactly and are never removed or repurposed
// (CLAUDE.md invariant 2, ADR-0008).
//
// Two properties the codecs guarantee beyond plain protobuf:
//
//   - Encoding is deterministic: fields in number order, map entries sorted
//     by key. The same record yields the same bytes on every node — what
//     replicated state and snapshot comparison will rely on.
//   - Unknown fields survive: a decoder keeps any field it does not know
//     (a newer writer's addition) and re-emits it on encode, so an older
//     node rewriting a record — a retention update, say — never sheds a
//     newer field.
//
// Zero values are omitted, proto3-style, and absent fields decode to zero.

// --- encode helpers ---

func putUvarint(b []byte, num protowire.Number, v uint64) []byte {
	if v == 0 {
		return b
	}
	b = protowire.AppendTag(b, num, protowire.VarintType)
	return protowire.AppendVarint(b, v)
}

func putBool(b []byte, num protowire.Number, v bool) []byte {
	if !v {
		return b
	}
	return putUvarint(b, num, 1)
}

func putBytes(b []byte, num protowire.Number, v []byte) []byte {
	if len(v) == 0 {
		return b
	}
	b = protowire.AppendTag(b, num, protowire.BytesType)
	return protowire.AppendBytes(b, v)
}

func putString(b []byte, num protowire.Number, v string) []byte {
	if v == "" {
		return b
	}
	b = protowire.AppendTag(b, num, protowire.BytesType)
	return protowire.AppendString(b, v)
}

func putID(b []byte, num protowire.Number, id VersionID) []byte {
	if id.IsZero() {
		return b
	}
	return putBytes(b, num, id[:])
}

// putMap encodes a map<string,string> field: one entry message per pair
// (key = 1, value = 2), sorted by key for deterministic bytes.
func putMap(b []byte, num protowire.Number, m map[string]string) []byte {
	for _, k := range slices.Sorted(maps.Keys(m)) {
		var entry []byte
		entry = putString(entry, 1, k)
		entry = putString(entry, 2, m[k])
		b = protowire.AppendTag(b, num, protowire.BytesType)
		b = protowire.AppendBytes(b, entry)
	}
	return b
}

// --- decode helpers ---

// dec walks one message's fields. Methods consume the current field's value
// and record the first error; callers check err once at the end.
type dec struct {
	b      []byte
	err    error
	num    protowire.Number
	typ    protowire.Type
	tagLen int
}

func (d *dec) next() bool {
	if d.err != nil || len(d.b) == 0 {
		return false
	}
	num, typ, n := protowire.ConsumeTag(d.b)
	if n < 0 {
		d.err = protowire.ParseError(n)
		return false
	}
	d.num, d.typ, d.tagLen = num, typ, n
	return true
}

func (d *dec) fail(format string, args ...any) {
	if d.err == nil {
		d.err = fmt.Errorf(format, args...)
	}
}

func (d *dec) uvarint() uint64 {
	if d.typ != protowire.VarintType {
		d.fail("field %d: wire type %d, want varint", d.num, d.typ)
		return 0
	}
	v, n := protowire.ConsumeVarint(d.b[d.tagLen:])
	if n < 0 {
		d.err = protowire.ParseError(n)
		return 0
	}
	d.b = d.b[d.tagLen+n:]
	return v
}

func (d *dec) int64() int64 { return int64(d.uvarint()) }

func (d *dec) uint32() uint32 {
	v := d.uvarint()
	if v > 1<<32-1 {
		d.fail("field %d: value %d overflows uint32", d.num, v)
	}
	return uint32(v)
}

func (d *dec) enum8() uint8 {
	v := d.uvarint()
	if v > 255 {
		d.fail("field %d: enum value %d out of range", d.num, v)
	}
	return uint8(v)
}

func (d *dec) bool() bool { return d.uvarint() != 0 }

// bytes returns a copy of the field's payload: decoded records own their
// memory, sharing nothing with the input buffer.
func (d *dec) bytes() []byte {
	if d.typ != protowire.BytesType {
		d.fail("field %d: wire type %d, want bytes", d.num, d.typ)
		return nil
	}
	v, n := protowire.ConsumeBytes(d.b[d.tagLen:])
	if n < 0 {
		d.err = protowire.ParseError(n)
		return nil
	}
	d.b = d.b[d.tagLen+n:]
	return slices.Clone(v)
}

func (d *dec) str() string { return string(d.bytes()) }

func (d *dec) id() VersionID {
	v := d.bytes()
	var id VersionID
	if d.err != nil {
		return id
	}
	if len(v) != len(id) {
		d.fail("field %d: ID is %d bytes, want %d", d.num, len(v), len(id))
		return id
	}
	copy(id[:], v)
	return id
}

func (d *dec) mapEntry(m map[string]string) map[string]string {
	e := &dec{b: d.bytes()}
	var k, v string
	for e.next() {
		switch e.num {
		case 1:
			k = e.str()
		case 2:
			v = e.str()
		default:
			e.skipUnknown(nil)
		}
	}
	if e.err != nil {
		d.fail("field %d: map entry: %w", d.num, e.err)
		return m
	}
	if m == nil {
		m = make(map[string]string)
	}
	m[k] = v
	return m
}

// skipUnknown consumes the current field and, when u is non-nil, appends its
// raw bytes to *u — unknown-field preservation.
func (d *dec) skipUnknown(u *[]byte) {
	n := protowire.ConsumeFieldValue(d.num, d.typ, d.b[d.tagLen:])
	if n < 0 {
		d.err = protowire.ParseError(n)
		return
	}
	if u != nil {
		*u = append(*u, d.b[:d.tagLen+n]...)
	}
	d.b = d.b[d.tagLen+n:]
}

// --- per-record codecs ---

func marshalBucketConfig(c BucketConfig) []byte {
	var b []byte
	b = putUvarint(b, 1, uint64(c.FormatVersion))
	b = putString(b, 2, c.Name)
	b = putUvarint(b, 3, uint64(c.CreatedUnixMS))
	b = putUvarint(b, 4, uint64(c.Versioning))
	b = putBool(b, 5, c.ObjectLockEnabled)
	b = putUvarint(b, 6, uint64(c.DefaultRetentionMode))
	b = putUvarint(b, 7, uint64(c.DefaultRetentionDays))
	b = putUvarint(b, 8, uint64(c.DefaultRetentionYears))
	return append(b, c.unknown...)
}

func unmarshalBucketConfig(b []byte) (BucketConfig, error) {
	var c BucketConfig
	d := &dec{b: b}
	for d.next() {
		switch d.num {
		case 1:
			c.FormatVersion = d.uint32()
		case 2:
			c.Name = d.str()
		case 3:
			c.CreatedUnixMS = d.int64()
		case 4:
			c.Versioning = VersioningState(d.enum8())
		case 5:
			c.ObjectLockEnabled = d.bool()
		case 6:
			c.DefaultRetentionMode = RetentionMode(d.enum8())
		case 7:
			c.DefaultRetentionDays = d.uint32()
		case 8:
			c.DefaultRetentionYears = d.uint32()
		default:
			d.skipUnknown(&c.unknown)
		}
	}
	return c, d.err
}

func marshalVersionEntry(e VersionEntry) []byte {
	var b []byte
	b = putUvarint(b, 1, uint64(e.FormatVersion))
	b = putID(b, 2, e.VersionID)
	b = putUvarint(b, 3, uint64(e.Kind))
	b = putUvarint(b, 4, uint64(e.Size))
	b = putUvarint(b, 5, uint64(e.CreatedUnixMS))
	b = putBytes(b, 6, e.ETag)
	b = putString(b, 7, e.ContentType)
	b = putMap(b, 8, e.UserMetadata)
	b = putUvarint(b, 9, e.Partition)
	b = putUvarint(b, 10, uint64(e.ECDataShards))
	b = putUvarint(b, 11, uint64(e.ECParityShards))
	b = putBytes(b, 12, e.ObjectChecksum)
	for _, sc := range e.ShardChecksums {
		b = protowire.AppendTag(b, 13, protowire.BytesType)
		b = protowire.AppendBytes(b, sc)
	}
	b = putUvarint(b, 14, uint64(e.RetentionMode))
	b = putUvarint(b, 15, uint64(e.RetainUntilUnixMS))
	b = putBool(b, 16, e.LegalHold)
	b = putBool(b, 17, e.NullVersion)
	b = putID(b, 18, e.DataID)
	for _, p := range e.Parts {
		b = protowire.AppendTag(b, 19, protowire.BytesType)
		b = protowire.AppendBytes(b, marshalPartRef(p))
	}
	b = putUvarint(b, 20, uint64(e.EncAlgorithm))
	b = putBytes(b, 21, e.WrappedDEK)
	return append(b, e.unknown...)
}

func unmarshalVersionEntry(b []byte) (VersionEntry, error) {
	var e VersionEntry
	d := &dec{b: b}
	for d.next() {
		switch d.num {
		case 1:
			e.FormatVersion = d.uint32()
		case 2:
			e.VersionID = d.id()
		case 3:
			e.Kind = Kind(d.enum8())
		case 4:
			e.Size = d.int64()
		case 5:
			e.CreatedUnixMS = d.int64()
		case 6:
			e.ETag = d.bytes()
		case 7:
			e.ContentType = d.str()
		case 8:
			e.UserMetadata = d.mapEntry(e.UserMetadata)
		case 9:
			e.Partition = d.uvarint()
		case 10:
			e.ECDataShards = d.uint32()
		case 11:
			e.ECParityShards = d.uint32()
		case 12:
			e.ObjectChecksum = d.bytes()
		case 13:
			e.ShardChecksums = append(e.ShardChecksums, d.bytes())
		case 14:
			e.RetentionMode = RetentionMode(d.enum8())
		case 15:
			e.RetainUntilUnixMS = d.int64()
		case 16:
			e.LegalHold = d.bool()
		case 17:
			e.NullVersion = d.bool()
		case 18:
			e.DataID = d.id()
		case 19:
			p, err := unmarshalPartRef(d.bytes())
			if err != nil {
				d.fail("field 19: part ref: %w", err)
				break
			}
			e.Parts = append(e.Parts, p)
		case 20:
			e.EncAlgorithm = EncAlgorithm(d.enum8())
		case 21:
			e.WrappedDEK = d.bytes()
		default:
			d.skipUnknown(&e.unknown)
		}
	}
	return e, d.err
}

func marshalPartRef(p PartRef) []byte {
	var b []byte
	b = putID(b, 1, p.DataID)
	b = putUvarint(b, 2, uint64(p.Size))
	b = putBytes(b, 3, p.Checksum)
	return append(b, p.unknown...)
}

func unmarshalPartRef(b []byte) (PartRef, error) {
	var p PartRef
	d := &dec{b: b}
	for d.next() {
		switch d.num {
		case 1:
			p.DataID = d.id()
		case 2:
			p.Size = d.int64()
		case 3:
			p.Checksum = d.bytes()
		default:
			d.skipUnknown(&p.unknown)
		}
	}
	return p, d.err
}

func marshalCurrentRecord(c CurrentRecord) []byte {
	var b []byte
	b = putUvarint(b, 1, uint64(c.FormatVersion))
	b = putID(b, 2, c.VersionID)
	b = putUvarint(b, 3, uint64(c.Size))
	b = putBytes(b, 4, c.ETag)
	b = putUvarint(b, 5, uint64(c.CreatedUnixMS))
	b = putUvarint(b, 6, uint64(c.PartCount))
	return append(b, c.unknown...)
}

func unmarshalCurrentRecord(b []byte) (CurrentRecord, error) {
	var c CurrentRecord
	d := &dec{b: b}
	for d.next() {
		switch d.num {
		case 1:
			c.FormatVersion = d.uint32()
		case 2:
			c.VersionID = d.id()
		case 3:
			c.Size = d.int64()
		case 4:
			c.ETag = d.bytes()
		case 5:
			c.CreatedUnixMS = d.int64()
		case 6:
			c.PartCount = d.uint32()
		default:
			d.skipUnknown(&c.unknown)
		}
	}
	return c, d.err
}

func marshalUploadRecord(u UploadRecord) []byte {
	var b []byte
	b = putUvarint(b, 1, uint64(u.FormatVersion))
	b = putID(b, 2, u.UploadID)
	b = putUvarint(b, 3, uint64(u.CreatedUnixMS))
	b = putString(b, 4, u.ContentType)
	b = putMap(b, 5, u.UserMetadata)
	return append(b, u.unknown...)
}

func unmarshalUploadRecord(b []byte) (UploadRecord, error) {
	var u UploadRecord
	d := &dec{b: b}
	for d.next() {
		switch d.num {
		case 1:
			u.FormatVersion = d.uint32()
		case 2:
			u.UploadID = d.id()
		case 3:
			u.CreatedUnixMS = d.int64()
		case 4:
			u.ContentType = d.str()
		case 5:
			u.UserMetadata = d.mapEntry(u.UserMetadata)
		default:
			d.skipUnknown(&u.unknown)
		}
	}
	return u, d.err
}

func marshalPartRecord(p PartRecord) []byte {
	var b []byte
	b = putUvarint(b, 1, uint64(p.FormatVersion))
	b = putUvarint(b, 2, uint64(p.PartNumber))
	b = putID(b, 3, p.DataID)
	b = putUvarint(b, 4, uint64(p.Size))
	b = putBytes(b, 5, p.ETag)
	b = putBytes(b, 6, p.Checksum)
	b = putUvarint(b, 7, uint64(p.UploadedUnixMS))
	return append(b, p.unknown...)
}

func unmarshalPartRecord(b []byte) (PartRecord, error) {
	var p PartRecord
	d := &dec{b: b}
	for d.next() {
		switch d.num {
		case 1:
			p.FormatVersion = d.uint32()
		case 2:
			p.PartNumber = d.uint32()
		case 3:
			p.DataID = d.id()
		case 4:
			p.Size = d.int64()
		case 5:
			p.ETag = d.bytes()
		case 6:
			p.Checksum = d.bytes()
		case 7:
			p.UploadedUnixMS = d.int64()
		default:
			d.skipUnknown(&p.unknown)
		}
	}
	return p, d.err
}

func marshalClusterLayout(l ClusterLayout) []byte {
	var b []byte
	b = putUvarint(b, 1, uint64(l.FormatVersion))
	b = putUvarint(b, 2, l.Version)
	b = putUvarint(b, 3, uint64(l.PartitionCount))
	for _, m := range l.Members {
		b = putString(b, 4, m)
	}
	for _, n := range l.Nodes {
		b = protowire.AppendTag(b, 5, protowire.BytesType)
		b = protowire.AppendBytes(b, marshalLayoutNode(n))
	}
	// Field 6 (previous) is additive and written only during a transition, so a
	// steady-state layout encodes byte-identically to a pre-transition one.
	for _, n := range l.Previous {
		b = protowire.AppendTag(b, 6, protowire.BytesType)
		b = protowire.AppendBytes(b, marshalLayoutNode(n))
	}
	return append(b, l.unknown...)
}

func unmarshalClusterLayout(b []byte) (ClusterLayout, error) {
	var l ClusterLayout
	d := &dec{b: b}
	for d.next() {
		switch d.num {
		case 1:
			l.FormatVersion = d.uint32()
		case 2:
			l.Version = d.uvarint()
		case 3:
			l.PartitionCount = d.uint32()
		case 4:
			l.Members = append(l.Members, d.str())
		case 5:
			n, err := unmarshalLayoutNode(d.bytes())
			if err != nil {
				d.fail("field 5: node: %w", err)
				break
			}
			l.Nodes = append(l.Nodes, n)
		case 6:
			n, err := unmarshalLayoutNode(d.bytes())
			if err != nil {
				d.fail("field 6: previous node: %w", err)
				break
			}
			l.Previous = append(l.Previous, n)
		default:
			d.skipUnknown(&l.unknown)
		}
	}
	return l, d.err
}

func marshalEncryptionPosture(p EncryptionPosture) []byte {
	var b []byte
	b = putUvarint(b, 1, uint64(p.FormatVersion))
	b = putUvarint(b, 2, uint64(p.Algorithm))
	return append(b, p.unknown...)
}

func unmarshalEncryptionPosture(b []byte) (EncryptionPosture, error) {
	var p EncryptionPosture
	d := &dec{b: b}
	for d.next() {
		switch d.num {
		case 1:
			p.FormatVersion = d.uint32()
		case 2:
			p.Algorithm = EncAlgorithm(d.enum8())
		default:
			d.skipUnknown(&p.unknown)
		}
	}
	return p, d.err
}

// marshalLayoutNode encodes one labeled member (ADR-0016). A nested message,
// like marshalPartRef; the layout is rewritten wholesale each generation, so
// it carries no unknown-field preservation.
func marshalLayoutNode(n LayoutNode) []byte {
	var b []byte
	b = putString(b, 1, n.ID)
	b = putString(b, 2, n.Host)
	b = putString(b, 3, n.Zone)
	// Field 4 (weight) is additive and written only when nonzero, so an
	// equal-weight node encodes byte-identically to a pre-weighting one.
	if n.Weight != 0 {
		b = putUvarint(b, 4, uint64(n.Weight))
	}
	// Field 5 (draining) is additive and written only when true, so a node not
	// being drained encodes byte-identically to a pre-draining one.
	if n.Draining {
		b = putUvarint(b, 5, 1)
	}
	return b
}

func unmarshalLayoutNode(b []byte) (LayoutNode, error) {
	var n LayoutNode
	d := &dec{b: b}
	for d.next() {
		switch d.num {
		case 1:
			n.ID = d.str()
		case 2:
			n.Host = d.str()
		case 3:
			n.Zone = d.str()
		case 4:
			n.Weight = uint32(d.uvarint())
		case 5:
			n.Draining = d.uvarint() != 0
		default:
			d.skipUnknown(nil)
		}
	}
	return n, d.err
}

// marshalNodeRecord encodes one member registration (s/node/<id>). Unlike
// the layout, a node row is rewritten in place on re-registration, so it
// preserves unknown fields like the object records do.
func marshalNodeRecord(n NodeRecord) []byte {
	var b []byte
	b = putUvarint(b, 1, uint64(n.FormatVersion))
	b = putString(b, 2, n.NodeID)
	b = putString(b, 3, n.Host)
	b = putString(b, 4, n.Zone)
	b = putUvarint(b, 5, uint64(n.Capacity))
	// Field 6 (draining) is additive and written only when true.
	if n.Draining {
		b = putUvarint(b, 6, 1)
	}
	// Field 7 (replaced_by) is additive and written only when set.
	b = putString(b, 7, n.ReplacedBy)
	return append(b, n.unknown...)
}

func unmarshalNodeRecord(b []byte) (NodeRecord, error) {
	var n NodeRecord
	d := &dec{b: b}
	for d.next() {
		switch d.num {
		case 1:
			n.FormatVersion = d.uint32()
		case 2:
			n.NodeID = d.str()
		case 3:
			n.Host = d.str()
		case 4:
			n.Zone = d.str()
		case 5:
			n.Capacity = d.uint32()
		case 6:
			n.Draining = d.uvarint() != 0
		case 7:
			n.ReplacedBy = d.str()
		default:
			d.skipUnknown(&n.unknown)
		}
	}
	return n, d.err
}

// --- row dispatch: a key's shape names its record type ---

// marshalRecord encodes any keyspace record value.
func marshalRecord(v any) []byte {
	switch r := v.(type) {
	case BucketConfig:
		return marshalBucketConfig(r)
	case VersionEntry:
		return marshalVersionEntry(r)
	case CurrentRecord:
		return marshalCurrentRecord(r)
	case UploadRecord:
		return marshalUploadRecord(r)
	case PartRecord:
		return marshalPartRecord(r)
	case ClusterLayout:
		return marshalClusterLayout(r)
	case EncryptionPosture:
		return marshalEncryptionPosture(r)
	case NodeRecord:
		return marshalNodeRecord(r)
	default:
		panic(fmt.Sprintf("meta: unencodable record type %T", v))
	}
}

// decodeRow decodes one persisted row's value, selecting the record type
// from the key's prefix — and for u/ rows, from the fixed-width tail that
// distinguishes an upload row (16 bytes) from a part row (20).
func decodeRow(key string, value []byte) (any, error) {
	switch {
	case key == clusterLayoutKey:
		return unmarshalClusterLayout(value)
	case key == encryptionPostureKey:
		return unmarshalEncryptionPosture(value)
	case strings.HasPrefix(key, nodeScanPrefix):
		return unmarshalNodeRecord(value)
	case strings.HasPrefix(key, bucketScanPrefix):
		return unmarshalBucketConfig(value)
	case strings.HasPrefix(key, "v/"):
		return unmarshalVersionEntry(value)
	case strings.HasPrefix(key, "c/"):
		return unmarshalCurrentRecord(value)
	case strings.HasPrefix(key, "u/"):
		rest := key[2:]
		i := strings.IndexByte(rest, 0) // ends the bucket
		if i >= 0 {
			if j := strings.IndexByte(rest[i+1:], 0); j >= 0 { // ends the object key
				switch len(rest) - (i + 1 + j + 1) {
				case 16:
					return unmarshalUploadRecord(value)
				case 20:
					return unmarshalPartRecord(value)
				}
			}
		}
		return nil, fmt.Errorf("malformed u/ row key %q", key)
	default:
		return nil, fmt.Errorf("unknown row prefix in key %q", key)
	}
}
