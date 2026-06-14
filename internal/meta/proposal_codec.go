package meta

import (
	"fmt"

	"google.golang.org/protobuf/encoding/protowire"
)

// Proposal encoding: the Proposal envelope from docs/METADATA.md, hand-
// written like the record codecs (ADR-0023) and sharing their guarantees —
// deterministic bytes, proto3 zero-omission, field numbers fixed forever.
// This is what travels the Raft log in v0.2: one S3 mutation per entry.
//
// Two rules differ from records, both because a proposal is *applied*, not
// stored. An unknown command number is an error: a replica must never
// half-apply a mutation it does not understand — refusing is what keeps
// state machines identical. Unknown fields inside a known command are
// skipped, proto3-style: additive evolution within a command is legal, but
// only once every node understands the field (ADR-0008 expand-then-
// contract; the v0.8 feature gates enforce it mechanically). And there is
// no unknown-field preservation: proposals are written once and never
// rewritten.

const proposalFormatVersion = 1

// Envelope field numbers (METADATA.md "Operations as transactions").
const (
	propAt              = 2
	propCreateBucket    = 3
	propDeleteBucket    = 4
	propSetVersioning   = 5
	propPutObject       = 6
	propDeleteObject    = 7
	propDeleteVersion   = 8
	propUpdateRetention = 9
	propUpdateLegalHold = 10
	propCreateUpload    = 11
	propUploadPart      = 12
	propCompleteUpload  = 13
	propAbortUpload     = 14
	propSetLayout       = 15 // cluster layout (ADR-0028)
	propRegisterNode    = 16 // member registration (ADR-0016, ADR-0004)
	propSetNodeDraining = 17 // member drain flag (ADR-0004)
)

// EncodeProposal encodes one proposal for the Raft log. p must be one of
// the proposal struct types in this package; anything else is a caller bug
// and panics.
func EncodeProposal(p any) []byte {
	var atMS int64
	var num protowire.Number
	var cmd []byte
	switch c := p.(type) {
	case CreateBucket:
		atMS, num = c.ProposedAtUnixMS, propCreateBucket
		cmd = putString(cmd, 1, c.Bucket)
		cmd = putBool(cmd, 2, c.ObjectLockEnabled)
	case DeleteBucket:
		atMS, num = c.ProposedAtUnixMS, propDeleteBucket
		cmd = putString(cmd, 1, c.Bucket)
	case SetBucketVersioning:
		atMS, num = c.ProposedAtUnixMS, propSetVersioning
		cmd = putString(cmd, 1, c.Bucket)
		cmd = putUvarint(cmd, 2, uint64(c.State))
	case PutObject:
		atMS, num = c.ProposedAtUnixMS, propPutObject
		cmd = putString(cmd, 1, c.Bucket)
		cmd = putString(cmd, 2, c.Key)
		cmd = putID(cmd, 3, c.VersionID)
		cmd = putUvarint(cmd, 4, uint64(c.Size))
		cmd = putBytes(cmd, 5, c.ETag)
		cmd = putString(cmd, 6, c.ContentType)
		cmd = putMap(cmd, 7, c.UserMetadata)
		cmd = putUvarint(cmd, 8, c.Partition)
		cmd = putUvarint(cmd, 9, uint64(c.ECDataShards))
		cmd = putUvarint(cmd, 10, uint64(c.ECParityShards))
		cmd = putBytes(cmd, 11, c.ObjectChecksum)
		for _, sc := range c.ShardChecksums {
			cmd = protowire.AppendTag(cmd, 12, protowire.BytesType)
			cmd = protowire.AppendBytes(cmd, sc)
		}
		cmd = putUvarint(cmd, 13, uint64(c.RetentionMode))
		cmd = putUvarint(cmd, 14, uint64(c.RetainUntilUnixMS))
		cmd = putBool(cmd, 15, c.LegalHold)
	case DeleteObject:
		atMS, num = c.ProposedAtUnixMS, propDeleteObject
		cmd = putString(cmd, 1, c.Bucket)
		cmd = putString(cmd, 2, c.Key)
		cmd = putID(cmd, 3, c.VersionID)
	case DeleteVersion:
		atMS, num = c.ProposedAtUnixMS, propDeleteVersion
		cmd = putString(cmd, 1, c.Bucket)
		cmd = putString(cmd, 2, c.Key)
		cmd = putID(cmd, 3, c.VersionID)
		cmd = putBool(cmd, 4, c.BypassGovernance)
	case UpdateRetention:
		atMS, num = c.ProposedAtUnixMS, propUpdateRetention
		cmd = putString(cmd, 1, c.Bucket)
		cmd = putString(cmd, 2, c.Key)
		cmd = putID(cmd, 3, c.VersionID)
		cmd = putUvarint(cmd, 4, uint64(c.Mode))
		cmd = putUvarint(cmd, 5, uint64(c.RetainUntilUnixMS))
		cmd = putBool(cmd, 6, c.BypassGovernance)
	case UpdateLegalHold:
		atMS, num = c.ProposedAtUnixMS, propUpdateLegalHold
		cmd = putString(cmd, 1, c.Bucket)
		cmd = putString(cmd, 2, c.Key)
		cmd = putID(cmd, 3, c.VersionID)
		cmd = putBool(cmd, 4, c.Hold)
	case CreateMultipartUpload:
		atMS, num = c.ProposedAtUnixMS, propCreateUpload
		cmd = putString(cmd, 1, c.Bucket)
		cmd = putString(cmd, 2, c.Key)
		cmd = putID(cmd, 3, c.UploadID)
		cmd = putString(cmd, 4, c.ContentType)
		cmd = putMap(cmd, 5, c.UserMetadata)
	case UploadPart:
		atMS, num = c.ProposedAtUnixMS, propUploadPart
		cmd = putString(cmd, 1, c.Bucket)
		cmd = putString(cmd, 2, c.Key)
		cmd = putID(cmd, 3, c.UploadID)
		cmd = putUvarint(cmd, 4, uint64(c.PartNumber))
		cmd = putID(cmd, 5, c.DataID)
		cmd = putUvarint(cmd, 6, uint64(c.Size))
		cmd = putBytes(cmd, 7, c.ETag)
		cmd = putBytes(cmd, 8, c.Checksum)
	case CompleteMultipartUpload:
		atMS, num = c.ProposedAtUnixMS, propCompleteUpload
		cmd = putString(cmd, 1, c.Bucket)
		cmd = putString(cmd, 2, c.Key)
		cmd = putID(cmd, 3, c.UploadID)
		cmd = putID(cmd, 4, c.VersionID)
		cmd = putBytes(cmd, 5, c.ETag)
		for _, p := range c.Parts {
			var part []byte
			part = putUvarint(part, 1, uint64(p.PartNumber))
			part = putBytes(part, 2, p.ETag)
			cmd = protowire.AppendTag(cmd, 6, protowire.BytesType)
			cmd = protowire.AppendBytes(cmd, part)
		}
	case AbortMultipartUpload:
		atMS, num = c.ProposedAtUnixMS, propAbortUpload
		cmd = putString(cmd, 1, c.Bucket)
		cmd = putString(cmd, 2, c.Key)
		cmd = putID(cmd, 3, c.UploadID)
	case SetClusterLayout:
		atMS, num = c.ProposedAtUnixMS, propSetLayout
		cmd = putUvarint(cmd, 1, c.Version)
		cmd = putUvarint(cmd, 2, uint64(c.PartitionCount))
		for _, m := range c.Members {
			cmd = putString(cmd, 3, m)
		}
		for _, n := range c.Nodes {
			cmd = protowire.AppendTag(cmd, 4, protowire.BytesType)
			cmd = protowire.AppendBytes(cmd, marshalLayoutNode(n))
		}
		for _, n := range c.Previous {
			cmd = protowire.AppendTag(cmd, 5, protowire.BytesType)
			cmd = protowire.AppendBytes(cmd, marshalLayoutNode(n))
		}
	case RegisterNode:
		atMS, num = c.ProposedAtUnixMS, propRegisterNode
		cmd = putString(cmd, 1, c.NodeID)
		cmd = putString(cmd, 2, c.Host)
		cmd = putString(cmd, 3, c.Zone)
		cmd = putUvarint(cmd, 4, uint64(c.Capacity))
	case SetNodeDraining:
		atMS, num = c.ProposedAtUnixMS, propSetNodeDraining
		cmd = putString(cmd, 1, c.NodeID)
		if c.Draining {
			cmd = putUvarint(cmd, 2, 1)
		}
	default:
		panic(fmt.Sprintf("meta: unencodable proposal type %T", p))
	}

	var b []byte
	b = putUvarint(b, 1, proposalFormatVersion)
	b = putUvarint(b, propAt, uint64(atMS))
	// The command field is emitted even when empty (a zero-value command
	// is still a command); putBytes would omit it.
	b = protowire.AppendTag(b, num, protowire.BytesType)
	b = protowire.AppendBytes(b, cmd)
	return b
}

// DecodeProposal decodes a Raft log entry back into its proposal struct.
// It returns exactly one of the proposal types EncodeProposal accepts.
func DecodeProposal(b []byte) (any, error) {
	var atMS int64
	var num protowire.Number
	var cmd []byte
	seen := false
	d := &dec{b: b}
	for d.next() {
		switch {
		case d.num == 1:
			d.uint32() // format_version: additive, no branching yet
		case d.num == propAt:
			atMS = d.int64()
		case d.num >= propCreateBucket && d.num <= propSetNodeDraining:
			if seen {
				d.fail("proposal carries more than one command")
				break
			}
			seen = true
			num, cmd = d.num, d.bytes()
		default:
			// An envelope field we do not know — including a reserved or
			// future command — cannot be applied safely.
			d.fail("unknown proposal field %d (newer node? upgrade first)", d.num)
		}
	}
	if d.err != nil {
		return nil, d.err
	}
	if !seen {
		return nil, fmt.Errorf("proposal carries no command")
	}
	return decodeCommand(num, atMS, cmd)
}

func decodeCommand(num protowire.Number, atMS int64, b []byte) (any, error) {
	d := &dec{b: b}
	switch num {
	case propCreateBucket:
		c := CreateBucket{ProposedAtUnixMS: atMS}
		for d.next() {
			switch d.num {
			case 1:
				c.Bucket = d.str()
			case 2:
				c.ObjectLockEnabled = d.bool()
			default:
				d.skipUnknown(nil)
			}
		}
		return c, d.err
	case propDeleteBucket:
		c := DeleteBucket{ProposedAtUnixMS: atMS}
		for d.next() {
			switch d.num {
			case 1:
				c.Bucket = d.str()
			default:
				d.skipUnknown(nil)
			}
		}
		return c, d.err
	case propSetVersioning:
		c := SetBucketVersioning{ProposedAtUnixMS: atMS}
		for d.next() {
			switch d.num {
			case 1:
				c.Bucket = d.str()
			case 2:
				c.State = VersioningState(d.enum8())
			default:
				d.skipUnknown(nil)
			}
		}
		return c, d.err
	case propPutObject:
		c := PutObject{ProposedAtUnixMS: atMS}
		for d.next() {
			switch d.num {
			case 1:
				c.Bucket = d.str()
			case 2:
				c.Key = d.str()
			case 3:
				c.VersionID = d.id()
			case 4:
				c.Size = d.int64()
			case 5:
				c.ETag = d.bytes()
			case 6:
				c.ContentType = d.str()
			case 7:
				c.UserMetadata = d.mapEntry(c.UserMetadata)
			case 8:
				c.Partition = d.uvarint()
			case 9:
				c.ECDataShards = d.uint32()
			case 10:
				c.ECParityShards = d.uint32()
			case 11:
				c.ObjectChecksum = d.bytes()
			case 12:
				c.ShardChecksums = append(c.ShardChecksums, d.bytes())
			case 13:
				c.RetentionMode = RetentionMode(d.enum8())
			case 14:
				c.RetainUntilUnixMS = d.int64()
			case 15:
				c.LegalHold = d.bool()
			default:
				d.skipUnknown(nil)
			}
		}
		return c, d.err
	case propDeleteObject:
		c := DeleteObject{ProposedAtUnixMS: atMS}
		for d.next() {
			switch d.num {
			case 1:
				c.Bucket = d.str()
			case 2:
				c.Key = d.str()
			case 3:
				c.VersionID = d.id()
			default:
				d.skipUnknown(nil)
			}
		}
		return c, d.err
	case propDeleteVersion:
		c := DeleteVersion{ProposedAtUnixMS: atMS}
		for d.next() {
			switch d.num {
			case 1:
				c.Bucket = d.str()
			case 2:
				c.Key = d.str()
			case 3:
				c.VersionID = d.id()
			case 4:
				c.BypassGovernance = d.bool()
			default:
				d.skipUnknown(nil)
			}
		}
		return c, d.err
	case propUpdateRetention:
		c := UpdateRetention{ProposedAtUnixMS: atMS}
		for d.next() {
			switch d.num {
			case 1:
				c.Bucket = d.str()
			case 2:
				c.Key = d.str()
			case 3:
				c.VersionID = d.id()
			case 4:
				c.Mode = RetentionMode(d.enum8())
			case 5:
				c.RetainUntilUnixMS = d.int64()
			case 6:
				c.BypassGovernance = d.bool()
			default:
				d.skipUnknown(nil)
			}
		}
		return c, d.err
	case propUpdateLegalHold:
		c := UpdateLegalHold{ProposedAtUnixMS: atMS}
		for d.next() {
			switch d.num {
			case 1:
				c.Bucket = d.str()
			case 2:
				c.Key = d.str()
			case 3:
				c.VersionID = d.id()
			case 4:
				c.Hold = d.bool()
			default:
				d.skipUnknown(nil)
			}
		}
		return c, d.err
	case propCreateUpload:
		c := CreateMultipartUpload{ProposedAtUnixMS: atMS}
		for d.next() {
			switch d.num {
			case 1:
				c.Bucket = d.str()
			case 2:
				c.Key = d.str()
			case 3:
				c.UploadID = d.id()
			case 4:
				c.ContentType = d.str()
			case 5:
				c.UserMetadata = d.mapEntry(c.UserMetadata)
			default:
				d.skipUnknown(nil)
			}
		}
		return c, d.err
	case propUploadPart:
		c := UploadPart{ProposedAtUnixMS: atMS}
		for d.next() {
			switch d.num {
			case 1:
				c.Bucket = d.str()
			case 2:
				c.Key = d.str()
			case 3:
				c.UploadID = d.id()
			case 4:
				c.PartNumber = d.uint32()
			case 5:
				c.DataID = d.id()
			case 6:
				c.Size = d.int64()
			case 7:
				c.ETag = d.bytes()
			case 8:
				c.Checksum = d.bytes()
			default:
				d.skipUnknown(nil)
			}
		}
		return c, d.err
	case propCompleteUpload:
		c := CompleteMultipartUpload{ProposedAtUnixMS: atMS}
		for d.next() {
			switch d.num {
			case 1:
				c.Bucket = d.str()
			case 2:
				c.Key = d.str()
			case 3:
				c.UploadID = d.id()
			case 4:
				c.VersionID = d.id()
			case 5:
				c.ETag = d.bytes()
			case 6:
				part, err := decodeCompletedPart(d.bytes())
				if err != nil {
					d.fail("field 6: completed part: %w", err)
					break
				}
				c.Parts = append(c.Parts, part)
			default:
				d.skipUnknown(nil)
			}
		}
		return c, d.err
	case propAbortUpload:
		c := AbortMultipartUpload{ProposedAtUnixMS: atMS}
		for d.next() {
			switch d.num {
			case 1:
				c.Bucket = d.str()
			case 2:
				c.Key = d.str()
			case 3:
				c.UploadID = d.id()
			default:
				d.skipUnknown(nil)
			}
		}
		return c, d.err
	case propSetLayout:
		c := SetClusterLayout{ProposedAtUnixMS: atMS}
		for d.next() {
			switch d.num {
			case 1:
				c.Version = d.uvarint()
			case 2:
				c.PartitionCount = d.uint32()
			case 3:
				c.Members = append(c.Members, d.str())
			case 4:
				n, err := unmarshalLayoutNode(d.bytes())
				if err != nil {
					d.fail("set_layout node: %w", err)
					break
				}
				c.Nodes = append(c.Nodes, n)
			case 5:
				n, err := unmarshalLayoutNode(d.bytes())
				if err != nil {
					d.fail("set_layout previous node: %w", err)
					break
				}
				c.Previous = append(c.Previous, n)
			default:
				d.skipUnknown(nil)
			}
		}
		return c, d.err
	case propRegisterNode:
		c := RegisterNode{ProposedAtUnixMS: atMS}
		for d.next() {
			switch d.num {
			case 1:
				c.NodeID = d.str()
			case 2:
				c.Host = d.str()
			case 3:
				c.Zone = d.str()
			case 4:
				c.Capacity = d.uint32()
			default:
				d.skipUnknown(nil)
			}
		}
		return c, d.err
	case propSetNodeDraining:
		c := SetNodeDraining{ProposedAtUnixMS: atMS}
		for d.next() {
			switch d.num {
			case 1:
				c.NodeID = d.str()
			case 2:
				c.Draining = d.uvarint() != 0
			default:
				d.skipUnknown(nil)
			}
		}
		return c, d.err
	default:
		return nil, fmt.Errorf("unknown proposal command %d", num)
	}
}

func decodeCompletedPart(b []byte) (CompletedPart, error) {
	var p CompletedPart
	d := &dec{b: b}
	for d.next() {
		switch d.num {
		case 1:
			p.PartNumber = d.uint32()
		case 2:
			p.ETag = d.bytes()
		default:
			d.skipUnknown(nil)
		}
	}
	return p, d.err
}
