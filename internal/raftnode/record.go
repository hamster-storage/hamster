package raftnode

import (
	"fmt"
	"maps"
	"slices"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/hamster-storage/hamster/internal/meta"
	"github.com/hamster-storage/hamster/internal/seam"
)

// One WAL record per Ready: the hard state, entries, and (rarely) the
// snapshot that must be durable before the node speaks. The payload is
// versioned protobuf (invariant 2) wrapping raftpb's own protobuf
// messages:
//
//	message RaftRecord {
//	  uint32 format_version = 1;
//	  raftpb.HardState hard_state = 2;
//	  repeated raftpb.Entry entries = 3;
//	  raftpb.Snapshot snapshot = 4;  // log rotation and MsgSnap; opens a log file
//	}
//
// A snapshot's Data is the metadata store's full row dump plus the address
// book — every member's transport address, which a freshly installed
// replica needs before it can speak to anyone:
//
//	message SnapshotData {
//	  uint32 format_version = 1;
//	  repeated Row rows = 2;        // Row: 1 key, 2 value — meta.Store.Dump order
//	  repeated Member members = 3;  // sorted by raft_id
//	  repeated uint64 removed = 4;  // tombstoned raft IDs, sorted; never re-admitted
//	}
//
//	message Member {
//	  uint64 raft_id = 1;
//	  string addr = 2;  // transport identity (seam.NodeID — the certificate CN)
//	  string dial = 3;  // TCP dial address; empty under the simulator, where identity is the address
//	}
//
// Member doubles as the ConfChange context: an AddNode/AddLearnerNode
// entry carries the new member's address, which is how every replica's
// address book stays consistent with the configuration it applies.
//
// Every message on the cluster transport travels in a versioned envelope
// (invariant 2 — raftpb.Message itself carries no format version), which
// also gives admission a lane of its own:
//
//	message Envelope {
//	  uint32 format_version = 1;
//	  uint32 kind = 2;   // 1 raft (payload: raftpb.Message), 2 admit (payload: Member)
//	  bytes payload = 3;
//	}
//
// An unknown kind is dropped, not an error: an upgraded peer may speak
// kinds this version does not know, and the transport contract already
// allows loss.

const (
	recordFormatVersion   = 1
	snapshotFormatVersion = 1
	envelopeFormatVersion = 1
)

// Envelope kinds.
const (
	kindRaft  = 1
	kindAdmit = 2
)

// record is one decoded WAL frame.
type record struct {
	hs      raftpb.HardState
	entries []raftpb.Entry
	snap    raftpb.Snapshot
}

func encodeRecord(rec record) []byte {
	b := protowire.AppendTag(nil, 1, protowire.VarintType)
	b = protowire.AppendVarint(b, recordFormatVersion)
	if !raft.IsEmptyHardState(rec.hs) {
		b = appendMessage(b, 2, &rec.hs)
	}
	for i := range rec.entries {
		b = appendMessage(b, 3, &rec.entries[i])
	}
	if !raft.IsEmptySnap(rec.snap) {
		b = appendMessage(b, 4, &rec.snap)
	}
	return b
}

// appendMessage frames one raftpb message as a length-delimited field.
func appendMessage(b []byte, num protowire.Number, m interface{ Marshal() ([]byte, error) }) []byte {
	data, err := m.Marshal()
	if err != nil {
		panic(fmt.Sprintf("raftnode: marshal record field %d: %v", num, err))
	}
	b = protowire.AppendTag(b, num, protowire.BytesType)
	return protowire.AppendBytes(b, data)
}

func decodeRecord(b []byte) (record, error) {
	var rec record
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return rec, protowire.ParseError(n)
		}
		b = b[n:]
		if typ != protowire.BytesType {
			n := protowire.ConsumeFieldValue(num, typ, b)
			if n < 0 {
				return rec, protowire.ParseError(n)
			}
			b = b[n:]
			continue
		}
		v, n := protowire.ConsumeBytes(b)
		if n < 0 {
			return rec, protowire.ParseError(n)
		}
		b = b[n:]
		var err error
		switch num {
		case 2:
			err = rec.hs.Unmarshal(v)
		case 3:
			var e raftpb.Entry
			if err = e.Unmarshal(v); err == nil {
				rec.entries = append(rec.entries, e)
			}
		case 4:
			err = rec.snap.Unmarshal(v)
		}
		if err != nil {
			return rec, fmt.Errorf("record field %d: %w", num, err)
		}
	}
	return rec, nil
}

// encodeSnapshotData serializes a store dump, the address book, and the
// removed-ID tombstone as a snapshot payload. Members and removed IDs are
// emitted in raft-ID order.
func encodeSnapshotData(rows []meta.Row, members map[uint64]peerInfo, removed map[uint64]struct{}) []byte {
	b := protowire.AppendTag(nil, 1, protowire.VarintType)
	b = protowire.AppendVarint(b, snapshotFormatVersion)
	for _, r := range rows {
		row := protowire.AppendTag(nil, 1, protowire.BytesType)
		row = protowire.AppendString(row, r.Key)
		row = protowire.AppendTag(row, 2, protowire.BytesType)
		row = protowire.AppendBytes(row, r.Value)
		b = protowire.AppendTag(b, 2, protowire.BytesType)
		b = protowire.AppendBytes(b, row)
	}
	for _, id := range slices.Sorted(maps.Keys(members)) {
		b = protowire.AppendTag(b, 3, protowire.BytesType)
		b = protowire.AppendBytes(b, encodeMember(id, members[id]))
	}
	for _, id := range slices.Sorted(maps.Keys(removed)) {
		b = protowire.AppendTag(b, 4, protowire.VarintType)
		b = protowire.AppendVarint(b, id)
	}
	return b
}

// decodeSnapshotData rebuilds a store, the address book, and the removed-ID
// tombstone from a snapshot payload.
func decodeSnapshotData(b []byte) (*meta.Store, map[uint64]peerInfo, map[uint64]struct{}, error) {
	store := meta.NewStore()
	members := make(map[uint64]peerInfo)
	removed := make(map[uint64]struct{})
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return nil, nil, nil, protowire.ParseError(n)
		}
		b = b[n:]
		if num == 4 && typ == protowire.VarintType {
			id, n := protowire.ConsumeVarint(b)
			if n < 0 {
				return nil, nil, nil, protowire.ParseError(n)
			}
			b = b[n:]
			removed[id] = struct{}{}
			continue
		}
		if (num == 2 || num == 3) && typ == protowire.BytesType {
			v, n := protowire.ConsumeBytes(b)
			if n < 0 {
				return nil, nil, nil, protowire.ParseError(n)
			}
			b = b[n:]
			if num == 3 {
				id, info, err := decodeMember(v)
				if err != nil {
					return nil, nil, nil, err
				}
				members[id] = info
				continue
			}
			key, value, err := decodeSnapshotRow(v)
			if err != nil {
				return nil, nil, nil, err
			}
			if err := store.Restore(key, value); err != nil {
				return nil, nil, nil, err
			}
			continue
		}
		n = protowire.ConsumeFieldValue(num, typ, b)
		if n < 0 {
			return nil, nil, nil, protowire.ParseError(n)
		}
		b = b[n:]
	}
	return store, members, removed, nil
}

// encodeMember serializes one Member record (see the package sketch): a
// snapshot's members entry and the ConfChange context are the same bytes.
func encodeMember(id uint64, info peerInfo) []byte {
	b := protowire.AppendTag(nil, 1, protowire.VarintType)
	b = protowire.AppendVarint(b, id)
	b = protowire.AppendTag(b, 2, protowire.BytesType)
	b = protowire.AppendString(b, string(info.node))
	if info.dial != "" {
		b = protowire.AppendTag(b, 3, protowire.BytesType)
		b = protowire.AppendString(b, info.dial)
	}
	return b
}

func decodeMember(b []byte) (id uint64, info peerInfo, err error) {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return 0, peerInfo{}, protowire.ParseError(n)
		}
		b = b[n:]
		switch {
		case num == 1 && typ == protowire.VarintType:
			v, n := protowire.ConsumeVarint(b)
			if n < 0 {
				return 0, peerInfo{}, protowire.ParseError(n)
			}
			b = b[n:]
			id = v
		case num == 2 && typ == protowire.BytesType:
			v, n := protowire.ConsumeBytes(b)
			if n < 0 {
				return 0, peerInfo{}, protowire.ParseError(n)
			}
			b = b[n:]
			info.node = seam.NodeID(v)
		case num == 3 && typ == protowire.BytesType:
			v, n := protowire.ConsumeBytes(b)
			if n < 0 {
				return 0, peerInfo{}, protowire.ParseError(n)
			}
			b = b[n:]
			info.dial = string(v)
		default:
			n := protowire.ConsumeFieldValue(num, typ, b)
			if n < 0 {
				return 0, peerInfo{}, protowire.ParseError(n)
			}
			b = b[n:]
		}
	}
	return id, info, nil
}

// encodeEnvelope wraps one transport message.
func encodeEnvelope(kind uint64, payload []byte) []byte {
	b := protowire.AppendTag(nil, 1, protowire.VarintType)
	b = protowire.AppendVarint(b, envelopeFormatVersion)
	b = protowire.AppendTag(b, 2, protowire.VarintType)
	b = protowire.AppendVarint(b, kind)
	b = protowire.AppendTag(b, 3, protowire.BytesType)
	b = protowire.AppendBytes(b, payload)
	return b
}

func decodeEnvelope(b []byte) (kind uint64, payload []byte, err error) {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return 0, nil, protowire.ParseError(n)
		}
		b = b[n:]
		switch {
		case num == 2 && typ == protowire.VarintType:
			v, n := protowire.ConsumeVarint(b)
			if n < 0 {
				return 0, nil, protowire.ParseError(n)
			}
			b = b[n:]
			kind = v
		case num == 3 && typ == protowire.BytesType:
			v, n := protowire.ConsumeBytes(b)
			if n < 0 {
				return 0, nil, protowire.ParseError(n)
			}
			b = b[n:]
			payload = v
		default:
			n := protowire.ConsumeFieldValue(num, typ, b)
			if n < 0 {
				return 0, nil, protowire.ParseError(n)
			}
			b = b[n:]
		}
	}
	return kind, payload, nil
}

func decodeSnapshotRow(b []byte) (key string, value []byte, err error) {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return "", nil, protowire.ParseError(n)
		}
		b = b[n:]
		if typ != protowire.BytesType {
			return "", nil, fmt.Errorf("snapshot row field %d: unexpected wire type %d", num, typ)
		}
		v, n := protowire.ConsumeBytes(b)
		if n < 0 {
			return "", nil, protowire.ParseError(n)
		}
		b = b[n:]
		switch num {
		case 1:
			key = string(v)
		case 2:
			value = append([]byte(nil), v...)
		}
	}
	return key, value, nil
}
