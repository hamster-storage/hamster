package raftnode

import (
	"fmt"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/encoding/protowire"
)

// One WAL record per Ready: the hard state and entries that must be
// durable before the node speaks. The payload is versioned protobuf
// (invariant 2) wrapping raftpb's own protobuf messages:
//
//	message RaftRecord {
//	  uint32 format_version = 1;
//	  raftpb.HardState hard_state = 2;
//	  repeated raftpb.Entry entries = 3;
//	}

const recordFormatVersion = 1

func encodeRecord(hs raftpb.HardState, entries []raftpb.Entry) []byte {
	b := protowire.AppendTag(nil, 1, protowire.VarintType)
	b = protowire.AppendVarint(b, recordFormatVersion)
	if !raft.IsEmptyHardState(hs) {
		data, err := hs.Marshal()
		if err != nil {
			panic(fmt.Sprintf("raftnode: marshal hard state: %v", err))
		}
		b = protowire.AppendTag(b, 2, protowire.BytesType)
		b = protowire.AppendBytes(b, data)
	}
	for _, e := range entries {
		data, err := e.Marshal()
		if err != nil {
			panic(fmt.Sprintf("raftnode: marshal entry: %v", err))
		}
		b = protowire.AppendTag(b, 3, protowire.BytesType)
		b = protowire.AppendBytes(b, data)
	}
	return b
}

func decodeRecord(rec []byte) (raftpb.HardState, []raftpb.Entry, error) {
	var hs raftpb.HardState
	var entries []raftpb.Entry
	for len(rec) > 0 {
		num, typ, n := protowire.ConsumeTag(rec)
		if n < 0 {
			return hs, nil, protowire.ParseError(n)
		}
		rec = rec[n:]
		switch {
		case num == 1 && typ == protowire.VarintType:
			_, n := protowire.ConsumeVarint(rec)
			if n < 0 {
				return hs, nil, protowire.ParseError(n)
			}
			rec = rec[n:]
		case num == 2 && typ == protowire.BytesType:
			v, n := protowire.ConsumeBytes(rec)
			if n < 0 {
				return hs, nil, protowire.ParseError(n)
			}
			if err := hs.Unmarshal(v); err != nil {
				return hs, nil, fmt.Errorf("hard state: %w", err)
			}
			rec = rec[n:]
		case num == 3 && typ == protowire.BytesType:
			v, n := protowire.ConsumeBytes(rec)
			if n < 0 {
				return hs, nil, protowire.ParseError(n)
			}
			var e raftpb.Entry
			if err := e.Unmarshal(v); err != nil {
				return hs, nil, fmt.Errorf("entry: %w", err)
			}
			entries = append(entries, e)
			rec = rec[n:]
		default:
			n := protowire.ConsumeFieldValue(num, typ, rec)
			if n < 0 {
				return hs, nil, protowire.ParseError(n)
			}
			rec = rec[n:]
		}
	}
	return hs, entries, nil
}
