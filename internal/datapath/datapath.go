// Package datapath moves shard bytes between nodes ([ADR-0027]): the
// channel envelope that lets Raft and shard traffic share one transport,
// the versioned protobuf shard-transfer messages, the server that stages
// incoming shards durably on a node's disk, and the client state machines
// (WriteStream, Fetch) a coordinator drives.
//
// The transport seam delivers messages unreliably — delayed, reordered,
// duplicated, dropped, across receiver restarts — so the protocol carries
// its own reliability: chunks are addressed by (data ID, shard index,
// offset) and therefore idempotent, acks are cumulative, retransmission is
// timer-driven, and flow control is a fixed window of unacknowledged bytes.
// Built over seam.Clock, seam.Transport, and seam.Disk only, so the
// simulation harness tortures exactly the code that ships.
//
// All Service state is loop-owned: HandleData, NewWrite, Fetch, and every
// callback run on the node's event loop, never concurrently.
//
// Commit protocol: a shard file is durable *and complete* only when its
// commit marker (name + ".ok", an empty file) is durable — the marker is
// synced strictly after the shard bytes, so a file without one is staging
// garbage from a crashed transfer, safe to overwrite or collect. The
// marker carries no checksum: integrity lives in replicated metadata,
// never beside the shard (a shard cannot vouch for itself, [ADR-0026]).
//
// [ADR-0026]: ../../docs/adr/0026-stripe-and-shard-layout.md
// [ADR-0027]: ../../docs/adr/0027-v03-distributed-data-path.md
package datapath

import (
	"encoding/hex"
	"fmt"

	"google.golang.org/protobuf/encoding/protowire"

	"github.com/hamster-storage/hamster/internal/meta"
)

// Transport channels (ADR-0027 decision 6): every inter-node message is
// wrapped in the channel envelope; the node's demux routes by channel.
const (
	ChannelRaft uint64 = 1 // etcd-raft messages, handled by raftnode
	ChannelData uint64 = 2 // shard transfer, handled by this package
)

const envelopeVersion = 1

// Envelope field numbers: 1 format_version, 2 channel, 3 payload.
// Frozen forever; evolution is additive (ADR-0008).

// Wrap encodes one message for the shared transport.
func Wrap(channel uint64, payload []byte) []byte {
	b := make([]byte, 0, len(payload)+8)
	b = putUvarint(b, 1, envelopeVersion)
	b = putUvarint(b, 2, channel)
	b = putBytes(b, 3, payload)
	return b
}

// Unwrap decodes a transport message. An unknown channel is the caller's
// to drop — a newer node may speak channels this binary has never heard of,
// and dropping is always safe on an unreliable transport.
func Unwrap(msg []byte) (channel uint64, payload []byte, err error) {
	version := uint64(0)
	for len(msg) > 0 {
		num, typ, n := protowire.ConsumeTag(msg)
		if n < 0 {
			return 0, nil, fmt.Errorf("datapath: malformed envelope tag")
		}
		msg = msg[n:]
		switch num {
		case 1:
			version, n = consumeUvarint(msg, typ)
		case 2:
			channel, n = consumeUvarint(msg, typ)
		case 3:
			payload, n = consumeBytes(msg, typ)
		default:
			n = protowire.ConsumeFieldValue(num, typ, msg)
		}
		if n < 0 {
			return 0, nil, fmt.Errorf("datapath: malformed envelope field %d", num)
		}
		msg = msg[n:]
	}
	if version == 0 || version > envelopeVersion {
		return 0, nil, fmt.Errorf("datapath: envelope version %d (written by a newer hamster?)", version)
	}
	return channel, payload, nil
}

// shardKey names one shard file: the object's data ID plus the shard index.
type shardKey struct {
	id    meta.VersionID
	index uint32
}

func (k shardKey) String() string {
	return fmt.Sprintf("%s shard %d", hex.EncodeToString(k.id[:]), k.index)
}

// ShardFileName is the disk path of a shard: shards/<32 hex>-<index>.
// Its commit marker is the same name + ".ok".
func ShardFileName(id meta.VersionID, index uint32) string {
	return fmt.Sprintf("shards/%s-%d", hex.EncodeToString(id[:]), index)
}

func markerName(name string) string { return name + ".ok" }

// protowire helpers, proto3 zero-omission like the meta codecs (ADR-0023).

func putUvarint(b []byte, num protowire.Number, v uint64) []byte {
	if v == 0 {
		return b
	}
	b = protowire.AppendTag(b, num, protowire.VarintType)
	return protowire.AppendVarint(b, v)
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

func putBool(b []byte, num protowire.Number, v bool) []byte {
	if !v {
		return b
	}
	b = protowire.AppendTag(b, num, protowire.VarintType)
	return protowire.AppendVarint(b, 1)
}

func consumeUvarint(b []byte, typ protowire.Type) (uint64, int) {
	if typ != protowire.VarintType {
		return 0, -1
	}
	return protowire.ConsumeVarint(b)
}

func consumeBytes(b []byte, typ protowire.Type) ([]byte, int) {
	if typ != protowire.BytesType {
		return nil, -1
	}
	return protowire.ConsumeBytes(b)
}
