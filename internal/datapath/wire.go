package datapath

import (
	"fmt"

	"google.golang.org/protobuf/encoding/protowire"

	"github.com/hamster-storage/hamster/internal/meta"
)

// The data-channel message set. One DataMessage envelope, exactly one
// command per message, hand-written protowire like the metadata codecs
// (ADR-0023). Field numbers are frozen forever; unknown fields inside a
// known command are skipped (additive evolution); an unknown command is
// an error the receiver drops — always safe on an unreliable transport,
// because the sender's timer treats silence as loss.
//
//	DataMessage: 1 format_version, then one of:
//	  2 chunk      ShardChunk:     1 data_id, 2 index, 3 offset, 4 data,
//	                               5 stream
//	  3 commit     ShardCommit:    1 data_id, 2 index, 3 length,
//	                               4 checksum, 5 stream
//	  4 write_ack  ShardWriteAck:  1 data_id, 2 index, 3 staged,
//	                               4 committed, 5 error, 6 stream
//	  5 read       ShardRead:      1 req_id, 2 data_id, 3 index,
//	                               4 offset, 5 length
//	  6 read_result ShardReadResult: 1 req_id, 2 data, 3 error
//	  7 delete     ShardDelete:    1 req_id, 2 data_id, 3 index
//	  8 delete_ack ShardDeleteAck: 1 req_id, 2 error
//	  9 verify     ShardVerify:    1 req_id, 2 data_id, 3 index
//	  10 verify_result ShardVerifyResult: 1 req_id, 2 committed,
//	                               3 checksum, 4 length, 5 error
//
// The stream ID names one sender incarnation of one transfer; acks echo
// it, and the sender ignores acks from any other incarnation — so a
// stale duplicate ack can never masquerade as current state. Within one
// stream, acks only ever grow (the receiver never goes backwards within
// one staging), so anything at or below the sender's base is a reordered
// duplicate, ignored. And a receiver holding no staging for a mid-stream
// message answers with *silence*: any stateful answer it could give is
// ambiguous under reordering (is "I have nothing" a crash, or generated
// before the transfer's first chunk arrived?), while silence simply
// routes the decision to the sender's retransmit timer — a genuinely
// restarted receiver looks like sustained loss and fails the transfer by
// timeout, which is unambiguous, merely slower. Speed is the
// coordinator's policy problem; corruption would be everyone's.
//
// The commit carries the shard's SHA-256 — the same value the metadata
// commit is about to record — and the receiver verifies its incrementally
// hashed staging against it before acknowledging durability. A shard
// counted toward the ack rule therefore provably matches the checksum
// that will name it; readers re-verify against metadata on every read
// regardless.

const wireVersion = 1

const (
	msgChunk      = 2
	msgCommit     = 3
	msgWriteAck   = 4
	msgRead       = 5
	msgReadResult = 6
	msgDelete     = 7
	msgDeleteAck  = 8
	msgVerify     = 9
	msgVerifyAck  = 10
)

type chunkMsg struct {
	key    shardKey
	offset uint64
	data   []byte
	stream uint64
}

type commitMsg struct {
	key      shardKey
	length   uint64
	checksum []byte
	stream   uint64
}

type writeAckMsg struct {
	key       shardKey
	staged    uint64
	committed bool
	errMsg    string
	stream    uint64
}

type readMsg struct {
	reqID  uint64
	key    shardKey
	offset uint64
	length uint64
}

type readResultMsg struct {
	reqID  uint64
	data   []byte
	errMsg string
}

type deleteMsg struct {
	reqID uint64
	key   shardKey
}

type deleteAckMsg struct {
	reqID  uint64
	errMsg string
}

type verifyMsg struct {
	reqID uint64
	key   shardKey
}

type verifyAckMsg struct {
	reqID     uint64
	committed bool
	checksum  []byte
	length    uint64
	errMsg    string
}

func encodeMessage(num protowire.Number, cmd []byte) []byte {
	b := make([]byte, 0, len(cmd)+8)
	b = putUvarint(b, 1, wireVersion)
	b = putBytes(b, num, cmd)
	return b
}

func encodeChunk(m chunkMsg) []byte {
	var c []byte
	c = putBytes(c, 1, m.key.id[:])
	c = putUvarint(c, 2, uint64(m.key.index))
	c = putUvarint(c, 3, m.offset)
	c = putBytes(c, 4, m.data)
	c = putUvarint(c, 5, m.stream)
	return encodeMessage(msgChunk, c)
}

func encodeCommit(m commitMsg) []byte {
	var c []byte
	c = putBytes(c, 1, m.key.id[:])
	c = putUvarint(c, 2, uint64(m.key.index))
	c = putUvarint(c, 3, m.length)
	c = putBytes(c, 4, m.checksum)
	c = putUvarint(c, 5, m.stream)
	return encodeMessage(msgCommit, c)
}

func encodeWriteAck(m writeAckMsg) []byte {
	var c []byte
	c = putBytes(c, 1, m.key.id[:])
	c = putUvarint(c, 2, uint64(m.key.index))
	c = putUvarint(c, 3, m.staged)
	c = putBool(c, 4, m.committed)
	c = putString(c, 5, m.errMsg)
	c = putUvarint(c, 6, m.stream)
	return encodeMessage(msgWriteAck, c)
}

func encodeRead(m readMsg) []byte {
	var c []byte
	c = putUvarint(c, 1, m.reqID)
	c = putBytes(c, 2, m.key.id[:])
	c = putUvarint(c, 3, uint64(m.key.index))
	c = putUvarint(c, 4, m.offset)
	c = putUvarint(c, 5, m.length)
	return encodeMessage(msgRead, c)
}

func encodeReadResult(m readResultMsg) []byte {
	var c []byte
	c = putUvarint(c, 1, m.reqID)
	c = putBytes(c, 2, m.data)
	c = putString(c, 3, m.errMsg)
	return encodeMessage(msgReadResult, c)
}

func encodeDelete(m deleteMsg) []byte {
	var c []byte
	c = putUvarint(c, 1, m.reqID)
	c = putBytes(c, 2, m.key.id[:])
	c = putUvarint(c, 3, uint64(m.key.index))
	return encodeMessage(msgDelete, c)
}

func encodeDeleteAck(m deleteAckMsg) []byte {
	var c []byte
	c = putUvarint(c, 1, m.reqID)
	c = putString(c, 2, m.errMsg)
	return encodeMessage(msgDeleteAck, c)
}

func encodeVerify(m verifyMsg) []byte {
	var c []byte
	c = putUvarint(c, 1, m.reqID)
	c = putBytes(c, 2, m.key.id[:])
	c = putUvarint(c, 3, uint64(m.key.index))
	return encodeMessage(msgVerify, c)
}

func encodeVerifyAck(m verifyAckMsg) []byte {
	var c []byte
	c = putUvarint(c, 1, m.reqID)
	c = putBool(c, 2, m.committed)
	c = putBytes(c, 3, m.checksum)
	c = putUvarint(c, 4, m.length)
	c = putString(c, 5, m.errMsg)
	return encodeMessage(msgVerifyAck, c)
}

// decodeMessage splits a data-channel payload into its command number and
// command bytes, validating the version.
func decodeMessage(b []byte) (cmd protowire.Number, body []byte, err error) {
	version := uint64(0)
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return 0, nil, fmt.Errorf("datapath: malformed message tag")
		}
		b = b[n:]
		switch {
		case num == 1:
			version, n = consumeUvarint(b, typ)
		case num >= msgChunk && num <= msgVerifyAck:
			if cmd != 0 {
				return 0, nil, fmt.Errorf("datapath: message carries two commands")
			}
			cmd = num
			body, n = consumeBytes(b, typ)
		default:
			return 0, nil, fmt.Errorf("datapath: unknown command %d", num)
		}
		if n < 0 {
			return 0, nil, fmt.Errorf("datapath: malformed message field %d", num)
		}
		b = b[n:]
	}
	if version == 0 || version > wireVersion {
		return 0, nil, fmt.Errorf("datapath: message version %d (written by a newer hamster?)", version)
	}
	if cmd == 0 {
		return 0, nil, fmt.Errorf("datapath: message carries no command")
	}
	return cmd, body, nil
}

// fieldScan walks one command's fields, dispatching each to fn; unknown
// fields are skipped (additive evolution within a command is legal).
func fieldScan(b []byte, fn func(num protowire.Number, typ protowire.Type, b []byte) int) error {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return fmt.Errorf("datapath: malformed field tag")
		}
		b = b[n:]
		if used := fn(num, typ, b); used != 0 {
			n = used
		} else {
			n = protowire.ConsumeFieldValue(num, typ, b)
		}
		if n < 0 {
			return fmt.Errorf("datapath: malformed field %d", num)
		}
		b = b[n:]
	}
	return nil
}

func consumeID(b []byte, typ protowire.Type, id *meta.VersionID) int {
	v, n := consumeBytes(b, typ)
	if n < 0 || len(v) != len(id) {
		return -1
	}
	copy(id[:], v)
	return n
}

func decodeChunk(b []byte) (m chunkMsg, err error) {
	err = fieldScan(b, func(num protowire.Number, typ protowire.Type, b []byte) (n int) {
		switch num {
		case 1:
			n = consumeID(b, typ, &m.key.id)
		case 2:
			var v uint64
			v, n = consumeUvarint(b, typ)
			m.key.index = uint32(v)
		case 3:
			m.offset, n = consumeUvarint(b, typ)
		case 4:
			m.data, n = consumeBytes(b, typ)
		case 5:
			m.stream, n = consumeUvarint(b, typ)
		}
		return n
	})
	return m, err
}

func decodeCommit(b []byte) (m commitMsg, err error) {
	err = fieldScan(b, func(num protowire.Number, typ protowire.Type, b []byte) (n int) {
		switch num {
		case 1:
			n = consumeID(b, typ, &m.key.id)
		case 2:
			var v uint64
			v, n = consumeUvarint(b, typ)
			m.key.index = uint32(v)
		case 3:
			m.length, n = consumeUvarint(b, typ)
		case 4:
			m.checksum, n = consumeBytes(b, typ)
		case 5:
			m.stream, n = consumeUvarint(b, typ)
		}
		return n
	})
	return m, err
}

func decodeWriteAck(b []byte) (m writeAckMsg, err error) {
	err = fieldScan(b, func(num protowire.Number, typ protowire.Type, b []byte) (n int) {
		switch num {
		case 1:
			n = consumeID(b, typ, &m.key.id)
		case 2:
			var v uint64
			v, n = consumeUvarint(b, typ)
			m.key.index = uint32(v)
		case 3:
			m.staged, n = consumeUvarint(b, typ)
		case 4:
			var v uint64
			v, n = consumeUvarint(b, typ)
			m.committed = v != 0
		case 5:
			var s []byte
			s, n = consumeBytes(b, typ)
			m.errMsg = string(s)
		case 6:
			m.stream, n = consumeUvarint(b, typ)
		}
		return n
	})
	return m, err
}

func decodeRead(b []byte) (m readMsg, err error) {
	err = fieldScan(b, func(num protowire.Number, typ protowire.Type, b []byte) (n int) {
		switch num {
		case 1:
			m.reqID, n = consumeUvarint(b, typ)
		case 2:
			n = consumeID(b, typ, &m.key.id)
		case 3:
			var v uint64
			v, n = consumeUvarint(b, typ)
			m.key.index = uint32(v)
		case 4:
			m.offset, n = consumeUvarint(b, typ)
		case 5:
			m.length, n = consumeUvarint(b, typ)
		}
		return n
	})
	return m, err
}

func decodeReadResult(b []byte) (m readResultMsg, err error) {
	err = fieldScan(b, func(num protowire.Number, typ protowire.Type, b []byte) (n int) {
		switch num {
		case 1:
			m.reqID, n = consumeUvarint(b, typ)
		case 2:
			m.data, n = consumeBytes(b, typ)
		case 3:
			var s []byte
			s, n = consumeBytes(b, typ)
			m.errMsg = string(s)
		}
		return n
	})
	return m, err
}

func decodeDelete(b []byte) (m deleteMsg, err error) {
	err = fieldScan(b, func(num protowire.Number, typ protowire.Type, b []byte) (n int) {
		switch num {
		case 1:
			m.reqID, n = consumeUvarint(b, typ)
		case 2:
			n = consumeID(b, typ, &m.key.id)
		case 3:
			var v uint64
			v, n = consumeUvarint(b, typ)
			m.key.index = uint32(v)
		}
		return n
	})
	return m, err
}

func decodeDeleteAck(b []byte) (m deleteAckMsg, err error) {
	err = fieldScan(b, func(num protowire.Number, typ protowire.Type, b []byte) (n int) {
		switch num {
		case 1:
			m.reqID, n = consumeUvarint(b, typ)
		case 2:
			var s []byte
			s, n = consumeBytes(b, typ)
			m.errMsg = string(s)
		}
		return n
	})
	return m, err
}

func decodeVerify(b []byte) (m verifyMsg, err error) {
	err = fieldScan(b, func(num protowire.Number, typ protowire.Type, b []byte) (n int) {
		switch num {
		case 1:
			m.reqID, n = consumeUvarint(b, typ)
		case 2:
			n = consumeID(b, typ, &m.key.id)
		case 3:
			var v uint64
			v, n = consumeUvarint(b, typ)
			m.key.index = uint32(v)
		}
		return n
	})
	return m, err
}

func decodeVerifyAck(b []byte) (m verifyAckMsg, err error) {
	err = fieldScan(b, func(num protowire.Number, typ protowire.Type, b []byte) (n int) {
		switch num {
		case 1:
			m.reqID, n = consumeUvarint(b, typ)
		case 2:
			var v uint64
			v, n = consumeUvarint(b, typ)
			m.committed = v != 0
		case 3:
			m.checksum, n = consumeBytes(b, typ)
		case 4:
			m.length, n = consumeUvarint(b, typ)
		case 5:
			var s []byte
			s, n = consumeBytes(b, typ)
			m.errMsg = string(s)
		}
		return n
	})
	return m, err
}
