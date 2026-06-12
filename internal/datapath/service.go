package datapath

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io/fs"
	"time"

	"github.com/hamster-storage/hamster/internal/seam"
)

// Tunables. Variables so tests can tighten them; production uses the
// defaults. None of these affect formats — only pacing.
var (
	// chunkSize is the payload of one ShardChunk message.
	chunkSize = 64 << 10
	// windowBytes bounds unacknowledged bytes per shard write stream —
	// the sender's whole buffer, so also its memory bound.
	windowBytes = 512 << 10
	// rto is the retransmission timeout for writes and the retry interval
	// for reads and deletes.
	rto = 500 * time.Millisecond
	// maxAttempts is how many timer firings without progress fail an
	// operation. Progress resets the count.
	maxAttempts = 20
)

// Config carries a Service's world: the node's clock, transport, and disk.
type Config struct {
	Clock     seam.Clock
	Transport seam.Transport
	Disk      seam.Disk
}

// Service is one node's data-plane endpoint: the server side (staging
// incoming shards, serving reads and deletes) and the registry of the
// node's own outgoing operations (WriteStream, Fetch, Delete).
//
// Loop-owned: HandleData and every method run on the node's event loop.
// Construct it in the node's boot function; in-flight server staging dies
// with the process by design — the commit marker is what survival means.
type Service struct {
	cfg Config

	staging map[shardKey]*staging     // server: in-flight incoming writes
	writes  map[writeKey]*WriteStream // client: outgoing writes
	reads   map[uint64]*fetch         // client: outgoing reads
	deletes map[uint64]*pendingDelete // client: outgoing deletes
	nextReq uint64
}

type writeKey struct {
	to  seam.NodeID
	key shardKey
}

// staging is the receiver's state for one incoming shard: how much is
// staged on disk and the running hash of it. It exists only between the
// first chunk and the commit; a crash discards it, and the sender's
// restart-from-zero rule rebuilds it.
type staging struct {
	stream uint64
	staged uint64
	hash   hash.Hash
}

// New returns a Service over cfg.
func New(cfg Config) *Service {
	return &Service{
		cfg:     cfg,
		staging: make(map[shardKey]*staging),
		writes:  make(map[writeKey]*WriteStream),
		reads:   make(map[uint64]*fetch),
		deletes: make(map[uint64]*pendingDelete),
	}
}

// HandleData dispatches one data-channel message (the payload Unwrap
// returned for ChannelData). Errors mean a malformed or unknown message;
// the caller may log and must otherwise drop — the peer's timer treats
// silence as loss.
func (s *Service) HandleData(from seam.NodeID, payload []byte) error {
	cmd, body, err := decodeMessage(payload)
	if err != nil {
		return err
	}
	switch cmd {
	case msgChunk:
		m, err := decodeChunk(body)
		if err != nil {
			return err
		}
		s.handleChunk(from, m)
	case msgCommit:
		m, err := decodeCommit(body)
		if err != nil {
			return err
		}
		s.handleCommit(from, m)
	case msgWriteAck:
		m, err := decodeWriteAck(body)
		if err != nil {
			return err
		}
		if w := s.writes[writeKey{from, m.key}]; w != nil {
			w.handleAck(m)
		}
	case msgRead:
		m, err := decodeRead(body)
		if err != nil {
			return err
		}
		s.handleRead(from, m)
	case msgReadResult:
		m, err := decodeReadResult(body)
		if err != nil {
			return err
		}
		if f := s.reads[m.reqID]; f != nil {
			f.finish(m)
		}
	case msgDelete:
		m, err := decodeDelete(body)
		if err != nil {
			return err
		}
		s.handleDelete(from, m)
	case msgDeleteAck:
		m, err := decodeDeleteAck(body)
		if err != nil {
			return err
		}
		if d := s.deletes[m.reqID]; d != nil {
			d.finish(m.errMsg)
		}
	}
	return nil
}

// send wraps and sends one data-channel message.
func (s *Service) send(to seam.NodeID, msg []byte) {
	s.cfg.Transport.Send(to, Wrap(ChannelData, msg))
}

// committed reports whether the shard's commit marker exists — the one
// fact that survives restarts. A zero-length ReadFileAt is the existence
// probe.
func (s *Service) committed(key shardKey) bool {
	_, err := s.cfg.Disk.ReadFileAt(markerName(ShardFileName(key.id, key.index)), 0, 0)
	return err == nil
}

// handleChunk is the receiving half of a shard write. Chunks are
// idempotent — addressed by (stream, key, offset) — and the reply is the
// staging's cumulative offset, so duplicates, gaps, and retransmissions
// all collapse to "here is how far we are". The case analysis is ordered
// by who owns the staging; see wire.go for why each answer is sound under
// reordering and duplication.
func (s *Service) handleChunk(from seam.NodeID, m chunkMsg) {
	name := ShardFileName(m.key.id, m.key.index)
	st := s.staging[m.key]

	switch {
	case st == nil && s.committed(m.key):
		// A straggler from a transfer that already finished. The durable
		// shard is immutable; re-acknowledge and touch nothing.
		s.send(from, encodeWriteAck(writeAckMsg{key: m.key, committed: true, stream: m.stream}))
		return

	case st != nil && st.stream == m.stream:
		// The staging's own stream. Exactly the next chunk appends; a
		// duplicate (below staged) or a gap (above) just earns the
		// cumulative ack. A duplicate of the first chunk lands here too —
		// it must never reset live staging.
		if m.offset == st.staged {
			if err := s.cfg.Disk.Append(name, m.data); err != nil {
				s.fail(from, m.key, st.stream, name, fmt.Sprintf("appending: %v", err))
				return
			}
			st.hash.Write(m.data)
			st.staged += uint64(len(m.data))
		}
		s.send(from, encodeWriteAck(writeAckMsg{key: m.key, staged: st.staged, stream: st.stream}))

	case m.offset == 0 && (st == nil || m.stream > st.stream):
		// A new incarnation starting (over leftover staging of a dead
		// predecessor, or fresh). WriteFile replaces whatever staging
		// garbage exists. The monotonic-stream guard keeps a *stale*
		// first chunk from clobbering a newer incarnation's staging;
		// streams from one Service counter are monotonic, which is the
		// v0.3 shape — one coordinator per object's write.
		if err := s.cfg.Disk.WriteFile(name, m.data); err != nil {
			s.send(from, encodeWriteAck(writeAckMsg{key: m.key, errMsg: err.Error(), stream: m.stream}))
			return
		}
		st = &staging{stream: m.stream, staged: uint64(len(m.data)), hash: sha256.New()}
		st.hash.Write(m.data)
		s.staging[m.key] = st
		s.send(from, encodeWriteAck(writeAckMsg{key: m.key, staged: st.staged, stream: st.stream}))

	default:
		// A mid-transfer chunk with no staging for its stream (this
		// process restarted, or the stream's first chunk has not arrived
		// yet), or a stale incarnation's chunk while another stream owns
		// the staging. Silence, in every case: any answer here is
		// ambiguous under reordering (see wire.go), and the sender's
		// timer already treats silence correctly.
	}
}

// handleCommit finishes a shard write: verify length and checksum, sync
// the shard, then write and sync the commit marker — strictly in that
// order, so a durable marker proves durable, complete, checksum-true
// bytes. Only then is durability acknowledged.
func (s *Service) handleCommit(from seam.NodeID, m commitMsg) {
	name := ShardFileName(m.key.id, m.key.index)
	st := s.staging[m.key]

	if m.length == 0 {
		// Shard files always carry at least a header; a zero-length
		// commit is a protocol error, refused terminally.
		s.fail(from, m.key, m.stream, name, "zero-length shard refused")
		return
	}
	if st == nil {
		if s.committed(m.key) {
			// Duplicate commit after success: re-acknowledge.
			s.send(from, encodeWriteAck(writeAckMsg{key: m.key, committed: true, stream: m.stream}))
		}
		// Nothing staged (restarted, or the commit overtook every chunk):
		// silence, as for chunks — the sender retransmits or times out.
		return
	}
	if m.stream != st.stream {
		// A commit must never seal bytes another incarnation staged.
		// Silence; the live staging answers to its own stream only.
		return
	}
	if st.staged != m.length {
		// Not all bytes arrived yet (the commit overtook chunks, or some
		// were dropped): the cumulative ack resumes the sender.
		s.send(from, encodeWriteAck(writeAckMsg{key: m.key, staged: st.staged, stream: st.stream}))
		return
	}
	if sum := st.hash.Sum(nil); !bytes.Equal(sum, m.checksum) {
		// The staged bytes do not match what metadata is about to record.
		// Terminal: the sender must not count this shard as durable.
		s.fail(from, m.key, st.stream, name, "staged shard fails its checksum")
		return
	}
	if err := s.cfg.Disk.Sync(name); err != nil {
		s.fail(from, m.key, st.stream, name, fmt.Sprintf("syncing shard: %v", err))
		return
	}
	marker := markerName(name)
	if err := s.cfg.Disk.WriteFile(marker, nil); err != nil {
		s.fail(from, m.key, st.stream, name, fmt.Sprintf("writing commit marker: %v", err))
		return
	}
	if err := s.cfg.Disk.Sync(marker); err != nil {
		s.fail(from, m.key, st.stream, name, fmt.Sprintf("syncing commit marker: %v", err))
		return
	}
	delete(s.staging, m.key)
	s.send(from, encodeWriteAck(writeAckMsg{key: m.key, committed: true, stream: st.stream}))
}

// fail abandons an incoming transfer: drop the staging state, remove the
// partial file (best effort — leftovers without a marker are garbage by
// definition), and send a terminal error ack.
func (s *Service) fail(from seam.NodeID, key shardKey, stream uint64, name, msg string) {
	delete(s.staging, key)
	if err := s.cfg.Disk.Remove(name); err == nil {
		_ = s.cfg.Disk.Sync(name)
	}
	s.send(from, encodeWriteAck(writeAckMsg{key: key, errMsg: msg, stream: stream}))
}

// handleRead serves a ranged shard read. Only committed shards are
// readable: a transfer's staging is nobody's business, and a file without
// a marker is garbage.
func (s *Service) handleRead(from seam.NodeID, m readMsg) {
	if !s.committed(m.key) {
		s.send(from, encodeReadResult(readResultMsg{reqID: m.reqID, errMsg: fmt.Sprintf("no committed shard %v", m.key)}))
		return
	}
	if m.length > uint64(maxReadLength) {
		s.send(from, encodeReadResult(readResultMsg{reqID: m.reqID, errMsg: fmt.Sprintf("read of %d bytes exceeds the %d limit", m.length, maxReadLength)}))
		return
	}
	data, err := s.cfg.Disk.ReadFileAt(ShardFileName(m.key.id, m.key.index), int64(m.offset), int(m.length))
	if err != nil {
		s.send(from, encodeReadResult(readResultMsg{reqID: m.reqID, errMsg: err.Error()}))
		return
	}
	s.send(from, encodeReadResult(readResultMsg{reqID: m.reqID, data: data}))
}

// handleDelete removes a shard: marker first, then bytes, each synced —
// so a crash in between leaves bytes without a marker, which is garbage,
// never a marker without trustworthy bytes. Deleting what is absent is
// idempotent success.
func (s *Service) handleDelete(from seam.NodeID, m deleteMsg) {
	name := ShardFileName(m.key.id, m.key.index)
	for _, f := range []string{markerName(name), name} {
		err := s.cfg.Disk.Remove(f)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err == nil {
			err = s.cfg.Disk.Sync(f)
		}
		if err != nil {
			s.send(from, encodeDeleteAck(deleteAckMsg{reqID: m.reqID, errMsg: err.Error()}))
			return
		}
	}
	s.send(from, encodeDeleteAck(deleteAckMsg{reqID: m.reqID}))
}

// maxReadLength caps one read response message. Readers ask for slices
// (256 KiB), so the cap is comfortably above need while keeping any
// single message bounded.
var maxReadLength = 1 << 20
