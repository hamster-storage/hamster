package datapath

import (
	"errors"
	"fmt"

	"github.com/hamster-storage/hamster/internal/meta"
	"github.com/hamster-storage/hamster/internal/seam"
)

// WriteStream sends one shard to one node: a loop-owned state machine
// pacing chunks under a fixed window of unacknowledged bytes, with
// timer-driven retransmission. The caller pushes bytes with Write as
// Window allows, calls Commit exactly once after the last byte, and hears
// the outcome through the done callback — nil means the shard and its
// commit marker are durable on the target.
//
// The buffer holds only unacknowledged bytes, so memory per stream is
// bounded by the window. The flip side: bytes the receiver acknowledged
// are gone here, so if the receiver crashes mid-transfer and loses its
// staging, the stream fails rather than resends: the receiver answers a
// stream it holds no staging for with silence (see wire.go), so a crashed
// receiver looks like sustained loss and the transfer fails by timeout —
// a failed write for the coordinator's ack rule to absorb, and repair's
// to heal.
type WriteStream struct {
	s      *Service
	to     seam.NodeID
	key    shardKey
	stream uint64 // this incarnation's ID; foreign-stream acks are ignored

	buf  []byte // unacknowledged bytes; buf[0] is at offset base
	base uint64 // cumulative acknowledged offset
	sent uint64 // high-water transmit offset (≥ base, ≤ base+len(buf))

	committing bool // Commit was called: length and checksum are final
	length     uint64
	checksum   []byte

	timer    seam.Timer
	attempts int
	finished bool

	onWindow func()      // window opened: the caller may Write again
	done     func(error) // terminal outcome, exactly once
}

// NewWrite registers a write of shard (id, index) to a node. onWindow
// fires (on the loop) whenever acknowledged progress opens the window;
// done fires exactly once with the outcome. Writing the same shard to the
// same node twice concurrently is a caller bug and panics.
func (s *Service) NewWrite(to seam.NodeID, id meta.VersionID, index uint32, onWindow func(), done func(error)) *WriteStream {
	wk := writeKey{to, shardKey{id, index}}
	if _, dup := s.writes[wk]; dup {
		panic(fmt.Sprintf("datapath: duplicate write of %v to %s", wk.key, to))
	}
	s.nextReq++
	w := &WriteStream{s: s, to: to, key: wk.key, stream: s.nextReq, onWindow: onWindow, done: done}
	s.writes[wk] = w
	w.rearm()
	return w
}

// Window reports how many bytes Write currently accepts.
func (w *WriteStream) Window() int {
	if w.finished {
		return 0
	}
	return windowBytes - len(w.buf)
}

// Write queues p and transmits what the window allows. Pushing more than
// Window is a pacing bug and panics — silently buffering would unbound
// the memory the window exists to bound.
func (w *WriteStream) Write(p []byte) {
	if w.finished {
		return
	}
	if w.committing {
		panic("datapath: Write after Commit")
	}
	if len(p) > w.Window() {
		panic(fmt.Sprintf("datapath: write of %d bytes exceeds the %d-byte window", len(p), w.Window()))
	}
	w.buf = append(w.buf, p...)
	w.pump()
}

// Commit declares the shard complete: length is the total byte count and
// checksum its SHA-256 (the value metadata will record). The commit
// message goes out once every byte has been transmitted.
func (w *WriteStream) Commit(length int64, checksum []byte) {
	if w.finished {
		return
	}
	if w.committing {
		panic("datapath: Commit twice")
	}
	if uint64(length) != w.base+uint64(len(w.buf)) {
		panic(fmt.Sprintf("datapath: commit length %d but %d bytes written", length, w.base+uint64(len(w.buf))))
	}
	w.committing = true
	w.length = uint64(length)
	w.checksum = checksum
	w.pump()
}

// pump transmits whatever is transmittable: unsent buffered chunks, then
// the commit once everything has gone out at least once.
func (w *WriteStream) pump() {
	for w.sent < w.base+uint64(len(w.buf)) {
		start := w.sent - w.base
		end := min(start+uint64(chunkSize), uint64(len(w.buf)))
		w.s.send(w.to, encodeChunk(chunkMsg{key: w.key, offset: w.sent, data: w.buf[start:end], stream: w.stream}))
		w.sent = w.base + end
	}
	if w.committing && w.sent == w.length {
		w.s.send(w.to, encodeCommit(commitMsg{key: w.key, length: w.length, checksum: w.checksum, stream: w.stream}))
	}
}

// handleAck folds in the receiver's cumulative state. Acks echoing a
// different stream belong to another incarnation of this transfer — late
// duplicates, or a predecessor's — and are ignored; within this stream,
// only acks above base carry news (wire.go).
func (w *WriteStream) handleAck(m writeAckMsg) {
	if w.finished || m.stream != w.stream {
		return
	}
	switch {
	case m.errMsg != "":
		w.finish(fmt.Errorf("datapath: writing %v to %s: %s", w.key, w.to, m.errMsg))
	case m.committed:
		w.finish(nil)
	case m.staged > w.base+uint64(len(w.buf)):
		// More than this stream ever sent: a protocol violation, never a
		// state to resume from.
		w.finish(fmt.Errorf("datapath: %s acknowledged %d of %v but only %d were sent", w.to, m.staged, w.key, w.base+uint64(len(w.buf))))
	case m.staged > w.base:
		// Progress: drop acknowledged bytes, reset the failure clock,
		// and let the caller refill the window.
		w.buf = w.buf[m.staged-w.base:]
		w.base = m.staged
		w.attempts = 0
		w.rearm()
		w.pump()
		if w.onWindow != nil && !w.committing {
			w.onWindow()
		}
	default:
		// staged ≤ base: a reordered duplicate of an older ack. The
		// receiver never goes backwards within one staging, so this says
		// nothing new. Ignore.
	}
}

// Abort fails the stream locally — the coordinator's early exit when the
// write as a whole cannot succeed. The receiver's staging times out into
// markerless garbage, which is what staging means. The done callback
// fires with the abort error, exactly once like every other outcome.
func (w *WriteStream) Abort() {
	if !w.finished {
		w.finish(errors.New("datapath: write aborted"))
	}
}

// onTimer retransmits from the acknowledged base after a silent interval,
// and gives up after maxAttempts of them without progress. An idle stream
// — everything acknowledged, no commit pending — is owed no response and
// never times out: the caller may be pacing other shards of the same
// object, and patience here is free.
func (w *WriteStream) onTimer() {
	if w.finished {
		return
	}
	if len(w.buf) == 0 && !w.committing {
		w.rearm()
		return
	}
	w.attempts++
	if w.attempts >= maxAttempts {
		w.finish(fmt.Errorf("datapath: writing %v to %s: no response after %d attempts: %w", w.key, w.to, w.attempts, ErrUnreachable))
		return
	}
	w.sent = w.base // resend everything unacknowledged
	w.pump()
	w.rearm()
}

func (w *WriteStream) rearm() {
	if w.timer != nil {
		w.timer.Stop()
	}
	w.timer = w.s.cfg.Clock.AfterFunc(rto, w.onTimer)
}

func (w *WriteStream) finish(err error) {
	w.finished = true
	if w.timer != nil {
		w.timer.Stop()
	}
	delete(w.s.writes, writeKey{w.to, w.key})
	w.buf = nil
	if w.done != nil {
		w.done(err)
	}
}
