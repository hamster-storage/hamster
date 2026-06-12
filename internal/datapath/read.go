package datapath

import (
	"fmt"

	"github.com/hamster-storage/hamster/internal/meta"
	"github.com/hamster-storage/hamster/internal/seam"
)

// fetch is one outstanding ranged shard read: send, retry on a timer,
// finish once.
type fetch struct {
	s     *Service
	to    seam.NodeID
	reqID uint64
	msg   []byte // the encoded request, resent verbatim

	timer    seam.Timer
	attempts int
	done     func([]byte, error)
}

// Fetch reads length bytes of shard (id, index) at offset from a node.
// done fires exactly once on the loop: the bytes (short only at end of
// file), or the error. Reads beyond maxReadLength are refused by the
// server; callers ask per slice.
func (s *Service) Fetch(to seam.NodeID, id meta.VersionID, index uint32, offset int64, length int, done func([]byte, error)) {
	s.nextReq++
	f := &fetch{
		s: s, to: to, reqID: s.nextReq, done: done,
		msg: encodeRead(readMsg{
			reqID:  s.nextReq,
			key:    shardKey{id, index},
			offset: uint64(offset),
			length: uint64(length),
		}),
	}
	s.reads[f.reqID] = f
	s.send(to, f.msg)
	f.rearm()
}

func (f *fetch) finish(m readResultMsg) {
	delete(f.s.reads, f.reqID)
	if f.timer != nil {
		f.timer.Stop()
	}
	if m.errMsg != "" {
		f.done(nil, fmt.Errorf("datapath: reading from %s: %s", f.to, m.errMsg))
		return
	}
	f.done(m.data, nil)
}

func (f *fetch) onTimer() {
	if _, live := f.s.reads[f.reqID]; !live {
		return
	}
	f.attempts++
	if f.attempts >= maxAttempts {
		delete(f.s.reads, f.reqID)
		f.done(nil, fmt.Errorf("datapath: reading from %s: no response after %d attempts", f.to, f.attempts))
		return
	}
	f.s.send(f.to, f.msg)
	f.rearm()
}

func (f *fetch) rearm() {
	f.timer = f.s.cfg.Clock.AfterFunc(rto, f.onTimer)
}

// pendingDelete is one outstanding shard delete, same retry shape.
type pendingDelete struct {
	s     *Service
	to    seam.NodeID
	reqID uint64
	msg   []byte

	timer    seam.Timer
	attempts int
	done     func(error)
}

// Delete removes shard (id, index) from a node — marker first, then
// bytes, so an interrupted delete leaves garbage, never a lie. Absent
// shards delete successfully (idempotent, like the retries demand).
func (s *Service) Delete(to seam.NodeID, id meta.VersionID, index uint32, done func(error)) {
	s.nextReq++
	d := &pendingDelete{
		s: s, to: to, reqID: s.nextReq, done: done,
		msg: encodeDelete(deleteMsg{reqID: s.nextReq, key: shardKey{id, index}}),
	}
	s.deletes[d.reqID] = d
	s.send(to, d.msg)
	d.rearm()
}

func (d *pendingDelete) finish(errMsg string) {
	delete(d.s.deletes, d.reqID)
	if d.timer != nil {
		d.timer.Stop()
	}
	if errMsg != "" {
		d.done(fmt.Errorf("datapath: deleting from %s: %s", d.to, errMsg))
		return
	}
	d.done(nil)
}

func (d *pendingDelete) onTimer() {
	if _, live := d.s.deletes[d.reqID]; !live {
		return
	}
	d.attempts++
	if d.attempts >= maxAttempts {
		delete(d.s.deletes, d.reqID)
		d.done(fmt.Errorf("datapath: deleting from %s: no response after %d attempts", d.to, d.attempts))
		return
	}
	d.s.send(d.to, d.msg)
	d.rearm()
}

func (d *pendingDelete) rearm() {
	d.timer = d.s.cfg.Clock.AfterFunc(rto, d.onTimer)
}
