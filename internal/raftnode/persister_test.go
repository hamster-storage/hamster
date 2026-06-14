package raftnode_test

import (
	"encoding/binary"
	"fmt"
	"slices"

	"github.com/hamster-storage/hamster/internal/meta"
	"github.com/hamster-storage/hamster/internal/seam"
	"github.com/hamster-storage/hamster/internal/wal"
)

// simPersister is a deterministic, crash-faithful raftnode.MetaPersister for
// the simulation harness, where BadgerDB cannot run. It stands in for the
// production BadgerDB store with the same contract: CommitAt and ResetAt make
// the rows and the Raft applied index durable together in one atomic frame,
// LoadState reads them back, and a crash mid-write loses at most the un-synced
// final frame. It is built on the same WAL framing raftnode's own log uses
// (length + CRC, synced on append, torn-tail-tolerant on replay), so the
// harness exercises the real persister integration — apply, the
// skip-don't-reapply at boot, the snapshot-install reset, and the WAL-rebuild
// fallback — under adversarial crash and restart schedules.
type simPersister struct {
	log   *wal.Log
	rows  map[string][]byte
	index uint64
	any   bool // a commit or reset has been written (distinguishes empty from fresh)
}

// openSimPersister opens the persister's frame log on disk and replays it,
// reconstructing the surviving rows and the applied index — the crash-recovery
// path, run on every (re)boot.
func openSimPersister(disk seam.Disk, name string) (*simPersister, error) {
	log, records, err := wal.Open(disk, name)
	if err != nil {
		return nil, err
	}
	p := &simPersister{log: log, rows: make(map[string][]byte)}
	for i, rec := range records {
		index, reset, rows, err := decodeFrame(rec)
		if err != nil {
			return nil, fmt.Errorf("sim persister: frame %d: %w", i, err)
		}
		if reset {
			clear(p.rows)
		}
		p.apply(rows)
		p.index = index
		p.any = true
	}
	return p, nil
}

func (p *simPersister) apply(rows []meta.Row) {
	for _, r := range rows {
		if r.Value == nil {
			delete(p.rows, r.Key)
		} else {
			p.rows[r.Key] = append([]byte(nil), r.Value...)
		}
	}
}

func (p *simPersister) CommitAt(index uint64, rows []meta.Row) error {
	if err := p.log.Append(encodeFrame(index, false, rows)); err != nil {
		return err
	}
	p.apply(rows)
	p.index = index
	p.any = true
	return nil
}

func (p *simPersister) ResetAt(index uint64, rows []meta.Row) error {
	if err := p.log.Append(encodeFrame(index, true, rows)); err != nil {
		return err
	}
	clear(p.rows)
	p.apply(rows)
	p.index = index
	p.any = true
	return nil
}

func (p *simPersister) LoadState() (rows []meta.Row, appliedIndex uint64, ok bool, err error) {
	if !p.any {
		return nil, 0, false, nil
	}
	rows = make([]meta.Row, 0, len(p.rows))
	for k, v := range p.rows {
		rows = append(rows, meta.Row{Key: k, Value: v})
	}
	// Sorted, like a real key-value store's scan, so the load is deterministic.
	slices.SortFunc(rows, func(a, b meta.Row) int {
		if a.Key < b.Key {
			return -1
		}
		if a.Key > b.Key {
			return 1
		}
		return 0
	})
	return rows, p.index, true, nil
}

// encodeFrame lays out one frame: [reset:1][index:8][nRows:4] then per row
// [keyLen:4][key][tombstone:1] and, unless a tombstone, [valLen:4][value]. The
// WAL wraps it with a length and CRC; this is just the payload.
func encodeFrame(index uint64, reset bool, rows []meta.Row) []byte {
	var b []byte
	if reset {
		b = append(b, 1)
	} else {
		b = append(b, 0)
	}
	b = binary.BigEndian.AppendUint64(b, index)
	b = binary.BigEndian.AppendUint32(b, uint32(len(rows)))
	for _, r := range rows {
		b = binary.BigEndian.AppendUint32(b, uint32(len(r.Key)))
		b = append(b, r.Key...)
		if r.Value == nil {
			b = append(b, 1)
			continue
		}
		b = append(b, 0)
		b = binary.BigEndian.AppendUint32(b, uint32(len(r.Value)))
		b = append(b, r.Value...)
	}
	return b
}

func decodeFrame(b []byte) (index uint64, reset bool, rows []meta.Row, err error) {
	// The WAL returns only intact (CRC-verified) frames, so a short read here
	// is a bug, not a torn tail; recover turns it into a clean error.
	defer func() {
		if recover() != nil {
			err = fmt.Errorf("short frame")
		}
	}()
	reset = b[0] != 0
	b = b[1:]
	index = binary.BigEndian.Uint64(b)
	b = b[8:]
	n := binary.BigEndian.Uint32(b)
	b = b[4:]
	for i := uint32(0); i < n; i++ {
		kl := binary.BigEndian.Uint32(b)
		b = b[4:]
		key := string(b[:kl])
		b = b[kl:]
		tombstone := b[0] != 0
		b = b[1:]
		if tombstone {
			rows = append(rows, meta.Row{Key: key, Value: nil})
			continue
		}
		vl := binary.BigEndian.Uint32(b)
		b = b[4:]
		rows = append(rows, meta.Row{Key: key, Value: append([]byte(nil), b[:vl]...)})
		b = b[vl:]
	}
	return index, reset, rows, nil
}
