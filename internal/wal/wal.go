// Package wal implements an append-only record log on a seam.Disk — the
// durable-commit primitive for everything that must survive a crash through
// the simulation harness's crash-faithful disk: the metadata row log here
// in v0.1 (rows.go), the Raft entry log when clustering arrives in v0.2.
//
// Each record is framed as uvarint payload length, CRC-32C of the payload
// (4 bytes, little-endian), then the payload. Append stages the frame and
// syncs it: durable when Append returns. The framing leans on the Disk
// crash contract — content durable before an append survives, the appended
// tail may be lost or torn — so a crash can only damage the end of the
// file. Open detects that torn tail by framing and checksum, truncates it
// away durably, and never surfaces it as a record.
//
// The framing is a container, not a format: record payloads are additively
// versioned protobuf (CLAUDE.md invariant 2), defined by each log's owner.
package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io/fs"
	"math"

	"github.com/hamster-storage/hamster/internal/seam"
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Log is an append-only record log on one file of a Disk.
type Log struct {
	disk seam.Disk
	name string
}

// Open reads the log at name and returns it ready for appends, along with
// every intact record in append order. A missing file is an empty log. A
// torn tail is cut off durably before Open returns, so the next Append
// lands on a frame boundary.
func Open(disk seam.Disk, name string) (*Log, [][]byte, error) {
	l := &Log{disk: disk, name: name}
	data, err := disk.ReadFile(name)
	if errors.Is(err, fs.ErrNotExist) {
		return l, nil, nil
	}
	if err != nil {
		return nil, nil, fmt.Errorf("wal: opening %s: %w", name, err)
	}

	var records [][]byte
	good := 0
	for good < len(data) {
		payload, n, ok := parseFrame(data[good:])
		if !ok {
			break
		}
		records = append(records, payload)
		good += n
	}
	if good != len(data) {
		// The torn tail of the crash that ended the last process. Rewrite
		// the intact prefix; if this write tears in its own crash, the
		// result is a shorter intact prefix, handled the same way next
		// Open.
		if err := disk.WriteFile(name, data[:good]); err != nil {
			return nil, nil, fmt.Errorf("wal: truncating torn tail of %s: %w", name, err)
		}
		if err := disk.Sync(name); err != nil {
			return nil, nil, fmt.Errorf("wal: syncing truncated %s: %w", name, err)
		}
	}
	return l, records, nil
}

// Append writes one record: durable when it returns.
func (l *Log) Append(record []byte) error {
	frame := make([]byte, 0, binary.MaxVarintLen64+crc32.Size+len(record))
	frame = binary.AppendUvarint(frame, uint64(len(record)))
	frame = binary.LittleEndian.AppendUint32(frame, crc32.Checksum(record, castagnoli))
	frame = append(frame, record...)
	if err := l.disk.Append(l.name, frame); err != nil {
		return fmt.Errorf("wal: appending to %s: %w", l.name, err)
	}
	if err := l.disk.Sync(l.name); err != nil {
		return fmt.Errorf("wal: syncing %s: %w", l.name, err)
	}
	return nil
}

// parseFrame reads one frame from the head of b. Any shortfall or checksum
// mismatch reports not-ok: under the Disk crash contract that can only be
// the torn tail.
func parseFrame(b []byte) (payload []byte, n int, ok bool) {
	length, ln := binary.Uvarint(b)
	if ln <= 0 || length > math.MaxInt32 {
		return nil, 0, false
	}
	end := ln + crc32.Size + int(length)
	if end > len(b) {
		return nil, 0, false
	}
	crc := binary.LittleEndian.Uint32(b[ln:])
	payload = b[ln+crc32.Size : end]
	if crc32.Checksum(payload, castagnoli) != crc {
		return nil, 0, false
	}
	return payload, end, true
}
