package wal

import (
	"fmt"

	"google.golang.org/protobuf/encoding/protowire"

	"github.com/hamster-storage/hamster/internal/meta"
	"github.com/hamster-storage/hamster/internal/seam"
)

// RowLog is a meta.Persister over a Log: one metadata transaction, one
// record. It is how a store persists through a seam.Disk — under the
// simulation harness, where BadgerDB cannot go, and anywhere else a
// crash-faithful Disk is the storage.
//
// The record payload (additively versioned protobuf, field numbers fixed):
//
//	message RowBatch {
//	  uint32 format_version = 1;
//	  repeated Row rows     = 2;
//	}
//	message Row {
//	  string key       = 1;
//	  bytes  value     = 2;  // an encoded metadata record; never empty
//	  bool   tombstone = 3;  // the transaction deleted this key
//	}
type RowLog struct {
	log *Log
}

const rowBatchFormatVersion = 1

// OpenRows opens the metadata row log at name and replays it, returning
// the surviving rows — the final state after every committed transaction,
// ready to feed meta.Store.Restore.
func OpenRows(disk seam.Disk, name string) (*RowLog, map[string][]byte, error) {
	l, records, err := Open(disk, name)
	if err != nil {
		return nil, nil, err
	}
	rows := make(map[string][]byte)
	for i, rec := range records {
		if err := applyBatch(rows, rec); err != nil {
			return nil, nil, fmt.Errorf("wal: replaying row batch %d of %s: %w", i, name, err)
		}
	}
	return &RowLog{log: l}, rows, nil
}

// Commit implements meta.Persister: the batch is durable when it returns.
func (l *RowLog) Commit(batch []meta.Row) error {
	return l.log.Append(encodeBatch(batch))
}

func encodeBatch(batch []meta.Row) []byte {
	b := protowire.AppendTag(nil, 1, protowire.VarintType)
	b = protowire.AppendVarint(b, rowBatchFormatVersion)
	for _, r := range batch {
		row := protowire.AppendTag(nil, 1, protowire.BytesType)
		row = protowire.AppendString(row, r.Key)
		if r.Value == nil {
			row = protowire.AppendTag(row, 3, protowire.VarintType)
			row = protowire.AppendVarint(row, 1)
		} else {
			row = protowire.AppendTag(row, 2, protowire.BytesType)
			row = protowire.AppendBytes(row, r.Value)
		}
		b = protowire.AppendTag(b, 2, protowire.BytesType)
		b = protowire.AppendBytes(b, row)
	}
	return b
}

// applyBatch replays one committed transaction onto rows. Unknown fields
// are skipped (a newer writer may know more); malformed protobuf inside an
// intact frame is corruption, not a torn tail, and is an error.
func applyBatch(rows map[string][]byte, rec []byte) error {
	for len(rec) > 0 {
		num, typ, n := protowire.ConsumeTag(rec)
		if n < 0 {
			return protowire.ParseError(n)
		}
		rec = rec[n:]
		if num == 2 && typ == protowire.BytesType {
			row, n := protowire.ConsumeBytes(rec)
			if n < 0 {
				return protowire.ParseError(n)
			}
			rec = rec[n:]
			if err := applyRow(rows, row); err != nil {
				return err
			}
			continue
		}
		n = protowire.ConsumeFieldValue(num, typ, rec)
		if n < 0 {
			return protowire.ParseError(n)
		}
		rec = rec[n:]
	}
	return nil
}

func applyRow(rows map[string][]byte, row []byte) error {
	var key string
	var value []byte
	var tombstone bool
	for len(row) > 0 {
		num, typ, n := protowire.ConsumeTag(row)
		if n < 0 {
			return protowire.ParseError(n)
		}
		row = row[n:]
		switch {
		case num == 1 && typ == protowire.BytesType:
			v, n := protowire.ConsumeBytes(row)
			if n < 0 {
				return protowire.ParseError(n)
			}
			key, row = string(v), row[n:]
		case num == 2 && typ == protowire.BytesType:
			v, n := protowire.ConsumeBytes(row)
			if n < 0 {
				return protowire.ParseError(n)
			}
			value, row = append([]byte(nil), v...), row[n:]
		case num == 3 && typ == protowire.VarintType:
			v, n := protowire.ConsumeVarint(row)
			if n < 0 {
				return protowire.ParseError(n)
			}
			tombstone, row = v != 0, row[n:]
		default:
			n := protowire.ConsumeFieldValue(num, typ, row)
			if n < 0 {
				return protowire.ParseError(n)
			}
			row = row[n:]
		}
	}
	if key == "" {
		return fmt.Errorf("row without a key")
	}
	if tombstone {
		delete(rows, key)
	} else {
		rows[key] = value
	}
	return nil
}
