package sys

import (
	"encoding/binary"
	"fmt"

	badger "github.com/dgraph-io/badger/v4"

	"github.com/hamster-storage/hamster/internal/meta"
)

// MetaDB is the durable metadata row store: BadgerDB (ADR-0005) behind
// meta's Persister interface. Thin by rule — record encoding lives in
// internal/meta; this adapter moves opaque bytes.
type MetaDB struct {
	db *badger.DB
}

// appliedIndexKey holds the Raft applied index alongside the metadata rows,
// written in the same transaction as a clustered commit (CommitAt/ResetAt) so
// the index and the rows can never disagree across a crash. The leading NUL
// keeps it outside the metadata keyspace — every meta key begins with a letter
// prefix (internal/meta/keys.go) — so LoadState tells it apart from a row and
// never hands it to the store. Single-node serve does not write it.
const appliedIndexKey = "\x00raft.applied-index"

// OpenMetaDB opens (creating if absent) the metadata database in dir.
func OpenMetaDB(dir string) (*MetaDB, error) {
	opts := badger.DefaultOptions(dir).
		WithLogger(nil).     // badger's own chatter does not belong on serve's output
		WithSyncWrites(true) // a committed transaction is durable, full stop
	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("open metadata db at %s: %w", dir, err)
	}
	return &MetaDB{db: db}, nil
}

// CommitAt writes one apply's rows together with the Raft applied index in a
// single atomic, durable transaction (ADR-0005): the clustered metadata plane
// makes the index and the state it reflects durable together, so a crash
// cannot leave them disagreeing. The bridge in raftnode calls it through
// meta's Persister with the index of the entry being applied.
func (m *MetaDB) CommitAt(appliedIndex uint64, rows []meta.Row) error {
	return m.db.Update(func(txn *badger.Txn) error {
		for _, r := range rows {
			if r.Value == nil {
				if err := txn.Delete([]byte(r.Key)); err != nil {
					return err
				}
				continue
			}
			if err := txn.Set([]byte(r.Key), r.Value); err != nil {
				return err
			}
		}
		var idx [8]byte
		binary.BigEndian.PutUint64(idx[:], appliedIndex)
		return txn.Set([]byte(appliedIndexKey), idx[:])
	})
}

// ResetAt replaces the database's entire contents with rows at appliedIndex —
// the wholesale replacement a snapshot install (or a WAL rebuild after the
// durable store was lost or corrupt) performs. It uses DropAll rather than an
// iterate-and-delete, so it is robust to corrupt blocks: the recovery path can
// always re-materialise the store from the authoritative Raft log.
func (m *MetaDB) ResetAt(appliedIndex uint64, rows []meta.Row) error {
	if err := m.db.DropAll(); err != nil {
		return fmt.Errorf("reset metadata db: %w", err)
	}
	return m.CommitAt(appliedIndex, rows)
}

// LoadState reads the persisted rows and the Raft applied index. ok is false
// on a fresh store (nothing has been committed); a non-nil error means the
// store is unreadable — the caller rebuilds from the Raft log. The
// applied-index row is consumed here, never returned as a metadata row.
func (m *MetaDB) LoadState() (rows []meta.Row, appliedIndex uint64, ok bool, err error) {
	err = m.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			value, verr := item.ValueCopy(nil)
			if verr != nil {
				return verr
			}
			if key := string(item.Key()); key == appliedIndexKey {
				if len(value) != 8 {
					return fmt.Errorf("malformed applied-index row (%d bytes)", len(value))
				}
				appliedIndex = binary.BigEndian.Uint64(value)
				ok = true
			} else {
				rows = append(rows, meta.Row{Key: key, Value: value})
			}
		}
		return nil
	})
	if err != nil {
		return nil, 0, false, err
	}
	return rows, appliedIndex, ok, nil
}

// Commit writes one transaction's rows atomically and durably, without an
// applied index — single-node serve, which recovers by loading every row.
func (m *MetaDB) Commit(rows []meta.Row) error {
	return m.db.Update(func(txn *badger.Txn) error {
		for _, r := range rows {
			if r.Value == nil {
				if err := txn.Delete([]byte(r.Key)); err != nil {
					return err
				}
				continue
			}
			if err := txn.Set([]byte(r.Key), r.Value); err != nil {
				return err
			}
		}
		return nil
	})
}

// Load visits every persisted row — the startup replay into meta.Store for
// single-node serve.
func (m *MetaDB) Load(fn func(key string, value []byte) error) error {
	return m.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			value, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			if string(item.Key()) == appliedIndexKey {
				continue
			}
			if err := fn(string(item.Key()), value); err != nil {
				return err
			}
		}
		return nil
	})
}

// Close flushes and closes the database.
func (m *MetaDB) Close() error { return m.db.Close() }
