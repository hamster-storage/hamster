package sys

import (
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

// Commit writes one transaction's rows atomically and durably.
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

// Reset atomically replaces the database's entire contents with rows: it
// deletes every existing key, then writes rows. A Raft replica calls it on
// snapshot install, where the whole metadata state is supplied at once and
// any prior rows are stale. Metadata is small, so one transaction suffices;
// a state large enough to exceed badger's per-transaction limit would need
// batching, which the source-of-truth recovery work will revisit.
func (m *MetaDB) Reset(rows []meta.Row) error {
	return m.db.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		var keys [][]byte
		for it.Rewind(); it.Valid(); it.Next() {
			keys = append(keys, it.Item().KeyCopy(nil))
		}
		it.Close()
		for _, k := range keys {
			if err := txn.Delete(k); err != nil {
				return err
			}
		}
		for _, r := range rows {
			if r.Value == nil {
				continue
			}
			if err := txn.Set([]byte(r.Key), r.Value); err != nil {
				return err
			}
		}
		return nil
	})
}

// Load visits every persisted row — the startup replay into meta.Store.
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
			if err := fn(string(item.Key()), value); err != nil {
				return err
			}
		}
		return nil
	})
}

// Close flushes and closes the database.
func (m *MetaDB) Close() error { return m.db.Close() }
