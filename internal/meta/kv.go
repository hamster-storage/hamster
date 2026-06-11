package meta

import "slices"

// memKV is a sorted in-memory key-value map: the stand-in for BadgerDB
// with the one property apply depends on — ordered iteration over encoded
// keys. Values are typed records; their protobuf encoding arrives with
// persistence. No locking: a Store is owned by one event loop, and apply's
// multi-row mutations are atomic by construction because nothing can
// observe state mid-apply.
type memKV struct {
	keys []string // sorted
	vals map[string]any
}

func newMemKV() *memKV {
	return &memKV{vals: make(map[string]any)}
}

func (kv *memKV) get(k string) (any, bool) {
	v, ok := kv.vals[k]
	return v, ok
}

func (kv *memKV) set(k string, v any) {
	if _, ok := kv.vals[k]; !ok {
		i, _ := slices.BinarySearch(kv.keys, k)
		kv.keys = slices.Insert(kv.keys, i, k)
	}
	kv.vals[k] = v
}

func (kv *memKV) delete(k string) {
	if _, ok := kv.vals[k]; !ok {
		return
	}
	delete(kv.vals, k)
	i, _ := slices.BinarySearch(kv.keys, k)
	kv.keys = slices.Delete(kv.keys, i, i+1)
}

// scan visits rows with key >= from in ascending key order until fn
// returns false. Callers must not mutate the KV during a scan; apply
// collects first and mutates after.
func (kv *memKV) scan(from string, fn func(k string, v any) bool) {
	i, _ := slices.BinarySearch(kv.keys, from)
	for ; i < len(kv.keys); i++ {
		if !fn(kv.keys[i], kv.vals[kv.keys[i]]) {
			return
		}
	}
}
