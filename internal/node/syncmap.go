package node

import "sync"

// syncMap is a sync.Map with types, for the table's entries, which are added
// once and read many times. It adds with LoadOrStore and removes with
// CompareAndDelete, so a removal can only take out the value it means to.
type syncMap[K comparable, V comparable] struct{ m sync.Map }

func (s *syncMap[K, V]) Load(k K) (v V, ok bool) {
	x, ok := s.m.Load(k)
	if ok {
		v = x.(V)
	}
	return v, ok
}

// LoadOrStore returns the value already there for k (loaded true), or stores
// v and returns it.
func (s *syncMap[K, V]) LoadOrStore(k K, v V) (actual V, loaded bool) {
	x, loaded := s.m.LoadOrStore(k, v)
	return x.(V), loaded
}

// CompareAndDelete deletes k if its value is v.
func (s *syncMap[K, V]) CompareAndDelete(k K, v V) bool { return s.m.CompareAndDelete(k, v) }

// Range calls fn for each key and value until fn returns false.
func (s *syncMap[K, V]) Range(fn func(K, V) bool) {
	s.m.Range(func(k, v any) bool { return fn(k.(K), v.(V)) })
}
