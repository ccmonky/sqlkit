package sqlkit

import (
	"sync"
)

type SyncMap[K comparable, V any] struct {
	kvs  map[K]V
	lock sync.RWMutex
}

// NewSyncMap creates a new map
func NewSyncMap[K comparable, V any]() *SyncMap[K, V] {
	return &SyncMap[K, V]{
		kvs: make(map[K]V),
	}
}

// Set set a V's instance with key, if exists then override
func (m *SyncMap[K, V]) Delete(key K) {
	m.lock.Lock()
	defer m.lock.Unlock()
	delete(m.kvs, key)
}

// Set set a V's instance with key, if exists then override
func (m *SyncMap[K, V]) Load(key K) (value V, ok bool) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	value, ok = m.kvs[key]
	return
}
func (m *SyncMap[K, V]) LoadAndDelete(key K) (value any, loaded bool) {
	m.lock.Lock()
	defer m.lock.Unlock()
	value, loaded = m.kvs[key]
	delete(m.kvs, key)
	return
}

func (m *SyncMap[K, V]) LoadOrStore(key K, value V) (actual any, loaded bool) {
	m.lock.Lock()
	defer m.lock.Unlock()
	actual, loaded = m.kvs[key]
	if !loaded {
		m.kvs[key] = value
		actual = value
	}
	return
}

func (m *SyncMap[K, V]) Range(f func(key, value any) bool) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	for k, v := range m.kvs {
		c := f(k, v)
		if !c {
			return
		}
	}
}

// Set set a V's instance with key, if exists then override
func (m *SyncMap[K, V]) Store(key K, value V) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.kvs[key] = value
}
