package memtable

import (
	"sync"

	"github.com/ashmitsharp/lsm-tree/backend/internal/tree"
)

type Memtable struct {
	tree      tree.Tree
	size      int64
	maxSize   int64
	flushChan chan<- *Memtable
	mutex     sync.RWMutex
}

func NewMemTable(maxSize int64, flushChan chan<- *Memtable) *Memtable {
	return &Memtable{
		tree:      tree.NewAVLTree(),
		maxSize:   maxSize,
		flushChan: flushChan,
	}
}

func (m *Memtable) Put(key tree.Comparable, value interface{}) bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	stringKey, ok := key.(tree.StringComparable)
	if !ok {
		panic("Key must be of type StringComparable")
	}

	if m.tree.Insert(key, value) {
		m.size += int64(stringKey.Length() + len(value.(string)))
		if m.size >= m.maxSize {
			m.flushChan <- m
		}
		return true
	}

	return false
}

func (m *Memtable) Get(key tree.Comparable) (interface{}, bool) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.tree.Search(key)
}

func (m *Memtable) Delete(key tree.Comparable) bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.tree.Delete(key)
}

func (m *Memtable) InOrderTraversal(visit func(key tree.Comparable, value interface{})) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.tree.InOrderTraversal(visit)
}
