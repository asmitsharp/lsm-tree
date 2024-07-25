package lsm

import (
	"sync"

	"github.com/ashmitsharp/lsm-tree/backend/internal/compaction"
	"github.com/ashmitsharp/lsm-tree/backend/internal/memtable"
	"github.com/ashmitsharp/lsm-tree/backend/internal/sstable"
	"github.com/ashmitsharp/lsm-tree/backend/internal/tree"
	"github.com/ashmitsharp/lsm-tree/backend/internal/wal"
)

type LSMTree struct {
	memtable       *memtable.Memtable
	sstableManager *sstable.SSTableManager
	wal            *wal.WAL
	compactor      *compaction.Compactor
	flushChan      chan *memtable.Memtable
	mutex          sync.RWMutex
}

func NewLSMTree() (*LSMTree, error) {
	flushChan := make(chan *memtable.Memtable, 1)
	sstableManager := sstable.NewSSTableManager()
	walLog, err := wal.NewWAL("lsm.log")
	if err != nil {
		return nil, err

	}

	lsm := &LSMTree{
		memtable:       memtable.NewMemTable(1024*1024, flushChan),
		sstableManager: sstableManager,
		wal:            walLog,
		compactor:      compaction.NewCompactor(sstableManager, 4, 1000000),
		flushChan:      flushChan,
	}

	go lsm.Run()
	lsm.compactor.Start()

	return lsm, nil
}

func (lsm *LSMTree) Put(key, value string) error {
	lsm.mutex.Lock()
	defer lsm.mutex.Unlock()

	if err := lsm.wal.AppendPut(key, value); err != nil {
		return err
	}

	lsm.memtable.Put(tree.StringComparable{Value: key}, value)
	return nil
}

func (lsm *LSMTree) Get(key string) (string, bool) {
	lsm.mutex.Lock()
	defer lsm.mutex.Unlock()

	if value, found := lsm.memtable.Get(tree.StringComparable{Value: key}); found {
		return value.(string), true
	}

	return lsm.sstableManager.Read(key)
}

func (lsm *LSMTree) Delete(key string) error {
	lsm.mutex.Lock()
	defer lsm.mutex.Unlock()

	if err := lsm.wal.AppendDelete(key); err != nil {
		return err
	}

	lsm.memtable.Delete(tree.StringComparable{Value: key})
	return nil
}

func (lsm *LSMTree) FlushMemtable() error {
	lsm.mutex.Lock()
	defer lsm.mutex.Unlock()

	data := make(map[string]string)
	lsm.memtable.InOrderTraversal(func(key tree.Comparable, value interface{}) {
		strKey := key.(tree.StringComparable)
		data[string(strKey.Value)] = value.(string)
	})

	err := lsm.sstableManager.CreateSSTable(data)
	if err != nil {
		return err
	}

	lsm.memtable = memtable.NewMemTable(1024*1024*1024, lsm.flushChan)
	return nil
}

func (lsm *LSMTree) Run() {
	for {
		select {
		case <-lsm.flushChan:
			lsm.FlushMemtable()
		}
	}
}

func (lsm *LSMTree) Close() error {
	lsm.mutex.Lock()
	defer lsm.mutex.Unlock()

	lsm.compactor.Stop()
	return lsm.wal.Close()
}

func (lsm *LSMTree) Recover() error {
	return lsm.wal.Replay(func(opType uint8, key, value string) error {
		switch opType {
		case 1:
			lsm.memtable.Put(tree.StringComparable{Value: key}, value)
		case 2:
			lsm.memtable.Delete(tree.StringComparable{Value: key})
		}
		return nil
	})
}
