package sstable

import (
	"fmt"
	"sync"
)

type SSTableManager struct {
	tables []*SSTable
	mutex  sync.RWMutex
}

func NewSSTableManager() *SSTableManager {
	return &SSTableManager{
		tables: make([]*SSTable, 0),
	}
}

func (m *SSTableManager) CreateSSTable(data map[string]string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	filename := fmt.Sprintf("sstable_%d.db", len(m.tables))
	sst := NewSSTable(filename)
	err := sst.Write(data)
	if err != nil {
		return err
	}

	m.tables = append(m.tables, sst)
	return nil
}

func (m *SSTableManager) GetSSTables() []*SSTable {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	tablesCopy := make([]*SSTable, len(m.tables))
	copy(tablesCopy, m.tables)
	return tablesCopy
}

func (m *SSTableManager) Read(key string) (string, bool) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	for i := len(m.tables) - 1; i >= 0; i-- {
		if value, found := m.tables[i].Read(key); found {
			return value, true
		}
	}

	return "", false
}
