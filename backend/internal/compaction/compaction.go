package compaction

import (
	"container/heap"
	"sort"
	"sync"
	"time"

	"github.com/ashmitsharp/lsm-tree/backend/internal/sstable"
)

type Compactor struct {
	sstableManager *sstable.SSTableManager
	mutex          sync.Mutex
	stopChan       chan struct{}
	minThreshold   int
	gcBefore       int64
}

func NewCompactor(sstableManager *sstable.SSTableManager, minThreshold int, gcBefore int64) *Compactor {
	return &Compactor{
		sstableManager: sstableManager,
		minThreshold:   minThreshold,
		gcBefore:       gcBefore,
		stopChan:       make(chan struct{}),
	}
}

func (c *Compactor) Start() {
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				c.performCompaction()
			case <-c.stopChan:
				return
			}
		}
	}()
}

func (c *Compactor) Stop() {
	close(c.stopChan)
}

func (c *Compactor) groupSSTablesBySize() [][]*sstable.SSTable {
	sstables := c.sstableManager.GetSSTables()
	sort.Slice(sstables, func(i, j int) bool {
		return sstables[i].Size() < sstables[j].Size()
	})

	var groups [][]*sstable.SSTable
	var currentGroup []*sstable.SSTable

	for _, sstable := range sstables {
		if len(currentGroup) == 0 {
			currentGroup = append(currentGroup, sstable)
		} else {
			if sstable.Size() == currentGroup[0].Size() {
				currentGroup = append(currentGroup, sstable)
			} else {
				groups = append(groups, currentGroup)
			}
		}
	}

	if len(currentGroup) > 0 {
		groups = append(groups, currentGroup)
	}

	return groups
}

func (c *Compactor) filterBuckets(groups [][]*sstable.SSTable) [][]*sstable.SSTable {
	var filteredGroups [][]*sstable.SSTable
	for _, group := range groups {
		if len(group) >= c.minThreshold {
			filteredGroups = append(filteredGroups, group)
		}
	}
	return filteredGroups
}

func (c *Compactor) selectHighestReadHotnessScore(groups [][]*sstable.SSTable) []*sstable.SSTable {
	var highestScoreGroup []*sstable.SSTable
	var highestScore int64

	for _, group := range groups {
		var score int64
		for _, sstable := range group {
			score += sstable.ReadHotnessScore()
		}
		if score > highestScore {
			highestScore = score
			highestScoreGroup = group
		}
	}

	return highestScoreGroup
}

func (c *Compactor) checkAvailableDiskSpace(inputSSTables []*sstable.SSTable) []*sstable.SSTable {
	var totalSize int64
	for _, sstable := range inputSSTables {
		totalSize += sstable.Size()
	}

	availableDiskSpace := c.getAvailableDiskSpace()
	if availableDiskSpace < totalSize {
		sort.Slice(inputSSTables, func(i, j int) bool {
			return inputSSTables[i].Size() > inputSSTables[j].Size()
		})

		var filteredSSTables []*sstable.SSTable
		for _, sstable := range inputSSTables {
			if availableDiskSpace >= sstable.Size() {
				filteredSSTables = append(filteredSSTables, sstable)
				availableDiskSpace -= sstable.Size()
			}
		}
		return filteredSSTables
	}
	return inputSSTables
}

func (c *Compactor) getAvailableDiskSpace() int64 {
	// Implement this method to get the available disk space
	// For example, you can use the syscall package to get the disk usage
	return 1024 * 1024 * 1024 // Example: 1 GB available disk space
}

func (c *Compactor) performCompaction() error {
	groups := c.groupSSTablesBySize()
	filteredGroups := c.filterBuckets(groups)
	selectedGroup := c.selectHighestReadHotnessScore(filteredGroups)
	inputSSTables := c.checkAvailableDiskSpace(selectedGroup)

	outputFileName := "output.sst"
	return c.mergeSSTables(inputSSTables, outputFileName)
}

func (c *Compactor) mergeSSTables(inputSSTables []*sstable.SSTable, outputFileName string) error {
	outputSSTable := sstable.NewSSTable(outputFileName)
	scanners := make([]*sstable.Scanner, len(inputSSTables))
	for i, sstable := range inputSSTables {
		scanners[i] = sstable.NewScanner()
	}

	pq := &PriorityQueue{}
	heap.Init(pq)

	for _, scanner := range scanners {
		if scanner.HasNext() {
			heap.Push(pq, scanner)
		}
	}

	for pq.Len() > 0 {
		scanner := heap.Pop(pq).(*sstable.Scanner)
		key, value := scanner.Next()

		outputSSTable.Write(map[string]string{key: value})

		if scanner.HasNext() {
			heap.Push(pq, scanner)
		}
	}
	return nil
}
