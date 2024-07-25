package lsmtree

import (
	"fmt"
	"sync"
	"time"
)

type KeyValue interface {
	Get(key string) (string, bool)
	Put(key, value string)
	Delete(key string)
}

type TreeNode struct {
	key    string
	value  string
	Left   *TreeNode
	Right  *TreeNode
	height int
}

// In-Memory Table configurations :

type MemTableConfig struct {
	HeapMemoryInMB       int64         // Limit for heap memory usage
	OffHeapMemoryInMB    int64         // Limit for off heap memory usage
	FlushWriteInMB       int64         // Size threshold to trigger flush
	FlushPeriodInMinutes time.Duration // Time threshold to trigger flush
}

type MemTable struct {
	sync.RWMutex
	root          *TreeNode
	size          int64
	config        MemTableConfig
	lastFlushTime time.Time
	isOffHeap     bool
	offHeapMemory []byte
}

func NewMemTable(config MemTableConfig, isOffHeap bool) *MemTable {
	mt := &MemTable{
		config:        config,
		lastFlushTime: time.Now(),
		isOffHeap:     isOffHeap,
	}

	if isOffHeap {
		mt.offHeapMemory = make([]byte, config.OffHeapMemoryInMB*1024*1024)
	}

	return mt
}

func (mt *MemTable) shouldFlush(additionalSize int64) bool {
	// Size based Threshold
	if mt.size+additionalSize > mt.config.FlushWriteInMB*1024*1024 {
		return true
	}
	// Time based Threshold
	if time.Since(mt.lastFlushTime) > mt.config.FlushPeriodInMinutes*time.Minute {
		return true
	}
	// is heap or off heap
	if mt.isOffHeap {
		return mt.size+additionalSize > mt.config.OffHeapMemoryInMB*1024*1024
	}
	return mt.size+additionalSize > mt.config.HeapMemoryInMB*1024*1024
}

// func (mt *MemTable) FlushToSSTable(path string) error {
//     sst, err := NewSSTable(path)
//     if err != nil {
//         return err
//     }
//     defer sst.Close()

//     return mt.inOrderTraversal(mt.root, func(key, value string) error {
//         return sst.Write(key, []byte(value))
//     })
// }

func (mt *MemTable) flush() error {
	// Reseting size and last flush time
	mt.size = 0
	mt.lastFlushTime = time.Now()

	return nil
}

func height(node *TreeNode) int {
	if node == nil {
		return 0
	}
	return node.height
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func getBalanceFactor(node *TreeNode) int {
	if node == nil {
		return 0
	}
	return height(node.Left) - height(node.Right)
}

func updateHeight(node *TreeNode) {
	node.height = 1 + max(height(node.Left), height(node.Right))
}

func rotateRight(y *TreeNode) *TreeNode {
	x := y.Left
	t2 := x.Right

	x.Left = y
	y.Right = t2

	updateHeight(y)
	updateHeight(x)

	return x
}

func rotateLeft(x *TreeNode) *TreeNode {
	y := x.Right
	t2 := y.Left

	y.Left = x
	x.Right = t2

	updateHeight(y)
	updateHeight(x)

	return y
}

func (mt *MemTable) insert(node *TreeNode, key, value string) (*TreeNode, int64) {
	if node == nil {
		return &TreeNode{key: key, value: value, height: 1}, int64(len(key) + len(value))
	}

	var sizeChange int64
	if key < node.key {
		node.Left, sizeChange = mt.insert(node.Left, key, value)
	} else if key > node.key {
		node.Right, sizeChange = mt.insert(node.Right, key, value)
	} else {
		oldSize := int64(len(node.value))
		node.value = value
		return node, int64(len(value)) - oldSize
	}

	updateHeight(node)

	balance := getBalanceFactor(node)

	if balance > 1 && key < node.Left.key {
		return rotateRight(node), sizeChange
	}

	if balance < -1 && key > node.Right.key {
		return rotateLeft(node), sizeChange
	}

	if balance > 1 && key > node.Left.key {
		node.Left = rotateLeft(node.Left)
		return rotateRight(node), sizeChange
	}

	if balance < -1 && key < node.Right.key {
		node.Right = rotateRight(node.Right)
		return rotateLeft(node), sizeChange
	}

	return node, sizeChange

}

func (mt *MemTable) get(node *TreeNode, key string) (string, bool) {
	if node == nil {
		return "", false
	}

	if key < node.key {
		return mt.get(node.Left, key)
	} else if key > node.key {
		return mt.get(node.Right, key)
	}

	return node.value, true
}

func (mt *MemTable) Get(key string) (string, bool) {
	mt.Lock()
	defer mt.Unlock()

	return mt.get(mt.root, key)
}

func (mt *MemTable) Put(key, value string) error {
	mt.Lock()
	defer mt.Unlock()

	entrySize := int64(len(key) + len(value))
	if mt.shouldFlush(entrySize) {
		if err := mt.flush(); err != nil {
			return err
		}
	}

	var sizeChange int64
	mt.root, sizeChange = mt.insert(mt.root, key, value)
	mt.size += sizeChange

	return nil
}

func (mt *MemTable) minValueNode(node *TreeNode) *TreeNode {
	current := node
	for current.Left != nil {
		current = current.Left
	}
	return current
}

func (mt *MemTable) deleteNode(node *TreeNode, key string) (*TreeNode, int64, bool) {
	if node == nil {
		return nil, 0, false
	}

	var sizeChange int64
	var deleted bool
	if key < node.key {
		node.Left, sizeChange, deleted = mt.deleteNode(node.Left, key)
	} else if key > node.key {
		node.Right, sizeChange, deleted = mt.deleteNode(node.Right, key)
	} else {
		deleted = true
		sizeChange = -int64(len(node.key) + len(node.value))

		if node.Left == nil {
			return node.Right, sizeChange, deleted
		} else if node.Right == nil {
			return node.Left, sizeChange, deleted
		}

		temp := mt.minValueNode(node.Right)
		node.key = temp.key
		node.value = temp.value
		node.Right, _, _ = mt.deleteNode(node.Right, temp.key)
	}

	if !deleted {
		return node, 0, false
	}

	updateHeight(node)

	balance := getBalanceFactor(node)

	// Left Left Case
	if balance > 1 && getBalanceFactor(node.Left) >= 0 {
		return rotateRight(node), sizeChange, true
	}

	// Left Right Case
	if balance > 1 && getBalanceFactor(node.Left) < 0 {
		node.Left = rotateLeft(node.Left)
		return rotateRight(node), sizeChange, true
	}

	// Right Right Case
	if balance < -1 && getBalanceFactor(node.Right) <= 0 {
		return rotateLeft(node), sizeChange, true
	}

	// Right Left Case
	if balance < -1 && getBalanceFactor(node.Right) > 0 {
		node.Right = rotateRight(node.Right)
		return rotateLeft(node), sizeChange, true
	}

	return node, sizeChange, true
}

func (mt *MemTable) Delete(key string) bool {
	mt.Lock()
	defer mt.Unlock()

	var sizeChange int64
	var deleted bool
	mt.root, sizeChange, deleted = mt.deleteNode(mt.root, key)
	if deleted {
		mt.size += sizeChange
	}
	return deleted
}

func main() {
	config := MemTableConfig{
		HeapMemoryInMB:       256,
		OffHeapMemoryInMB:    1024,
		FlushWriteInMB:       64,
		FlushPeriodInMinutes: 30,
	}

	memtable := NewMemTable(config, false) // Using heap memory

	// Example usage
	memtable.Put("key1", "value1")
	memtable.Put("key2", "value2")
	memtable.Put("key3", "value3")

	if value, exists := memtable.Get("key1"); exists {
		fmt.Printf("Value for key2: %s\n", value)
	}

	memtable.Delete("key2")

	if _, exists := memtable.Get("key2"); !exists {
		fmt.Println("key2 was successfully deleted")
	}
}
