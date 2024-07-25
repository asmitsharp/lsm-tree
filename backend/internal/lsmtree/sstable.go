package lsmtree

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"sync"
)

type SSTable struct {
	path        string
	dataFile    *os.File
	indexFile   *os.File
	bloomFilter *BloomFilter
	summaryFile *os.File
	statsFile   *os.File
}

func hash1(key string) uint {
	h := fnv.New32a()
	h.Write([]byte(key))
	return uint(h.Sum32())
}

func hash2(key string) uint {
	h := fnv.New32()
	h.Write([]byte(key))
	return uint(h.Sum32())
}

func NewSSTable(path string) (*SSTable, error) {
	var wg sync.WaitGroup
	var dataFile, indexFile, summaryFile, statsFile *os.File
	var err error
	var mu sync.Mutex
	// Open or create all necessary files
	createFile := func(filePath string, file **os.File) {
		defer wg.Done()
		f, e := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0644)
		mu.Lock()
		defer mu.Unlock()
		if e != nil && err == nil {
			err = e
		}

		*file = f
	}

	wg.Add(4)
	go createFile(path+"/data.sstable", &dataFile)
	go createFile(path+"/index.sstable", &indexFile)
	go createFile(path+"/summary.sstable", &summaryFile)
	go createFile(path+"/stats.sstable", &statsFile)

	wg.Wait()
	if err == nil {
		return nil, err
	}
	// Initialize BloomFilter
	bloomFilter := NewBloomFilter(1000, hash1, hash2)

	return &SSTable{
		path:        path,
		dataFile:    dataFile,
		indexFile:   indexFile,
		bloomFilter: bloomFilter,
		summaryFile: summaryFile,
		statsFile:   statsFile,
	}, nil
}

func (sst *SSTable) Write(key string, value []byte) error {
	// Write to data file
	offset, err := sst.dataFile.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	keySize := len(key)
	valueSize := len(value)

	err = binary.Write(sst.dataFile, binary.BigEndian, int32(keySize))
	if err != nil {
		return err
	}
	_, err = sst.dataFile.Write([]byte(key))
	if err != nil {
		return err
	}
	err = binary.Write(sst.dataFile, binary.BigEndian, int32(valueSize))
	if err != nil {
		return err
	}
	_, err = sst.dataFile.Write(value)
	if err != nil {
		return err
	}
	// Update index file
	indexWriter := bufio.NewWriter(sst.indexFile)
	err = binary.Write(indexWriter, binary.BigEndian, int32(keySize))
	if err != nil {
		return err
	}

	_, err = indexWriter.WriteString(key)
	if err != nil {
		return err
	}
	err = binary.Write(indexWriter, binary.BigEndian, offset)
	if err != nil {
		return err
	}

	indexWriter.Flush()
	// Update bloom filter
	sst.bloomFilter.Add(key)
	// Update summary (periodically)
	// Update statistics
	return nil
}

func (sst *SSTable) Read(key string) ([]byte, error) {
	if !sst.bloomFilter.MightContain(key) {
		return nil, fmt.Errorf("key not found")
	}

	indexFile, err := os.Open(sst.path + "/index.sstable")
	if err != nil {
		return nil, err
	}
	defer indexFile.Close()

	// Perform binary search on index file
	fileInfo, err := indexFile.Stat()
	if err != nil {
		return nil, err
	}
	size := fileInfo.Size()
	var keySize int32
	var offset int64
	left := int64(0)
	right := size
	for left < right {
		mid := (left + right) / 2

		indexFile.Seek(mid, 0)
		scanner := bufio.NewScanner(indexFile)
		scanner.Scan() // Skip partial record
		scanner.Scan() // Read full record
		record := scanner.Bytes()

		keySize = int32(binary.BigEndian.Uint32(record[:4]))
		recordKey := string(record[4 : 4+keySize])
		offset = int64(binary.BigEndian.Uint64(record[4+keySize:]))

		if recordKey == key {
			dataFile, err := os.Open(sst.path + "/data.sstable")
			if err != nil {
				return nil, err
			}
			defer dataFile.Close()

			dataFile.Seek(offset, 0)
			var valueSize int32
			binary.Read(dataFile, binary.BigEndian, &keySize)
			keyBuf := make([]byte, keySize)
			dataFile.Read(keyBuf)
			binary.Read(dataFile, binary.BigEndian, &valueSize)
			valueBuf := make([]byte, valueSize)
			dataFile.Read(valueBuf)
			return valueBuf, nil
		} else if recordKey < key {
			left = mid + 1
		} else {
			right = mid
		}
	}

	return nil, fmt.Errorf("key not found")
}

func (sst *SSTable) Close() error {
	err := sst.dataFile.Close()
	if err != nil {
		return err
	}

	err = sst.indexFile.Close()
	if err != nil {
		return err
	}

	err = sst.summaryFile.Close()
	if err != nil {
		return err
	}

	err = sst.statsFile.Close()
	if err != nil {
		return err
	}

	// Persist bloom filter not implemented
	return nil
}
