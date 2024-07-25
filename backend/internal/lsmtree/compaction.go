package lsmtree

import (
	"bufio"
	"encoding/binary"
	"os"
)

func CompactSSTables(sstables []*SSTable) (*SSTable, error) {
	// Create a new SSTable
	newSSTable, err := NewSSTable("./sstable_compacted")
	if err != nil {
		return nil, err
	}

	entries, err := mergeEntries(sstables)
	if err != nil {
		return nil, err
	}

	// Write merged entries to the new SSTable
	for key, value := range entries {
		err := newSSTable.Write(key, value)
		if err != nil {
			return nil, err
		}
	}

	return newSSTable, nil
}

func mergeEntries(sstables []*SSTable) (map[string][]byte, error) {
	merged := make(map[string][]byte)

	for _, sstable := range sstables {
		indexFile, err := os.Open(sstable.path + "/index.sstable")
		if err != nil {
			return nil, err
		}
		defer indexFile.Close()

		scanner := bufio.NewScanner(indexFile)
		for scanner.Scan() {
			record := scanner.Bytes()
			keySize := int32(binary.BigEndian.Uint32(record[:4]))
			key := string(record[4 : 4+keySize])
			offset := int64(binary.BigEndian.Uint64(record[4+keySize:]))

			dataFile, err := os.Open(sstable.path + "/data.sstable")
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

			// Always keep the latest value for overlapping keys
			merged[key] = valueBuf
		}

		if err := scanner.Err(); err != nil {
			return nil, err
		}
	}

	return merged, nil
}
