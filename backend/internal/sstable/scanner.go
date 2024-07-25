package sstable

import (
	"encoding/binary"
	"os"
)

type Scanner struct {
	sstable *SSTable
	file    *os.File
	offset  int64
}

func (sst *SSTable) NewScanner() *Scanner {
	file, err := os.Open(sst.filename)
	if err != nil {
		panic(err)
	}
	return &Scanner{
		sstable: sst,
		file:    file,
		offset:  0,
	}
}

func (scanner *Scanner) HasNext() bool {
	// Implement this method to check if there are more partitions to scan
	return scanner.offset < scanner.sstable.Size()
}

func (scanner *Scanner) Next() (string, string) {
	// Implement this method to read the next partition
	var keySize, valueSize int64
	binary.Read(scanner.file, binary.LittleEndian, &keySize)
	binary.Read(scanner.file, binary.LittleEndian, &valueSize)

	keyBytes := make([]byte, keySize)
	valueBytes := make([]byte, valueSize)

	scanner.file.Read(keyBytes)
	scanner.file.Read(valueBytes)

	scanner.offset += 8 + 8 + keySize + valueSize

	return string(keyBytes), string(valueBytes)
}

func (scanner *Scanner) PeekKey() string {
	// Implement this method to peek the next key without advancing the scanner
	currentOffset := scanner.offset
	var keySize int64
	binary.Read(scanner.file, binary.LittleEndian, &keySize)

	keyBytes := make([]byte, keySize)
	scanner.file.Read(keyBytes)

	scanner.offset = currentOffset
	scanner.file.Seek(currentOffset, 0)

	return string(keyBytes)
}
