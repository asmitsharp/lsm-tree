package sstable

import (
	"encoding/binary"
	"os"
	"time"
)

type SSTable struct {
	filename      string
	index         map[string]int64
	size          int64
	readCounts    map[string]int64
	lastReadTimes map[string]time.Time
}

func NewSSTable(filename string) *SSTable {
	return &SSTable{
		filename:      filename,
		index:         make(map[string]int64),
		readCounts:    make(map[string]int64),
		lastReadTimes: make(map[string]time.Time),
	}
}

func (sst *SSTable) Size() int64 {
	return sst.size
}

func (sst *SSTable) ReadHotnessScore() int64 {
	var totalScore int64
	for key, count := range sst.readCounts {
		timeSinceLastRead := time.Since(sst.lastReadTimes[key]).Seconds()
		dedcayFactor := 0.9
		initalHotnessScore := float64(100)
		decayedScore := initalHotnessScore * dedcayFactor / (1 + timeSinceLastRead)
		hotnessScore := int64(decayedScore) + count
		totalScore += hotnessScore
	}
	return totalScore
}

func (sst *SSTable) Write(data map[string]string) error {
	file, err := os.Create(sst.filename)
	if err != nil {
		return err
	}
	defer file.Close()

	var offset int64 = 0
	for key, value := range data {
		keySize := int64(len(key))
		valueSize := int64(len(value))

		binary.Write(file, binary.LittleEndian, keySize)
		binary.Write(file, binary.LittleEndian, valueSize)
		file.Write([]byte(key))
		file.Write([]byte(value))

		sst.index[key] = offset
		offset += 8 + 8 + keySize + valueSize
	}
	sst.size += offset

	return nil
}

func (sst *SSTable) Read(key string) (string, bool) {
	offset, ok := sst.index[key]
	if !ok {
		return "", false
	}

	file, err := os.Open(sst.filename)
	if err != nil {
		return "", false
	}
	defer file.Close()

	file.Seek(offset, 0)

	var keySize, valueSize int64
	binary.Read(file, binary.LittleEndian, &keySize)
	binary.Read(file, binary.LittleEndian, &valueSize)

	keyBytes := make([]byte, keySize)
	valueBytes := make([]byte, valueSize)

	file.Read(keyBytes)
	file.Read(valueBytes)

	sst.readCounts[key]++
	sst.lastReadTimes[key] = time.Now()

	return string(valueBytes), true
}
