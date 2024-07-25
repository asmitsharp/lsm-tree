package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
)

type WAL struct {
	file   *os.File
	writer *bufio.Writer
	mutex  sync.Mutex
}

func NewWAL(filename string) (*WAL, error) {
	fmt.Println(filename)
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	fmt.Println("File descriptor:", file.Fd())
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %v", err)
	}

	return &WAL{
		file:   file,
		writer: bufio.NewWriter(file),
	}, nil
}

func (w *WAL) AppendPut(key, value string) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if err := w.writer.WriteByte(1); err != nil {
		return fmt.Errorf("failed to write operation type: %v", err)
	}

	if err := binary.Write(w.writer, binary.LittleEndian, uint32(len(key))); err != nil {
		return err
	}
	if _, err := w.writer.WriteString(key); err != nil {
		return err
	}

	if err := binary.Write(w.writer, binary.LittleEndian, uint32(len(value))); err != nil {
		return err
	}
	if _, err := w.writer.WriteString(value); err != nil {
		return err
	}

	return w.writer.Flush()
}

func (w *WAL) AppendDelete(key string) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	// Write operation type (2 for Delete)
	if err := w.writer.WriteByte(2); err != nil {
		return fmt.Errorf("failed to write operation type: %v", err)
	}

	// Write key length and key
	if err := binary.Write(w.writer, binary.LittleEndian, uint32(len(key))); err != nil {
		return err
	}
	if _, err := w.writer.WriteString(key); err != nil {
		return err
	}

	return w.writer.Flush()
}

func (w *WAL) Close() error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if err := w.writer.Flush(); err != nil {
		return err
	}
	return w.file.Close()
}

func (w *WAL) Replay(applyFunc func(opType uint8, key, value string) error) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	// Check if the file is empty
	info, err := w.file.Stat()
	if err != nil {
		return fmt.Errorf("failed to get WAL file info: %v", err)
	}
	if info.Size() == 0 {
		fmt.Println("WAL file is empty")
		return nil
	}

	// Seek to the beginning of the file
	_, err = w.file.Seek(0, 0)
	if err != nil {
		return fmt.Errorf("failed to seek WAL file: %v", err)
	}

	reader := bufio.NewReader(w.file)
	fmt.Printf("WAL file descriptor: %d\n", w.file.Fd())

	for {
		opType, err := reader.ReadByte()
		if err != nil {
			if err == io.EOF {
				fmt.Println("Reached end of WAL file")
				break
			}
			return fmt.Errorf("failed to read operation type from WAL: %v", err)
		}

		fmt.Printf("Read operation type: %d\n", opType)

		var keyLen uint32
		if err := binary.Read(reader, binary.LittleEndian, &keyLen); err != nil {
			return fmt.Errorf("failed to read key length from WAL: %v", err)
		}

		fmt.Printf("Key length: %d\n", keyLen)

		keyBytes := make([]byte, keyLen)
		n, err := io.ReadFull(reader, keyBytes)
		if err != nil {
			return fmt.Errorf("failed to read key from WAL (read %d bytes): %v", n, err)
		}

		key := string(keyBytes)
		fmt.Printf("Read key: %s\n", key)

		var value string
		if opType == 1 { // Put operation
			var valueLen uint32
			if err := binary.Read(reader, binary.LittleEndian, &valueLen); err != nil {
				return fmt.Errorf("failed to read value length from WAL: %v", err)
			}

			fmt.Printf("Value length: %d\n", valueLen)

			valueBytes := make([]byte, valueLen)
			n, err := io.ReadFull(reader, valueBytes)
			if err != nil {
				return fmt.Errorf("failed to read value from WAL (read %d bytes): %v", n, err)
			}
			value = string(valueBytes)
			fmt.Printf("Read value: %s\n", value)
		}

		if err := applyFunc(opType, key, value); err != nil {
			return fmt.Errorf("failed to apply WAL entry: %v", err)
		}
	}

	return nil
}

// Add a new method to check WAL file permissions
func (w *WAL) CheckPermissions() error {
	info, err := w.file.Stat()
	if err != nil {
		return fmt.Errorf("failed to get WAL file info: %v", err)
	}

	fmt.Printf("WAL file permissions: %v\n", info.Mode())
	return nil
}
