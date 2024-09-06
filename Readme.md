# LSM-Tree Based Key-Value Store

## Overview

This project implements a Log-Structured Merge-Tree (LSM-Tree) based key-value store in Go. It's designed to provide efficient write operations while maintaining good read performance, making it suitable for write-heavy workloads.

## Features

- In-memory balanced tree (AVL) for recent writes
- On-disk storage using Sorted String Tables (SSTables)
- Write-Ahead Log (WAL) for crash recovery and data durability
- Background compaction process to optimize storage and query performance
- Bloom filters for efficient negative lookups
- RESTful API for CRUD operations
- Concurrent access support using Go's synchronization primitives

## Installation

### Prerequisites

- Go 1.16 or higher
- Git

### Steps

1. Clone the repository:
   ```
   git clone https://github.com/yourusername/lsm-tree-kv-store.git
   cd lsm-tree-kv-store
   ```

2. Build the project:
   ```
   go build ./...
   ```

## Usage

### Starting the Server

To start the key-value store server, run:

```
go run main.go
```

By default, the server will start on port 8080.

### API Endpoints

The following RESTful API endpoints are available:

1. **PUT** a key-value pair:
   ```
   POST http://localhost:8080/put
   Content-Type: application/json

   {
     "key": "mykey",
     "value": "myvalue"
   }
   ```

2. **GET** a value by key:
   ```
   GET http://localhost:8080/get/mykey
   ```

3. **DELETE** a key-value pair:
   ```
   DELETE http://localhost:8080/delete/mykey
   ```

### Using as a Library

You can also use this project as a library in your Go applications:

1. Import the package:
   ```go
   import "github.com/yourusername/lsm-tree-kv-store/lsm"
   ```

2. Create a new LSM-Tree instance:
   ```go
   tree, err := lsm.NewLSMTree()
   if err != nil {
       log.Fatalf("Failed to create LSM-tree: %v", err)
   }
   ```

3. Perform operations:
   ```go
   // Put
   err = tree.Put("key", "value")

   // Get
   value, found := tree.Get("key")

   // Delete
   err = tree.Delete("key")
   ```

4. Close the tree when done:
   ```go
   err = tree.Close()
   ```

## Configuration

The LSM-Tree can be configured by modifying the following constants in `config.go`:

- `MemtableSize`: Maximum size of the memtable before flushing to disk
- `BloomFilterSize`: Size of the Bloom filter for each SSTable
- `CompactionInterval`: Time interval for running the background compaction process

## Architecture

The key components of this LSM-Tree implementation are:

1. **Memtable**: An in-memory AVL tree for storing recent writes.
2. **SSTable**: On-disk storage for sorted key-value pairs.
3. **Write-Ahead Log (WAL)**: Ensures durability by logging operations before they're applied to the memtable.
4. **Bloom Filter**: Reduces unnecessary disk reads by quickly checking if a key might exist in an SSTable.
5. **Compaction Process**: Merges SSTables to optimize storage and query performance.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Acknowledgments

- This project was inspired by the LSM-Tree design described in the "BigTable" paper by Chang et al.
