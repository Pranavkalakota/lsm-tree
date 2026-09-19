# LSM-Tree Storage Engine

A simplified LSM-tree (Log-Structured Merge-Tree) key-value storage engine built from scratch in Java, inspired by the architecture of RocksDB and LevelDB.

## What is an LSM-Tree?

LSM-trees turn random writes into sequential writes by buffering updates in memory (MemTable), then flushing them to immutable sorted files on disk (SSTables). This makes writes very fast at the cost of slightly more complex reads, which must check multiple sources. Background compaction merges and deduplicates SSTables to keep read performance and disk usage in check.

## Architecture

```
Client
  |
  v
StorageEngine (facade)
  |
  |--> WAL (Write-Ahead Log)         <- crash safety
  |--> MemTable (in-memory sorted map)
  |
  |  (when MemTable exceeds size threshold)
  v
Flush to SSTable (Level 0)            <- immutable, sorted, on disk
  |
  v
Compaction                            <- merges SSTables, reclaims space
```

## API

```java
public interface StorageEngine {
    void put(String key, String value);
    Optional<String> get(String key);
    void delete(String key);
    void close();
}
```

## Components

- **MemTable** — In-memory sorted structure (TreeMap) holding recent writes. Tracks size and triggers flush when threshold is exceeded.
- **Write-Ahead Log (WAL)** — Append-only log ensuring crash safety. Every write hits the WAL before the MemTable.
- **SSTable** — Immutable sorted files on disk with a sparse index for efficient lookups.
- **Bloom Filter** — Probabilistic filter per SSTable to skip unnecessary disk reads.
- **Compaction** — Background merging of SSTables across levels, dropping stale keys and tombstones.

## Build & Test

Requires Java 17+ and Maven.

```bash
mvn compile
mvn test
```

## Known Simplifications

- Single-writer model (no concurrent writer support)
- No block checksums
- No block cache
- Size-tiered compaction only (no leveled compaction)

## Tech Stack

- Java 17
- Maven
- JUnit 5
- Guava (Bloom filters)
- JMH (benchmarking)
