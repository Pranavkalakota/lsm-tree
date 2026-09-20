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
mvn test
```

All tests should pass. They cover crash recovery, truncated and corrupted
logs, unicode round trips, read precedence across tables, concurrent reads,
and randomised operation sequences checked against a reference map.

## Try It

The test suite proves the engine is correct but shows none of its behaviour.
The shell makes it visible:

```bash
mvn compile
java -cp target/classes lsm.Shell ./demo-data
```

```
> put name ada
ok
> put lang java
ok
> put city london
ok  (memtable filled up, flushed to disk)
> files
wal.log                     0 bytes   <- writes not yet flushed
L0_000000.sst             117 bytes
```

The log drops to zero the instant a flush moves those writes into a table.
Exit, run the same command again, and `get name` still answers `ada` — that
is the log and the table doing their jobs. `del` writes a tombstone rather
than reclaiming space, which is why a deleted key's bytes stay on disk until
compaction removes them.

The MemTable bound defaults to 32 bytes here so a flush happens while you
watch; the engine's real default is four megabytes.

## Status

| Component | State |
| --- | --- |
| MemTable | Done |
| Write-ahead log, crash recovery | Done |
| SSTable flush and read path | Done |
| Bloom filters | Planned |
| Compaction | Planned |
| Benchmarks | Planned |

Performance targets are **not yet measured**. The engine currently fsyncs on
every write, which caps throughput near 330 writes/sec on a laptop SSD; a
configurable durability mode and group commit are the next piece of work.

## Roadmap

Deliberately deferred, each planned rather than permanent:

- **Concurrent writers** — reads are already safe to share; writes are not
- **Block checksums** — the SSTable footer carries a format version so this
  can be added without breaking existing files
- **Block cache** — decoded blocks are currently re-read on every lookup
- **Leveled compaction** — size-tiered first, leveled after

## Tech Stack

- Java 17
- Maven
- JUnit 5
- Guava (Bloom filters)
- JMH (benchmarking)
