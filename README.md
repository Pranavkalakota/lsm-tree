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
| MemTable (skip list) | Done |
| Write-ahead log, crash recovery | Done |
| SSTable flush and read path | Done |
| Block checksums (CRC32C) | Done |
| Block cache | Done |
| Concurrent readers and writers | Done |
| Bloom filters | Planned |
| Compaction | Planned |
| Benchmarks | Planned |

Performance targets are **not yet measured**. The engine fsyncs on every
write, which caps throughput near 330 writes/sec on a laptop SSD. That is a
deliberate tradeoff for now: durability over speed, with group commit as the
way out when the number starts to matter.

## Design Notes

**Tables are immutable.** That one property does most of the work: a block's
bytes can never change, so the cache needs no invalidation protocol, and a
reader can hold a block indefinitely without coordinating with anything.

**Reads take no locks.** Lookups use positional reads rather than a shared
file cursor, the table list is copy-on-write, and the MemTable is a skip
list. A read-write lock would have been simpler and would have blocked every
reader behind every write.

**Flush ordering carries the crash guarantee.** The table is fsynced and
renamed before the log is reset, so a crash in the gap leaves the writes
recoverable from the log. The new table is published before the MemTable is
cleared, so no reader sees a key exist in neither.

**Deletes are lazy.** A delete writes a tombstone; the old value stays on
disk until compaction drops it. This keeps writes fast and is why a deleted
key's bytes are still in the file afterwards.

## Roadmap

- **Leveled compaction** — size-tiered first, leveled after
- **Bloom filters** — skip tables that cannot hold the key
- **Group commit** — amortize fsync across concurrent writers

## Tech Stack

- Java 17
- Maven
- JUnit 5
- Caffeine (block cache)
- Guava (Bloom filters)
- JMH (benchmarking)
