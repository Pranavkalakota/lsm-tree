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

- **MemTable** — In-memory sorted structure (skip list) holding recent writes. Tracks size and triggers flush when threshold is exceeded.
- **Write-Ahead Log (WAL)** — Append-only log ensuring crash safety. Every write hits the WAL before the MemTable, under a configurable durability mode.
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
| Durability modes, group commit | Done |
| SSTable flush and read path | Done |
| Block checksums (CRC32C) | Done |
| Block cache | Done |
| Concurrent readers and writers | Done |
| Leveled compaction | Done |
| Bloom filters | Done |
| Write and read benchmarks | Done |

## Write Throughput

```bash
mvn -q compile dependency:build-classpath -Dmdep.outputFile=target/cp.txt
java -cp "target/classes:$(cat target/cp.txt)" lsm.Benchmark
```

Measured on an M-series laptop SSD (12 cores):

| Mode | Writers | writes/sec | Survives |
| --- | ---: | ---: | --- |
| BUFFERED | 1 | 481,000 | process crash |
| BUFFERED | 8 | 370,000 | process crash |
| SYNC | 1 | 263 | power loss |
| SYNC | 8 | 829 | power loss |

`BUFFERED` is the default and returns once the bytes reach the operating
system. `SYNC` waits for the disk on every write, which costs ~3.8ms and is
the entire difference between the two rows.

Two things in that table are worth reading carefully. **SYNC at eight writers
is 3.2x SYNC at one** — that is group commit: one fsync commits every record
appended before it, so writers arriving during a commit ride along with it.
Before that existed, eight writers measured 262/sec against one writer's 266,
which is to say concurrency bought nothing at all.

**The write path is sharded, but only for buffered writes.** Keys hash to one
of eight stripes, each with its own log, MemTable and lock, so writers mostly
stay out of each other's way. Measured at 454K to 648K writes/sec at two
writers and 378K to 637K at four.

Sync mode deliberately keeps a single log. Splitting it splits exactly what
group commit batches, and sharding measured 872 down to 446 writes/sec at eight
writers. The two modes have different bottlenecks, so they get opposite
treatment: contention is the limit for buffered, the disk is the limit for sync.

## Read Latency

Same command, reported after the write table. 100,000 keys, read once
compaction has settled:

| Lookup | reads/sec | p50 | p99 |
| --- | ---: | ---: | ---: |
| Hit | 5,150,000 | 0.17us | 0.46us |
| Miss, key inside the stored range | 6,097,000 | 0.17us | 0.38us |

**Those are best-case reads in two ways,** and the number only means something
with both said: the keys are read in order, which keeps hitting the same block,
and the whole working set fits in the cache. Both are measured below.

### Reads that miss the cache

The same keys, read in random order, against caches sized against the data:

| Block cache | reads/sec | p50 | p99 |
| --- | ---: | ---: | ---: |
| 64MB, holds everything | 2,459,000 | 0.38us | 0.88us |
| 4MB | 2,679,000 | 0.33us | 0.75us |
| 512KB | 1,259,000 | 0.71us | 2.21us |
| 64KB | 1,378,000 | 0.63us | 2.08us |

Two things worth taking from this. Random access costs about half of
sequential even when everything is cached, because sequential reads keep
landing in a block that is already open. And once the cache cannot hold the
working set, throughput halves again and p99 roughly triples.

**What this still does not measure is a cold operating system.** The files stay
in the page cache throughout, so these reads pay for block lookup, checksum and
decompression, but not for a disk seek. A number that included one would need a
dataset larger than RAM.

### What the bloom filter is worth

Measured against the same data written twice: once with a filter sized for the
table, once with a filter deliberately sized for a single key, which Guava
saturates so that nearly every lookup says "maybe" and falls through to a block
read. That second table stands in for having no filter at all.

| Filter | reads/sec on misses |
| --- | ---: |
| Sized for the table | 3,651,000 |
| Saturated, no help | 942,000 |

**3.9x on lookups that miss.** Note that a table already rejects any key outside
its min/max range without reading anything, and leveled compaction already
bounds a lookup to roughly one table per level, so the filter is only earning
the part neither of those catches.

A note on how these were measured: an earlier version of this harness built its
keys inside the timed loop, and `String.format` is not free at these rates. It
understated reads by roughly 3x and hid the effect of write sharding entirely.
Keys are now built before the clock starts.

A miss whose key falls outside the stored range never consults the filter at
all, so benchmarking with out-of-range keys measures nothing. The harness uses
keys that sort between real ones.

## Compression

Blocks are LZ4 compressed. What that saves depends entirely on the data, so
the benchmark reports several shapes rather than one headline ratio:

| Dataset | Raw | LZ4 | Saving |
| --- | ---: | ---: | ---: |
| Repetitive values | 3.14 MB | 393 KB | **87%** |
| JSON-like values | 2.97 MB | 587 KB | **80%** |
| Tiny values | 1.17 MB | 332 KB | 72% |
| High entropy | 3.14 MB | 2.49 MB | 21% |

A block has to compress by at least 10% to be stored compressed; below that it
is written raw. The point is not the handful of bytes saved, it is that a block
only worth 2% should not cost a decompression on every read for the rest of its
life. Blocks record their own encoding, so a file can hold both kinds and a
reader does not need to be told which to expect.

**Compression also makes cold reads faster, not slower:**

| Cold reads | reads/sec | p99 |
| --- | ---: | ---: |
| Uncompressed blocks | 968,000 | 7.25us |
| LZ4 blocks | 1,063,000 | 6.13us |

That is the opposite of the obvious guess. A compressed block is a fraction of
the size, so there is far less to read, and LZ4 decompresses faster than the
saved I/O costs. The cache holds blocks already decompressed, so a cache hit
pays nothing either way; this comparison uses a cache small enough to miss
every time, which is the case where compression could have hurt.

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

- **Sharded write path** — a single lock currently caps buffered throughput
- **Idle compaction** — deletes are only reclaimed under write pressure, so an
  idle store keeps tombstones sitting in a level 0 that never hit its trigger
- **Cold read benchmark** — current read figures have the whole dataset cached

## Tech Stack

- Java 17
- Maven
- JUnit 5
- Caffeine (block cache)
- Guava (Bloom filters)
- JMH (benchmarking)
