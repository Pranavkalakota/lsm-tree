package lsm;

import lsm.compaction.Compactor;
import lsm.memtable.Entry;
import lsm.memtable.MemTable;
import lsm.memtable.VersionedKey;
import lsm.sstable.BlockCache;
import lsm.sstable.MergingCursor;
import lsm.sstable.RowSource;
import lsm.sstable.SSTableReader;
import lsm.sstable.SSTableWriter;
import lsm.wal.DurabilityMode;
import lsm.wal.WriteAheadLog;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class LSMStoreEngine implements StorageEngine {

    private static final long DEFAULT_MEMTABLE_SIZE = 4 * 1024 * 1024; // 4 MB

    /**
     * Stripes of the write path. More shards means less lock contention and
     * more concurrent fsyncs, at the cost of more open logs and smaller
     * MemTables, which flush more often.
     */
    private static final int DEFAULT_SHARDS = 8;

    /**
     * Least MemTable a shard is worth having.
     *
     * <p>The store's bound is split across shards, so sharding a small one
     * just makes every shard flush almost immediately: eight shards of a 64
     * byte MemTable is eight bytes each. Below this the store keeps fewer
     * shards rather than grinding itself into tiny tables.
     */
    private static final long MIN_SHARD_MEMTABLE = 64 * 1024;

    /**
     * How long the store must go without a write before compaction starts
     * clearing level 0 below its usual trigger.
     *
     * <p>Deliberately not a second or two. A bursty workload pauses constantly,
     * and treating every pause as idle would spend write bandwidth rewriting
     * files that are about to be written to again. Half a minute is long
     * enough to mean the store has actually stopped.
     */
    private static final long DEFAULT_IDLE_MILLIS = 30_000;
    private static final long DEFAULT_BLOCK_CACHE_SIZE = 16 * 1024 * 1024; // 16 MB
    private static final String TABLE_SUFFIX = ".sst";

    /**
     * One stripe of the write path: its own log, MemTable and lock.
     *
     * <p>A key belongs to exactly one shard, so shards never have to agree on
     * ordering with each other and each log can be replayed on its own. That
     * is what makes splitting the lock safe: the ordering guarantee a single
     * lock used to provide only ever mattered within a key.
     */
    private static final class Shard {
        final MemTable memTable;
        final WriteAheadLog wal;
        final ReentrantLock lock = new ReentrantLock();

        Shard(MemTable memTable, WriteAheadLog wal) {
            this.memTable = memTable;
            this.wal = wal;
        }
    }

    private final Shard[] shards;
    private final Path dataDir;
    private final FileChannel lockChannel;
    private final FileLock lock;
    private volatile boolean closed = false;

    /**
     * The live table set, ordered for reading: level ascending, and within a
     * level newest sequence first. Swapped wholesale rather than mutated, so a
     * lookup reading the reference sees one consistent set for its whole scan
     * even while a compaction is replacing half of it.
     */
    private final AtomicReference<List<SSTableReader>> tables =
            new AtomicReference<>(List.of());

    /** Shared by every reader, so the memory bound belongs to the store. */
    private final BlockCache blockCache;

    /** Guards the read-modify-write of {@link #tables} against flush vs compaction. */
    private final ReentrantLock installLock = new ReentrantLock();

    private final AtomicLong nextSequence = new AtomicLong();

    /**
     * The version to stamp on the next write. Separate from {@link #nextSequence},
     * which numbers files: one counts writes, the other counts tables, and
     * conflating them would tie a key's history to how often the store flushed.
     *
     * <p>Handed out under a shard's lock, so a version is always applied to the
     * MemTable and the log by the thread that took it, before any other write
     * in that shard can proceed.
     */
    private final AtomicLong nextVersion = new AtomicLong(1);

    /**
     * Open snapshots, counted by the version they read at. Sorted, so the
     * oldest live version — the one compaction must not discard past — is the
     * first key. Several readers can share a version, hence the count rather
     * than a set.
     */
    private final ConcurrentSkipListMap<Long, Integer> openSnapshots =
            new ConcurrentSkipListMap<>();

    /**
     * When the last write landed. Compaction leaves a busy store alone and
     * only reclaims below its trigger once writing has stopped, so this is
     * the clock it checks.
     */
    private volatile long lastWriteNanos = System.nanoTime();

    private final ScheduledExecutorService idleTimer;
    private final long idleMillis;
    private final Compactor compactor;
    private final ExecutorService compactionThread;
    private final AtomicBoolean compactionQueued = new AtomicBoolean();
    private final AtomicBoolean idleQueued = new AtomicBoolean();

    public LSMStoreEngine(Path dataDir) {
        this(dataDir, DEFAULT_MEMTABLE_SIZE);
    }

    public LSMStoreEngine(Path dataDir, long memTableMaxSize) {
        this(dataDir, memTableMaxSize, DurabilityMode.BUFFERED);
    }

    public LSMStoreEngine(Path dataDir, long memTableMaxSize, DurabilityMode durability) {
        this(dataDir, memTableMaxSize, durability, defaultShards(durability, memTableMaxSize));
    }

    /**
     * How many shards a mode wants, which is not the same answer for both.
     *
     * <p>Buffered writes are limited by contention, so splitting the path
     * helps: measured 454K to 648K writes/sec at two writers, 378K to 637K at
     * four. Synced writes are limited by the disk, and group commit already
     * fixes that by letting many writers share one fsync. Splitting the log
     * splits the thing being batched, and measured 872 down to 446 writes/sec
     * at eight writers. So sharding is applied where it helps and withheld
     * where it does not.
     */
    private static int defaultShards(DurabilityMode mode, long memTableMaxSize) {
        if (mode == DurabilityMode.SYNC) {
            return 1;
        }
        long affordable = Math.max(1, memTableMaxSize / MIN_SHARD_MEMTABLE);
        return (int) Math.min(DEFAULT_SHARDS, affordable);
    }

    public LSMStoreEngine(Path dataDir, long memTableMaxSize, DurabilityMode durability,
            int shardCount) {
        this(dataDir, memTableMaxSize, durability, shardCount, DEFAULT_BLOCK_CACHE_SIZE);
    }

    /**
     * @param blockCacheBytes memory for decoded blocks. Sized below the working
     *                        set, reads start paying for I/O and decompression
     *                        again, which is the regime a benchmark has to
     *                        measure if its numbers are to mean anything.
     */
    public LSMStoreEngine(Path dataDir, long memTableMaxSize, DurabilityMode durability,
            int shardCount, long blockCacheBytes) {
        this(dataDir, memTableMaxSize, durability, shardCount, blockCacheBytes,
                DEFAULT_IDLE_MILLIS);
    }

    /**
     * @param idleMillis quiet period before level 0 is cleared below its usual
     *                   trigger. Package private because shortening it is only
     *                   useful to a test that does not want to wait out the
     *                   real one.
     */
    LSMStoreEngine(Path dataDir, long memTableMaxSize, DurabilityMode durability,
            int shardCount, long blockCacheBytes, long idleMillis) {
        this.blockCache = new BlockCache(blockCacheBytes);
        this.idleMillis = idleMillis;
        try {
            this.dataDir = dataDir;
            Files.createDirectories(dataDir);

            // Acquire an exclusive file lock to prevent concurrent engine instances
            Path lockPath = dataDir.resolve("LOCK");
            this.lockChannel = FileChannel.open(lockPath,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            this.lock = lockChannel.tryLock();
            if (this.lock == null) {
                lockChannel.close();
                throw new IOException(
                        "Another engine instance holds the lock on " + dataDir);
            }

            loadExistingTables();
            // Above every version any surviving table holds, read from table
            // metadata rather than by scanning a single data block.
            for (SSTableReader table : tables.get()) {
                nextVersion.updateAndGet(current -> Math.max(current, table.maxVersion() + 1));
            }

            this.compactor = new Compactor(dataDir, blockCache,
                    nextSequence::getAndIncrement, this::oldestSnapshotVersion);
            this.compactionThread = Executors.newSingleThreadExecutor(runnable -> {
                Thread thread = new Thread(runnable, "lsm-compaction");
                thread.setDaemon(true);
                return thread;
            });
            this.idleTimer = Executors.newSingleThreadScheduledExecutor(runnable -> {
                Thread thread = new Thread(runnable, "lsm-idle");
                thread.setDaemon(true);
                return thread;
            });
            idleTimer.scheduleWithFixedDelay(this::idleTick,
                    idleMillis, idleMillis, TimeUnit.MILLISECONDS);

            // The bound is for the store, so split it rather than handing each
            // shard the whole thing and using shardCount times the memory.
            long perShard = Math.max(1, memTableMaxSize / shardCount);
            MemTable[] recovered = new MemTable[shardCount];
            for (int i = 0; i < shardCount; i++) {
                recovered[i] = new MemTable(perShard);
            }
            recoverLogs(recovered);
            for (MemTable table : recovered) {
                nextVersion.updateAndGet(current -> Math.max(current, table.maxVersion() + 1));
            }

            this.shards = new Shard[shardCount];
            for (int i = 0; i < shardCount; i++) {
                Path walPath = dataDir.resolve(walName(i));
                // Start a fresh log so replayed records are not replayed again,
                // then write the recovered state back into it.
                Files.deleteIfExists(walPath);
                WriteAheadLog wal = new WriteAheadLog(walPath, durability);
                // Every version, not just the newest: a snapshot read depends
                // on the history, so recovery has to put all of it back.
                for (var entry : recovered[i].versionedEntries().entrySet()) {
                    VersionedKey key = entry.getKey();
                    Entry e = entry.getValue();
                    if (e.isTombstone()) {
                        wal.appendDelete(key.key(), key.version());
                    } else {
                        wal.appendPut(key.key(), e.value().orElseThrow(), key.version());
                    }
                }
                shards[i] = new Shard(recovered[i], wal);
            }
            // Dropped only once every shard log holds its share. A crash before
            // this point replays both and lands on the same state.
            Files.deleteIfExists(dataDir.resolve("wal.log"));

            scheduleCompaction();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to initialize storage engine", e);
        }
    }

    private static String walName(int shard) {
        return "wal_" + shard + ".log";
    }

    /**
     * Picks the shard a key lives in.
     *
     * <p>String.hashCode clusters badly for short similar keys, which would
     * pile most of the load onto one shard and undo the point of sharding, so
     * the high bits are folded down first.
     */
    private static int shardIndex(String key, int shardCount) {
        int hash = key.hashCode();
        hash ^= hash >>> 16;
        return Math.floorMod(hash, shardCount);
    }

    private Shard shardFor(String key) {
        return shards[shardIndex(key, shards.length)];
    }

    /**
     * Replays every log in the directory and routes each record to the shard
     * that key belongs to now.
     *
     * <p>Replaying log N straight back into shard N would be faster and is
     * wrong: the shard count is not persisted, so a store written with one
     * shard and reopened with eight would leave every record in shard zero
     * while lookups went looking in the shard the key now hashes to. The
     * records would be on disk and unreachable. Routing on the way in also
     * covers a store written before the log was sharded at all, whose single
     * wal.log is just another log to read.
     */
    private void recoverLogs(MemTable[] recovered) throws IOException {
        List<Path> logs = new ArrayList<>();
        try (Stream<Path> files = Files.list(dataDir)) {
            for (Path path : files.sorted().toList()) {
                String name = path.getFileName().toString();
                if (name.startsWith("wal_") || name.equals("wal.log")) {
                    logs.add(path);
                }
            }
        }

        MemTable all = new MemTable(Long.MAX_VALUE);
        for (Path log : logs) {
            WriteAheadLog.replay(log, all);
        }
        for (var entry : all.versionedEntries().entrySet()) {
            VersionedKey key = entry.getKey();
            MemTable target = recovered[shardIndex(key.key(), recovered.length)];
            Entry value = entry.getValue();
            if (value.isTombstone()) {
                target.delete(key.key(), key.version());
            } else {
                target.put(key.key(), value.value().orElseThrow(), key.version());
            }
        }

        // Logs from a different shard count would otherwise be replayed again
        // on the next open, resurrecting whatever they hold.
        for (Path log : logs) {
            Files.deleteIfExists(log);
        }
    }

    @Override
    public void put(String key, String value) {
        if (key == null) throw new IllegalArgumentException("key must not be null");
        if (value == null) throw new IllegalArgumentException("value must not be null");
        commit(key, value, true);
    }

    @Override
    public Optional<String> get(String key) {
        checkNotClosed();
        if (key == null) throw new IllegalArgumentException("key must not be null");

        // Only one shard can hold this key, so the others need not be asked.
        Entry entry = shardFor(key).memTable.get(key);
        if (entry != null) {
            return entry.isTombstone() ? Optional.empty() : entry.value();
        }

        // Newest table first: the first one that mentions the key holds the
        // current answer, and a tombstone there ends the search rather than
        // falling through to the stale value an older table still carries.
        for (SSTableReader table : tables.get()) {
            if (!couldHold(table, key) || !table.acquire()) {
                continue;
            }
            try {
                Entry found = table.get(key);
                if (found != null) {
                    return found.isTombstone() ? Optional.empty() : found.value();
                }
            } catch (IOException e) {
                throw new UncheckedIOException("Read failed", e);
            } finally {
                table.release();
            }
        }

        return Optional.empty();
    }

    @Override
    public Optional<String> get(String key, Snapshot at) {
        checkNotClosed();
        if (key == null) throw new IllegalArgumentException("key must not be null");
        long version = versionOf(at);

        Entry entry = shardFor(key).memTable.get(key, version);
        if (entry != null) {
            return entry.isTombstone() ? Optional.empty() : entry.value();
        }

        // A table holding only versions newer than the snapshot answers
        // nothing for this key, so the search carries on into older tables
        // rather than stopping at the first table that mentions it.
        for (SSTableReader table : tables.get()) {
            if (!couldHold(table, key) || !table.acquire()) {
                continue;
            }
            try {
                Entry found = table.get(key, version);
                if (found != null) {
                    return found.isTombstone() ? Optional.empty() : found.value();
                }
            } catch (IOException e) {
                throw new UncheckedIOException("Read failed", e);
            } finally {
                table.release();
            }
        }

        return Optional.empty();
    }

    private static long versionOf(Snapshot at) {
        if (at == null) {
            throw new IllegalArgumentException("snapshot must not be null");
        }
        if (!at.isOpen()) {
            // Answering anyway would read at a version whose history the store
            // may already have reclaimed, giving a wrong answer rather than an
            // error. Better to say so.
            throw new IllegalStateException("Snapshot is closed: " + at);
        }
        return at.version();
    }

    /** Cheap range check that skips opening a block for a key the table cannot have. */
    private static boolean couldHold(SSTableReader table, String key) {
        String min = table.minKey();
        return min != null
                && key.compareTo(min) >= 0
                && key.compareTo(table.maxKey()) <= 0;
    }

    /**
     * {@inheritDoc}
     *
     * <p>The MemTable range is copied before the table list is snapshotted,
     * and the order matters. A flush publishes its new table before clearing
     * the MemTable, so reading the MemTable first means a key is either still
     * in the copy or already in a table this scan will see. Snapshotting the
     * other way round leaves a window where a flush lands in between and the
     * scan sees the key in neither.
     *
     * <p>The copy is bounded by the MemTable's own size limit, and taking it
     * avoids holding the write lock for the life of the scan.
     */
    @Override
    public Stream<Row> scan(String fromInclusive, String toExclusive) {
        return scan(fromInclusive, toExclusive, VersionedKey.NEWEST);
    }

    /**
     * {@inheritDoc}
     *
     * <p>Rows newer than the snapshot are stepped over before anything else
     * looks at them, so they cannot settle a key and hide the older version
     * the reader is entitled to.
     */
    @Override
    public Stream<Row> scan(String fromInclusive, String toExclusive, Snapshot at) {
        return scan(fromInclusive, toExclusive, versionOf(at));
    }

    private Stream<Row> scan(String fromInclusive, String toExclusive, long snapshot) {
        checkNotClosed();
        if (fromInclusive != null && toExclusive != null
                && fromInclusive.compareTo(toExclusive) > 0) {
            return Stream.empty();
        }

        // Keys scatter across shards, so a range can start in any of them.
        // Merged into one sorted view first, which also keeps them ahead of
        // every table in the merge order.
        // Keyed by version as well as key, because the merge orders records
        // that way and these have to interleave correctly with stored tables.
        TreeMap<VersionedKey, Entry> buffered = new TreeMap<>();
        for (Shard shard : shards) {
            buffered.putAll(shard.memTable.versionedRange(
                    fromInclusive, toExclusive, snapshot));
        }

        List<SSTableReader> held = new ArrayList<>();
        List<RowSource> sources = new ArrayList<>();
        sources.add(RowSource.ofVersioned(buffered.entrySet().iterator()));

        try {
            for (SSTableReader table : tables.get()) {
                if (!mightOverlap(table, fromInclusive, toExclusive) || !table.acquire()) {
                    continue;
                }
                held.add(table);
                sources.add(fromInclusive == null
                        ? table.cursor()
                        : table.cursorFrom(fromInclusive));
            }
        } catch (IOException e) {
            held.forEach(SSTableReader::release);
            throw new UncheckedIOException("Scan failed to start", e);
        }

        MergingCursor merged = new MergingCursor(sources);
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(
                        rows(merged, toExclusive, snapshot),
                        Spliterator.ORDERED | Spliterator.SORTED), false)
                // Each table stays alive until the scan finishes with it, even
                // if compaction retires it in the meantime.
                .onClose(() -> {
                    merged.close();
                    held.forEach(SSTableReader::release);
                });
    }

    /** Drops tombstones and stops at the upper bound; keys ascend, so it can stop early. */
    private Iterator<Row> rows(MergingCursor merged, String toExclusive, long snapshot) {
        return new Iterator<>() {
            // Declared before pending, whose initializer calls advance() and
            // writes this; the other order would reset it straight afterwards.
            private String decided;

            private Row pending = advance();

            private Row advance() {
                try {
                    while (merged.hasNext()) {
                        SSTableReader.Row row = merged.next();
                        if (toExclusive != null && row.key().compareTo(toExclusive) >= 0) {
                            return null;
                        }
                        // Checked before the key is settled, not after: a
                        // version this reader cannot see must not be the one
                        // that decides the key, or the visible older version
                        // below it would be skipped as stale history.
                        if (row.version() > snapshot) {
                            continue;
                        }
                        // The merge hands over a key's whole history, newest
                        // first, so the first record settles the key and the
                        // rest of its run is skipped. A tombstone settles it
                        // too, by deciding the key has no value to return.
                        if (row.key().equals(decided)) {
                            continue;
                        }
                        decided = row.key();
                        if (!row.value().isTombstone()) {
                            return new Row(row.key(), row.value().value().orElseThrow());
                        }
                    }
                    return null;
                } catch (IOException e) {
                    throw new UncheckedIOException("Scan failed", e);
                }
            }

            @Override
            public boolean hasNext() {
                return pending != null;
            }

            @Override
            public Row next() {
                if (pending == null) {
                    throw new NoSuchElementException();
                }
                Row row = pending;
                pending = advance();
                return row;
            }
        };
    }

    /** Whether a table's key range could intersect the requested one. */
    private static boolean mightOverlap(SSTableReader table, String from, String to) {
        if (table.minKey() == null) {
            return false;
        }
        if (from != null && table.maxKey().compareTo(from) < 0) {
            return false;
        }
        return to == null || table.minKey().compareTo(to) < 0;
    }

    @Override
    public void delete(String key) {
        if (key == null) throw new IllegalArgumentException("key must not be null");
        commit(key, null, false);
    }

    /**
     * Applies one write, then commits it.
     *
     * <p>The append and the MemTable insert happen under the lock because the
     * log has to record them in the order the MemTable accepted them. The
     * commit deliberately happens outside it: an fsync takes milliseconds, and
     * holding the lock across it would force every other writer to wait out a
     * disk round trip that would have covered their record too.
     */
    private void commit(String key, String value, boolean isPut) {
        Shard shard = shardFor(key);
        long seq;
        shard.lock.lock();
        try {
            checkNotClosed();
            // Taken inside the lock, so every version handed out has been
            // applied by the time the lock is released. Nothing can observe a
            // version that exists but has not landed yet.
            long version = nextVersion.getAndIncrement();
            if (isPut) {
                seq = shard.wal.appendPut(key, value, version);
                shard.memTable.put(key, value, version);
            } else {
                seq = shard.wal.appendDelete(key, version);
                shard.memTable.delete(key, version);
            }
            flushIfFull(shard);
        } catch (IOException e) {
            throw new UncheckedIOException("Write failed", e);
        } finally {
            shard.lock.unlock();
        }

        lastWriteNanos = System.nanoTime();

        // Outside the lock, and now per shard: concurrent writers to different
        // shards commit in parallel instead of queueing behind one another.
        try {
            shard.wal.syncTo(seq);
        } catch (IOException e) {
            throw new UncheckedIOException("Write failed to commit", e);
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p>Every shard lock is taken for the moment this runs. A version is
     * handed out and applied under one of those locks, so holding them all
     * means every version issued so far has landed, and the current version is
     * therefore a point the store genuinely passed through rather than one it
     * is halfway into. Without that, a snapshot could include version 7 while
     * version 6 was still in flight, and a reader would see a half-written
     * moment that never existed.
     *
     * <p>Snapshots are rare next to writes, so briefly blocking writers is the
     * cheap side of that trade.
     *
     * <p>Note this says nothing about durability: the fsync happens outside
     * the write lock, so a snapshot can include a write that is visible but
     * not yet committed to disk. For a reader that is the right answer — the
     * write was acknowledged by the store it is reading.
     */
    @Override
    public Snapshot snapshot() {
        checkNotClosed();
        for (Shard shard : shards) {
            shard.lock.lock();
        }
        long version;
        try {
            checkNotClosed();
            version = nextVersion.get() - 1;
            openSnapshots.merge(version, 1, Integer::sum);
        } finally {
            for (Shard shard : shards) {
                shard.lock.unlock();
            }
        }
        return new Snapshot(version, () -> releaseSnapshot(version));
    }

    private void releaseSnapshot(long version) {
        openSnapshots.compute(version, (key, count) ->
                count == null || count <= 1 ? null : count - 1);
    }

    /**
     * The oldest version any open snapshot reads at, or {@link Long#MAX_VALUE}
     * when none is open, which tells compaction nothing has to be preserved.
     *
     * <p>Safe to read without coordinating with an in-flight compaction, which
     * samples this once and keeps working from that value. A snapshot opened
     * mid-merge always reads at the current version, and a merge keeps the
     * newest version of every key whatever its watermark, so the newcomer's
     * answers survive regardless. That argument only holds because snapshots
     * cannot be opened at an arbitrary past version — if they ever can, this
     * needs revisiting.
     */
    private long oldestSnapshotVersion() {
        Map.Entry<Long, Integer> oldest = openSnapshots.firstEntry();
        return oldest == null ? Long.MAX_VALUE : oldest.getKey();
    }

    @Override
    public void close() {
        // Every shard, so a write in flight on any of them finishes first.
        for (Shard shard : shards) {
            shard.lock.lock();
        }
        try {
            if (closed) {
                return;
            }
            closed = true;
        } finally {
            for (Shard shard : shards) {
                shard.lock.unlock();
            }
        }

        // Outside the write lock: a compaction in flight may be waiting on the
        // install lock, and holding the write lock here would not help it finish.
        idleTimer.shutdownNow();
        compactionThread.shutdown();
        try {
            if (!compactionThread.awaitTermination(30, TimeUnit.SECONDS)) {
                compactionThread.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            compactionThread.shutdownNow();
        }

        try {
            // The MemTable is deliberately not flushed here. Every write is
            // already in the WAL, so replay restores it on the next open;
            // flushing instead would litter the directory with a tiny table
            // each time a process opens and closes the store.
            for (Shard shard : shards) {
                shard.wal.close();
            }
            tables.get().forEach(SSTableReader::close);
            lock.release();
            lockChannel.close();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to close engine", e);
        }
    }

    private void flushIfFull(Shard shard) throws IOException {
        if (shard.memTable.shouldFlush()) {
            flush(shard);
        }
    }

    /**
     * Writes the MemTable out as a new level 0 table and starts a fresh WAL.
     *
     * <p>Step order carries the crash guarantee. The table is fsynced and
     * atomically renamed before the WAL is reset, so a crash anywhere in
     * between leaves the writes in the WAL and replay recovers them. The
     * reader is published before the MemTable is cleared, so no window exists
     * where a concurrent lookup can see neither copy.
     */
    private void flush(Shard shard) throws IOException {
        if (shard.memTable.isEmpty()) {
            return;
        }
        // The sequence is shared, so tables from different shards still order
        // against each other correctly in the read path.
        Path path = dataDir.resolve(
                String.format("L0_%06d%s", nextSequence.getAndIncrement(), TABLE_SUFFIX));
        SSTableWriter.writeVersioned(path, shard.memTable.versionedEntries());
        syncDirectory();

        install(List.of(new SSTableReader(path, blockCache)), List.of());
        shard.memTable.clear();
        shard.wal.reset();

        scheduleCompaction();
    }

    /** Swaps {@code removed} out of the live set and {@code added} in, atomically. */
    private void install(List<SSTableReader> added, List<SSTableReader> removed) {
        installLock.lock();
        try {
            List<SSTableReader> next = new ArrayList<>(tables.get());
            next.removeAll(removed);
            next.addAll(added);
            next.sort(Comparator.comparingInt(SSTableReader::level)
                    .thenComparing(Comparator.comparingLong(SSTableReader::sequence).reversed()));
            tables.set(List.copyOf(next));
        } finally {
            installLock.unlock();
        }
    }

    /**
     * Asks the background thread to look for work. Runs at most one pass at a
     * time; a flush arriving mid-compaction queues another pass rather than a
     * second concurrent one.
     */
    private void scheduleCompaction() {
        if (closed || !compactionQueued.compareAndSet(false, true)) {
            return;
        }
        try {
            compactionThread.execute(this::compactUntilSettled);
        } catch (RuntimeException e) {
            compactionQueued.set(false);   // executor shutting down
        }
    }

    /**
     * Runs a compaction pass once the store has been quiet long enough.
     *
     * <p>Without this, deletes are only ever reclaimed as a side effect of
     * more writing: a store that goes quiet with two files in level 0 keeps
     * whatever tombstones they hold, and the values they shadow, forever.
     */
    private void idleTick() {
        if (closed || System.nanoTime() - lastWriteNanos < idleMillis * 1_000_000L) {
            return;
        }
        compactIdleNow();
    }

    /** Asks for an idle pass now, whatever the clock says. Visible for tests. */
    void compactIdleNow() {
        if (closed || !idleQueued.compareAndSet(false, true)) {
            return;
        }
        try {
            compactionThread.execute(() -> compactUntilSettled(true));
        } catch (RuntimeException e) {
            idleQueued.set(false);   // executor shutting down
        }
    }

    private void compactUntilSettled() {
        compactUntilSettled(false);
    }

    private void compactUntilSettled(boolean idle) {
        if (idle) {
            idleQueued.set(false);
        } else {
            compactionQueued.set(false);
        }
        try {
            Compactor.Job job;
            while (!closed && (job = compactor.choose(tables.get(), idle)) != null) {
                List<SSTableReader> fresh = compactor.open(compactor.run(job));
                install(fresh, job.inputs());
                syncDirectory();
                // Retired only after the swap: a lookup that started against
                // the old set keeps its file alive through its own reference.
                job.inputs().forEach(SSTableReader::retire);
            }
        } catch (IOException e) {
            // A failed compaction leaves the live set untouched, so the store
            // stays correct and merely keeps the tables it already had.
        }
    }

    private void loadExistingTables() throws IOException {
        List<Path> present;
        try (Stream<Path> files = Files.list(dataDir)) {
            present = files.toList();
        }

        List<Path> found = new ArrayList<>();
        for (Path path : present) {
            String name = path.getFileName().toString();
            if (name.endsWith(".tmp")) {
                // Half-written table left by a flush or compaction that crashed.
                // Its contents are still in the WAL or in the inputs that were
                // never retired, so dropping it loses nothing.
                Files.deleteIfExists(path);
            } else if (name.endsWith(TABLE_SUFFIX)) {
                found.add(path);
            }
        }

        List<SSTableReader> opened = new ArrayList<>();
        for (Path path : found) {
            SSTableReader reader = new SSTableReader(path, blockCache);
            opened.add(reader);
            nextSequence.updateAndGet(current -> Math.max(current, reader.sequence() + 1));
        }
        install(opened, List.of());
    }

    /**
     * Forces the directory entry for a freshly renamed table to disk. Without
     * this the table's bytes are durable but the name pointing at them may not
     * survive a crash. Not every platform allows opening a directory, so this
     * is best effort.
     */
    private void syncDirectory() {
        try (FileChannel dir = FileChannel.open(dataDir, StandardOpenOption.READ)) {
            dir.force(true);
        } catch (IOException ignored) {
            // Unsupported on this filesystem; the table itself is still fsynced.
        }
    }

    private void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException("Storage engine is closed");
        }
    }
}
