package lsm;

import lsm.compaction.Compactor;
import lsm.memtable.Entry;
import lsm.memtable.MemTable;
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
    private final Compactor compactor;
    private final ExecutorService compactionThread;
    private final AtomicBoolean compactionQueued = new AtomicBoolean();

    public LSMStoreEngine(Path dataDir) {
        this(dataDir, DEFAULT_MEMTABLE_SIZE);
    }

    public LSMStoreEngine(Path dataDir, long memTableMaxSize) {
        this(dataDir, memTableMaxSize, DurabilityMode.BUFFERED);
    }

    public LSMStoreEngine(Path dataDir, long memTableMaxSize, DurabilityMode durability) {
        this(dataDir, memTableMaxSize, durability, defaultShards(durability));
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
    private static int defaultShards(DurabilityMode mode) {
        return mode == DurabilityMode.SYNC ? 1 : DEFAULT_SHARDS;
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
        this.blockCache = new BlockCache(blockCacheBytes);
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

            this.compactor = new Compactor(dataDir, blockCache, nextSequence::getAndIncrement);
            this.compactionThread = Executors.newSingleThreadExecutor(runnable -> {
                Thread thread = new Thread(runnable, "lsm-compaction");
                thread.setDaemon(true);
                return thread;
            });

            // The bound is for the store, so split it rather than handing each
            // shard the whole thing and using shardCount times the memory.
            long perShard = Math.max(1, memTableMaxSize / shardCount);
            MemTable[] recovered = new MemTable[shardCount];
            for (int i = 0; i < shardCount; i++) {
                recovered[i] = new MemTable(perShard);
                WriteAheadLog.replay(dataDir.resolve(walName(i)), recovered[i]);
            }
            recoverUnshardedLog(recovered);

            this.shards = new Shard[shardCount];
            for (int i = 0; i < shardCount; i++) {
                Path walPath = dataDir.resolve(walName(i));
                // Start a fresh log so replayed records are not replayed again,
                // then write the recovered state back into it.
                Files.deleteIfExists(walPath);
                WriteAheadLog wal = new WriteAheadLog(walPath, durability);
                for (var entry : recovered[i].entries().entrySet()) {
                    Entry e = entry.getValue();
                    if (e.isTombstone()) {
                        wal.appendDelete(entry.getKey());
                    } else {
                        wal.appendPut(entry.getKey(), e.value().orElseThrow());
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
     * Migrates a store written before the log was sharded. Its single wal.log
     * is replayed and each record routed to the shard that key now belongs to.
     */
    private void recoverUnshardedLog(MemTable[] recovered) throws IOException {
        Path legacy = dataDir.resolve("wal.log");
        if (!Files.exists(legacy)) {
            return;
        }
        MemTable all = new MemTable(Long.MAX_VALUE);
        WriteAheadLog.replay(legacy, all);
        for (var entry : all.entries().entrySet()) {
            MemTable target = recovered[shardIndex(entry.getKey(), recovered.length)];
            Entry value = entry.getValue();
            if (value.isTombstone()) {
                target.delete(entry.getKey());
            } else {
                target.put(entry.getKey(), value.value().orElseThrow());
            }
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
        checkNotClosed();
        if (fromInclusive != null && toExclusive != null
                && fromInclusive.compareTo(toExclusive) > 0) {
            return Stream.empty();
        }

        // Keys scatter across shards, so a range can start in any of them.
        // Merged into one sorted view first, which also keeps them ahead of
        // every table in the merge order.
        TreeMap<String, Entry> buffered = new TreeMap<>();
        for (Shard shard : shards) {
            buffered.putAll(shard.memTable.range(fromInclusive, toExclusive));
        }

        List<SSTableReader> held = new ArrayList<>();
        List<RowSource> sources = new ArrayList<>();
        sources.add(RowSource.of(buffered.entrySet().iterator()));

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
                        rows(merged, toExclusive), Spliterator.ORDERED | Spliterator.SORTED), false)
                // Each table stays alive until the scan finishes with it, even
                // if compaction retires it in the meantime.
                .onClose(() -> {
                    merged.close();
                    held.forEach(SSTableReader::release);
                });
    }

    /** Drops tombstones and stops at the upper bound; keys ascend, so it can stop early. */
    private Iterator<Row> rows(MergingCursor merged, String toExclusive) {
        return new Iterator<>() {
            private Row pending = advance();

            private Row advance() {
                try {
                    while (merged.hasNext()) {
                        SSTableReader.Row row = merged.next();
                        if (toExclusive != null && row.key().compareTo(toExclusive) >= 0) {
                            return null;
                        }
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
            if (isPut) {
                seq = shard.wal.appendPut(key, value);
                shard.memTable.put(key, value);
            } else {
                seq = shard.wal.appendDelete(key);
                shard.memTable.delete(key);
            }
            flushIfFull(shard);
        } catch (IOException e) {
            throw new UncheckedIOException("Write failed", e);
        } finally {
            shard.lock.unlock();
        }

        // Outside the lock, and now per shard: concurrent writers to different
        // shards commit in parallel instead of queueing behind one another.
        try {
            shard.wal.syncTo(seq);
        } catch (IOException e) {
            throw new UncheckedIOException("Write failed to commit", e);
        }
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
        SSTableWriter.write(path, shard.memTable.entries());
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

    private void compactUntilSettled() {
        compactionQueued.set(false);
        try {
            Compactor.Job job;
            while (!closed && (job = compactor.choose(tables.get())) != null) {
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
