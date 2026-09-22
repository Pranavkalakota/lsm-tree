package lsm;

import lsm.compaction.Compactor;
import lsm.memtable.Entry;
import lsm.memtable.MemTable;
import lsm.sstable.BlockCache;
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
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

public class LSMStoreEngine implements StorageEngine {

    private static final long DEFAULT_MEMTABLE_SIZE = 4 * 1024 * 1024; // 4 MB
    private static final long DEFAULT_BLOCK_CACHE_SIZE = 16 * 1024 * 1024; // 16 MB
    private static final String TABLE_SUFFIX = ".sst";

    private final MemTable memTable;
    private final WriteAheadLog wal;
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
    private final BlockCache blockCache = new BlockCache(DEFAULT_BLOCK_CACHE_SIZE);

    /**
     * Serializes writers. The log and the MemTable have to agree on the order
     * writes happened, so appending and inserting cannot be interleaved by two
     * threads. Readers never take this, and neither does the slow part of a
     * compaction.
     */
    private final ReentrantLock writeLock = new ReentrantLock();

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

            this.memTable = new MemTable(memTableMaxSize);
            Path walPath = dataDir.resolve("wal.log");

            WriteAheadLog.replay(walPath, memTable);

            // After successful replay, start a fresh WAL so we don't
            // re-replay stale entries on the next startup
            Files.deleteIfExists(walPath);
            this.wal = new WriteAheadLog(walPath, durability);

            // Re-write current MemTable state into the fresh WAL so
            // crash recovery still works
            for (var entry : memTable.entries().entrySet()) {
                Entry e = entry.getValue();
                if (e.isTombstone()) {
                    wal.appendDelete(entry.getKey());
                } else {
                    wal.appendPut(entry.getKey(), e.value().orElseThrow());
                }
            }

            scheduleCompaction();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to initialize storage engine", e);
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

        Entry entry = memTable.get(key);
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
        long seq;
        writeLock.lock();
        try {
            checkNotClosed();
            if (isPut) {
                seq = wal.appendPut(key, value);
                memTable.put(key, value);
            } else {
                seq = wal.appendDelete(key);
                memTable.delete(key);
            }
            flushIfFull();
        } catch (IOException e) {
            throw new UncheckedIOException("Write failed", e);
        } finally {
            writeLock.unlock();
        }

        try {
            wal.syncTo(seq);
        } catch (IOException e) {
            throw new UncheckedIOException("Write failed to commit", e);
        }
    }

    @Override
    public void close() {
        writeLock.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
        } finally {
            writeLock.unlock();
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
            wal.close();
            tables.get().forEach(SSTableReader::close);
            lock.release();
            lockChannel.close();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to close engine", e);
        }
    }

    private void flushIfFull() throws IOException {
        if (memTable.shouldFlush()) {
            flush();
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
    private void flush() throws IOException {
        if (memTable.isEmpty()) {
            return;
        }
        Path path = dataDir.resolve(
                String.format("L0_%06d%s", nextSequence.getAndIncrement(), TABLE_SUFFIX));
        SSTableWriter.write(path, memTable.entries());
        syncDirectory();

        install(List.of(new SSTableReader(path, blockCache)), List.of());
        memTable.clear();
        wal.reset();

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
