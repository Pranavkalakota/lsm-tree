package lsm;

import lsm.memtable.Entry;
import lsm.memtable.MemTable;
import lsm.sstable.BlockCache;
import lsm.sstable.SSTableReader;
import lsm.sstable.SSTableWriter;
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
import java.util.concurrent.CopyOnWriteArrayList;
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
     * Flushed tables, newest first, so a lookup can stop at the first table
     * that mentions the key. Copy-on-write because reads walk this list while
     * a flush prepends to it.
     */
    private final List<SSTableReader> tables = new CopyOnWriteArrayList<>();

    /** Shared by every reader, so the memory bound belongs to the store. */
    private final BlockCache blockCache = new BlockCache(DEFAULT_BLOCK_CACHE_SIZE);

    private long nextSequence = 0;

    public LSMStoreEngine(Path dataDir) {
        this(dataDir, DEFAULT_MEMTABLE_SIZE);
    }

    public LSMStoreEngine(Path dataDir, long memTableMaxSize) {
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

            this.memTable = new MemTable(memTableMaxSize);
            Path walPath = dataDir.resolve("wal.log");

            WriteAheadLog.replay(walPath, memTable);

            // After successful replay, start a fresh WAL so we don't
            // re-replay stale entries on the next startup
            Files.deleteIfExists(walPath);
            this.wal = new WriteAheadLog(walPath);

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
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to initialize storage engine", e);
        }
    }

    @Override
    public void put(String key, String value) {
        checkNotClosed();
        if (key == null) throw new IllegalArgumentException("key must not be null");
        if (value == null) throw new IllegalArgumentException("value must not be null");
        try {
            wal.appendPut(key, value);
            memTable.put(key, value);
            flushIfFull();
        } catch (IOException e) {
            throw new UncheckedIOException("Write failed", e);
        }
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
        try {
            for (SSTableReader table : tables) {
                Entry found = table.get(key);
                if (found != null) {
                    return found.isTombstone() ? Optional.empty() : found.value();
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Read failed", e);
        }

        return Optional.empty();
    }

    @Override
    public void delete(String key) {
        checkNotClosed();
        if (key == null) throw new IllegalArgumentException("key must not be null");
        try {
            wal.appendDelete(key);
            memTable.delete(key);
            flushIfFull();
        } catch (IOException e) {
            throw new UncheckedIOException("Write failed", e);
        }
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        try {
            // The MemTable is deliberately not flushed here. Every write is
            // already in the fsynced WAL, so replay restores it on the next
            // open; flushing instead would litter the directory with a tiny
            // table each time a process opens and closes the store.
            wal.close();
            for (SSTableReader table : tables) {
                table.close();
            }
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
     * Writes the MemTable out as a new table and starts a fresh WAL.
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
        Path path = dataDir.resolve(String.format("L0_%06d%s", nextSequence, TABLE_SUFFIX));
        SSTableWriter.write(path, memTable.entries());
        syncDirectory();

        tables.add(0, new SSTableReader(path, blockCache));
        nextSequence++;
        memTable.clear();
        wal.reset();
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
                // Half-written table left by a flush that crashed. Its contents
                // are still in the WAL, so dropping it loses nothing.
                Files.deleteIfExists(path);
            } else if (name.endsWith(TABLE_SUFFIX)) {
                found.add(path);
            }
        }

        found.sort(Comparator.comparingLong(LSMStoreEngine::sequenceOf).reversed());
        for (Path path : found) {
            tables.add(new SSTableReader(path, blockCache));
            nextSequence = Math.max(nextSequence, sequenceOf(path) + 1);
        }
    }

    /** Parses the sequence out of a {@code L<level>_<sequence>.sst} file name. */
    private static long sequenceOf(Path path) {
        String name = path.getFileName().toString();
        int underscore = name.indexOf('_');
        try {
            return Long.parseLong(name.substring(underscore + 1,
                    name.length() - TABLE_SUFFIX.length()));
        } catch (RuntimeException e) {
            throw new IllegalStateException("Unrecognised table file: " + path, e);
        }
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
