package lsm;

import lsm.memtable.Entry;
import lsm.memtable.MemTable;
import lsm.wal.WriteAheadLog;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Optional;

public class LSMStoreEngine implements StorageEngine {

    private static final long DEFAULT_MEMTABLE_SIZE = 4 * 1024 * 1024; // 4 MB

    private final MemTable memTable;
    private final WriteAheadLog wal;
    private final Path dataDir;
    private final FileChannel lockChannel;
    private final FileLock lock;
    private volatile boolean closed = false;

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
        } catch (IOException e) {
            throw new UncheckedIOException("WAL write failed", e);
        }
        memTable.put(key, value);
    }

    @Override
    public Optional<String> get(String key) {
        checkNotClosed();
        if (key == null) throw new IllegalArgumentException("key must not be null");

        Entry entry = memTable.get(key);
        if (entry != null) {
            if (entry.isTombstone()) {
                return Optional.empty();
            }
            return entry.value();
        }

        return Optional.empty();
    }

    @Override
    public void delete(String key) {
        checkNotClosed();
        if (key == null) throw new IllegalArgumentException("key must not be null");
        try {
            wal.appendDelete(key);
        } catch (IOException e) {
            throw new UncheckedIOException("WAL write failed", e);
        }
        memTable.delete(key);
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        try {
            wal.close();
            lock.release();
            lockChannel.close();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to close engine", e);
        }
    }

    private void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException("Storage engine is closed");
        }
    }
}
