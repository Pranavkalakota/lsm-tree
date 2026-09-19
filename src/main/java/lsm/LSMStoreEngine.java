package lsm;

import lsm.memtable.Entry;
import lsm.memtable.MemTable;

import java.util.Optional;

public class LSMStoreEngine implements StorageEngine {

    private static final long DEFAULT_MEMTABLE_SIZE = 4 * 1024 * 1024; // 4 MB

    private final MemTable memTable;

    public LSMStoreEngine() {
        this(DEFAULT_MEMTABLE_SIZE);
    }

    public LSMStoreEngine(long memTableMaxSize) {
        this.memTable = new MemTable(memTableMaxSize);
    }

    @Override
    public void put(String key, String value) {
        if (key == null) throw new IllegalArgumentException("key must not be null");
        if (value == null) throw new IllegalArgumentException("value must not be null");
        memTable.put(key, value);
    }

    @Override
    public Optional<String> get(String key) {
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
        if (key == null) throw new IllegalArgumentException("key must not be null");
        memTable.delete(key);
    }

    @Override
    public void close() {
        // Phase 1: nothing to flush yet — no persistence layer
    }
}
