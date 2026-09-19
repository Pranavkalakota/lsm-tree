package lsm.memtable;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

public class MemTable {

    private final TreeMap<String, Entry> data = new TreeMap<>();
    private long sizeBytes = 0;
    private final long maxSizeBytes;

    public MemTable(long maxSizeBytes) {
        this.maxSizeBytes = maxSizeBytes;
    }

    public void put(String key, String value) {
        Entry old = data.get(key);
        if (old != null) {
            sizeBytes -= keySize(key) + old.sizeBytes();
        }
        Entry entry = Entry.put(value);
        data.put(key, entry);
        sizeBytes += keySize(key) + entry.sizeBytes();
    }

    public void delete(String key) {
        Entry old = data.get(key);
        if (old != null) {
            sizeBytes -= keySize(key) + old.sizeBytes();
        }
        Entry tombstone = Entry.tombstone();
        data.put(key, tombstone);
        sizeBytes += keySize(key);
    }

    public Entry get(String key) {
        return data.get(key);
    }

    public boolean shouldFlush() {
        return sizeBytes >= maxSizeBytes;
    }

    public long getSizeBytes() {
        return sizeBytes;
    }

    public int entryCount() {
        return data.size();
    }

    public boolean isEmpty() {
        return data.isEmpty();
    }

    public Map<String, Entry> entries() {
        return Collections.unmodifiableMap(data);
    }

    public void clear() {
        data.clear();
        sizeBytes = 0;
    }

    private static int keySize(String key) {
        return key.getBytes(StandardCharsets.UTF_8).length;
    }
}
