package lsm.memtable;

import java.util.Map;
import java.util.Optional;
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
            sizeBytes -= key.getBytes().length + old.sizeBytes();
        }
        Entry entry = Entry.put(value);
        data.put(key, entry);
        sizeBytes += key.getBytes().length + entry.sizeBytes();
    }

    public void delete(String key) {
        Entry old = data.get(key);
        if (old != null) {
            sizeBytes -= key.getBytes().length + old.sizeBytes();
        }
        Entry tombstone = Entry.tombstone();
        data.put(key, tombstone);
        sizeBytes += key.getBytes().length;
    }

    /**
     * Returns:
     *   - Optional containing the value if the key exists with a PUT
     *   - Optional containing empty-Entry (tombstone) if the key was deleted
     *   - null if the key is not in this MemTable at all
     *
     * The caller needs to distinguish "not here" (check older sources) from
     * "deleted" (stop looking, key is gone).
     */
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
        return data;
    }

    public void clear() {
        data.clear();
        sizeBytes = 0;
    }
}
