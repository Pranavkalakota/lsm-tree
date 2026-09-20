package lsm.memtable;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The in-memory half of the store: recent writes, held in key order so a flush
 * can stream them straight out as a sorted table.
 *
 * <p>Backed by a skip list rather than a balanced tree so that lookups stay
 * correct while another thread is writing. Reads need no coordination at all;
 * writers are expected to be serialized by the engine, which they must be
 * anyway to keep the write-ahead log in the same order as this map.
 */
public class MemTable {

    private final ConcurrentSkipListMap<String, Entry> data = new ConcurrentSkipListMap<>();
    private final AtomicLong sizeBytes = new AtomicLong();
    private final long maxSizeBytes;

    public MemTable(long maxSizeBytes) {
        this.maxSizeBytes = maxSizeBytes;
    }

    public void put(String key, String value) {
        Entry entry = Entry.put(value);
        Entry previous = data.put(key, entry);
        sizeBytes.addAndGet(previous == null
                ? keySize(key) + entry.sizeBytes()
                : entry.sizeBytes() - previous.sizeBytes());
    }

    public void delete(String key) {
        Entry previous = data.put(key, Entry.tombstone());
        sizeBytes.addAndGet(previous == null ? keySize(key) : -previous.sizeBytes());
    }

    public Entry get(String key) {
        return data.get(key);
    }

    public boolean shouldFlush() {
        return sizeBytes.get() >= maxSizeBytes;
    }

    public long getSizeBytes() {
        return sizeBytes.get();
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
        sizeBytes.set(0);
    }

    private static int keySize(String key) {
        return key.getBytes(StandardCharsets.UTF_8).length;
    }
}
