package lsm.memtable;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NavigableMap;
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
 *
 * <p>Writes are keyed by {@link VersionedKey}, so a key holds every version
 * written to it rather than only the latest. An overwrite therefore adds a
 * record instead of replacing one, and the size bound counts all of them: the
 * same workload fills this MemTable sooner than a single-version one would and
 * flushes more often. That is the standing cost of being able to read at a
 * past version, and it is bounded, because a flush moves the history out to a
 * table and compaction is what finally collapses it.
 */
public class MemTable {

    private final ConcurrentSkipListMap<VersionedKey, Entry> data = new ConcurrentSkipListMap<>();
    private final AtomicLong sizeBytes = new AtomicLong();
    private final long maxSizeBytes;

    public MemTable(long maxSizeBytes) {
        this.maxSizeBytes = maxSizeBytes;
    }

    /** Writes a value at a version. Versions are the engine's to hand out. */
    public void put(String key, String value, long version) {
        Entry entry = Entry.put(value);
        Entry previous = data.put(new VersionedKey(key, version), entry);
        sizeBytes.addAndGet(previous == null
                ? keySize(key) + entry.sizeBytes()
                : entry.sizeBytes() - previous.sizeBytes());
    }

    /** Marks a key deleted at a version. */
    public void delete(String key, long version) {
        Entry previous = data.put(new VersionedKey(key, version), Entry.tombstone());
        sizeBytes.addAndGet(previous == null ? keySize(key) : -previous.sizeBytes());
    }

    /**
     * Writes at version zero, for callers with no version of their own: a
     * MemTable used on its own, and recovery of a log written before versions
     * existed. Repeated writes to one key overwrite rather than accumulate,
     * which is the behaviour those callers already expect.
     */
    public void put(String key, String value) {
        put(key, value, 0);
    }

    /** Deletes at version zero. See {@link #put(String, String)}. */
    public void delete(String key) {
        delete(key, 0);
    }

    /** The current value of a key, or null if it was never written. */
    public Entry get(String key) {
        return get(key, VersionedKey.NEWEST);
    }

    /**
     * The value a reader at {@code snapshot} should see: the newest version of
     * the key written at or before it, or null if the key had no value yet.
     *
     * <p>One seek answers this. Versions of a key descend, so the first record
     * at or after {@code (key, snapshot)} is already the newest visible one,
     * and anything newer than the snapshot sorts before the seek position and
     * is skipped without being looked at.
     */
    public Entry get(String key, long snapshot) {
        Map.Entry<VersionedKey, Entry> found =
                data.ceilingEntry(new VersionedKey(key, snapshot));
        // The seek runs past the key's own history when it had no version this
        // old, landing on whatever key comes next.
        return found != null && found.getKey().hasKey(key) ? found.getValue() : null;
    }

    public boolean shouldFlush() {
        return sizeBytes.get() >= maxSizeBytes;
    }

    public long getSizeBytes() {
        return sizeBytes.get();
    }

    /** Distinct keys held, counting a key with several versions once. */
    public int entryCount() {
        return entries().size();
    }

    /** Records held, counting every version separately. */
    public int versionCount() {
        return data.size();
    }

    public boolean isEmpty() {
        return data.isEmpty();
    }

    /**
     * The current value of each key in a range, ascending. Null bounds are open.
     */
    public Map<String, Entry> range(String fromInclusive, String toExclusive) {
        return range(fromInclusive, toExclusive, VersionedKey.NEWEST);
    }

    /**
     * What a reader at {@code snapshot} sees in a key range, ascending.
     *
     * <p>Unlike the older single-version form this is a copy rather than a view
     * of the live map, because collapsing each key's history down to one
     * version is work that has to happen somewhere. A caller wanting a stable
     * range wanted a copy anyway.
     */
    public Map<String, Entry> range(String fromInclusive, String toExclusive, long snapshot) {
        Map<String, Entry> visible = new LinkedHashMap<>();
        for (Map.Entry<VersionedKey, Entry> entry : bounded(fromInclusive, toExclusive).entrySet()) {
            if (entry.getKey().version() > snapshot) {
                continue;   // written after this reader started; not its business
            }
            // Versions descend, so the first one that is visible is the newest
            // that is, and later ones for the same key are its older history.
            visible.putIfAbsent(entry.getKey().key(), entry.getValue());
        }
        return Collections.unmodifiableMap(visible);
    }

    /** The current value of every key, ascending; what a flush writes out. */
    public Map<String, Entry> entries() {
        return range(null, null);
    }

    public void clear() {
        data.clear();
        sizeBytes.set(0);
    }

    /**
     * Narrows the map to a key range. A bound is placed at the newest position
     * of its key, which sorts ahead of every version of that key: so the lower
     * bound admits all of its key's versions and the exclusive upper bound
     * excludes all of its key's versions.
     */
    private NavigableMap<VersionedKey, Entry> bounded(String fromInclusive, String toExclusive) {
        if (fromInclusive == null && toExclusive == null) {
            return data;
        }
        if (fromInclusive == null) {
            return data.headMap(VersionedKey.newest(toExclusive), false);
        }
        if (toExclusive == null) {
            return data.tailMap(VersionedKey.newest(fromInclusive), true);
        }
        if (fromInclusive.compareTo(toExclusive) >= 0) {
            return Collections.emptyNavigableMap();
        }
        return data.subMap(VersionedKey.newest(fromInclusive), true,
                VersionedKey.newest(toExclusive), false);
    }

    private static int keySize(String key) {
        return key.getBytes(StandardCharsets.UTF_8).length;
    }
}
