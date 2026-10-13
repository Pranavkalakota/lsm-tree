package lsm.memtable;

/**
 * A key paired with the version of the write that produced it.
 *
 * <p>Until now a key had exactly one value anywhere in the store and a newer
 * write replaced the older one outright. Keeping several versions of a key
 * instead is what lets a reader hold a fixed point in time: it reads at a
 * version, and sees the newest write that happened at or before it, however
 * much the store has moved on since.
 *
 * <p>The ordering is the useful part. Keys ascend, which every sorted structure
 * here already relies on, but versions within a key <em>descend</em>, so a
 * key's versions sit newest first. That turns the question a snapshot read
 * asks — "the newest version of this key not after mine" — into a single seek
 * to {@code (key, version)}, because the first record at or after that point is
 * exactly the answer. Ordering versions the other way round would make the same
 * lookup walk the whole history of the key.
 *
 * <p>{@link #NEWEST} sorts ahead of every real version of its key, so a seek
 * with it lands on the current value. That works because versions start at zero
 * and count up, which also means they are comparable across shards: the engine
 * hands them out from one counter.
 */
public record VersionedKey(String key, long version) implements Comparable<VersionedKey> {

    /**
     * A version above any the store will issue, used to seek the current value
     * rather than a historical one.
     */
    public static final long NEWEST = Long.MAX_VALUE;

    public VersionedKey {
        if (key == null) {
            throw new IllegalArgumentException("key must not be null");
        }
        if (version < 0) {
            throw new IllegalArgumentException("version must not be negative: " + version);
        }
    }

    /** A seek position landing on the newest version of {@code key}. */
    public static VersionedKey newest(String key) {
        return new VersionedKey(key, NEWEST);
    }

    /** Whether this holds the given key, whatever version it carries. */
    public boolean hasKey(String other) {
        return key.equals(other);
    }

    @Override
    public int compareTo(VersionedKey other) {
        int byKey = key.compareTo(other.key);
        if (byKey != 0) {
            return byKey;
        }
        // Reversed on purpose: newest version of a key comes first.
        return Long.compare(other.version, version);
    }

    @Override
    public String toString() {
        return key + "@" + (version == NEWEST ? "newest" : version);
    }
}
