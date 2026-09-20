package lsm.sstable;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Holds recently read SSTable blocks in memory, keyed by table and offset.
 *
 * <p>Tables are immutable once written, so a block's bytes can never go stale:
 * the only reason an entry leaves the cache is eviction. That is what makes
 * caching safe here without any invalidation protocol. Blocks belonging to a
 * table that compaction has deleted simply age out unused.
 *
 * <p>One cache is shared by every reader in a store, so the memory bound is a
 * property of the store rather than of how many tables happen to be open.
 * Instances are safe to use from multiple threads.
 */
public final class BlockCache {

    /** A loader that reads and verifies a block that was not resident. */
    @FunctionalInterface
    public interface BlockLoader {
        byte[] load() throws IOException;
    }

    private final Cache<Key, byte[]> cache;

    public BlockCache(long maxBytes) {
        this.cache = Caffeine.newBuilder()
                .maximumWeight(maxBytes)
                .weigher((Key key, byte[] block) -> block.length)
                .build();
    }

    /**
     * Returns the cached block, loading it through {@code loader} on a miss.
     *
     * <p>The returned array is shared with the cache and with any other caller
     * holding the same block, so callers must treat it as read only.
     */
    public byte[] get(Path table, long offset, BlockLoader loader) throws IOException {
        Key key = new Key(table, offset);
        byte[] cached = cache.getIfPresent(key);
        if (cached != null) {
            return cached;
        }
        // Deliberately outside the cache's own loading path: a corrupt block
        // must surface its IOException to this caller rather than being
        // wrapped, and a failed load must not be recorded as an entry.
        byte[] loaded = loader.load();
        cache.put(key, loaded);
        return loaded;
    }

    /** Approximate number of resident blocks; intended for tests and diagnostics. */
    public long blockCount() {
        return cache.estimatedSize();
    }

    private record Key(Path table, long offset) {
    }
}
