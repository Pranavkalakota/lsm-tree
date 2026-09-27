package lsm.sstable;

import lsm.memtable.Entry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The per-table bloom filter.
 *
 * <p>A filter is an optimization, so most of these assert on a side effect
 * rather than on a return value: whether a lookup actually touched the disk.
 * The block cache makes that observable, since a block only lands in it when
 * something read it.
 */
class BloomFilterTest {

    @TempDir
    Path dir;

    private Path write(int keyCount) throws IOException {
        TreeMap<String, Entry> entries = new TreeMap<>();
        for (int i = 0; i < keyCount; i++) {
            entries.put(String.format("key_%06d", i), Entry.put("value_" + i));
        }
        Path path = dir.resolve("table.sst");
        SSTableWriter.write(path, entries);
        return path;
    }

    // --- it skips work ---

    @Test
    void anAbsentKeyIsAnsweredWithoutReadingABlock() throws IOException {
        Path path = write(2_000);
        BlockCache cache = new BlockCache(4 * 1024 * 1024);

        try (SSTableReader reader = new SSTableReader(path, cache)) {
            // Inside the table's key range, so the index would happily point at
            // a block; only the filter can rule it out without reading.
            assertNull(reader.get("key_000500_but_absent"));
            assertEquals(0, cache.blockCount(),
                    "the filter should have answered before any block was read");
        }
    }

    @Test
    void aPresentKeyStillReadsItsBlock() throws IOException {
        Path path = write(2_000);
        BlockCache cache = new BlockCache(4 * 1024 * 1024);

        try (SSTableReader reader = new SSTableReader(path, cache)) {
            assertEquals("value_500", reader.get("key_000500").value().orElseThrow());
            assertEquals(1, cache.blockCount(), "a hit has to actually read the data");
        }
    }

    @Test
    void mostAbsentKeysAvoidTheDiskEntirely() throws IOException {
        Path path = write(5_000);
        BlockCache cache = new BlockCache(64 * 1024 * 1024);

        try (SSTableReader reader = new SSTableReader(path, cache)) {
            for (int i = 0; i < 2_000; i++) {
                assertNull(reader.get(String.format("key_%06d_absent", i)));
            }
            // Every one of these is a miss, so each block in the cache is a
            // false positive. The filter is built for one percent.
            long falsePositives = cache.blockCount();
            assertTrue(falsePositives < 100,
                    "expected well under 100 block reads across 2000 misses, got "
                            + falsePositives);
        }
    }

    // --- it never lies about a key being absent ---

    @Test
    void noKeyIsEverReportedAbsentWhenItIsPresent() throws IOException {
        Path path = write(10_000);

        try (SSTableReader reader = new SSTableReader(path)) {
            for (int i = 0; i < 10_000; i++) {
                String key = String.format("key_%06d", i);
                assertTrue(reader.mightContain(key), "filter denied a stored key: " + key);
                assertNotNull(reader.get(key), "lookup lost a stored key: " + key);
            }
        }
    }

    @Test
    void anUndersizedFilterStillNeverLosesAKey() throws IOException {
        // Ten times more keys than the filter was built for. Guava degrades to
        // a higher false positive rate, which costs reads; a false negative
        // would cost data, and must not happen at any load.
        Path path = dir.resolve("undersized.sst");
        try (SSTableWriter writer = SSTableWriter.create(path, 100)) {
            for (int i = 0; i < 1_000; i++) {
                writer.add(String.format("key_%06d", i), Entry.put("value_" + i));
            }
            writer.finish();
        }

        try (SSTableReader reader = new SSTableReader(path)) {
            for (int i = 0; i < 1_000; i++) {
                assertNotNull(reader.get(String.format("key_%06d", i)), "lost key " + i);
            }
        }
    }

    @Test
    void tombstonesAreInTheFilterToo() throws IOException {
        TreeMap<String, Entry> entries = new TreeMap<>();
        entries.put("alive", Entry.put("value"));
        entries.put("deleted", Entry.tombstone());
        Path path = dir.resolve("tombstones.sst");
        SSTableWriter.write(path, entries);

        try (SSTableReader reader = new SSTableReader(path)) {
            // A tombstone the filter hid would let an older table's value show
            // through, which is a deleted key coming back to life.
            assertTrue(reader.mightContain("deleted"));
            assertTrue(reader.get("deleted").isTombstone());
        }
    }

    // --- it survives the file ---

    @Test
    void theFilterIsRebuiltFromDiskNotMemory() throws IOException {
        Path path = write(1_000);

        try (SSTableReader reader = new SSTableReader(path)) {
            assertEquals(1_000, reader.entryCount());
            assertTrue(reader.mightContain("key_000001"));
            assertFalse(reader.mightContain("nowhere_near_this_table"));
        }
    }

    @Test
    void anEmptyTableHasAFilterThatRejectsEverything() throws IOException {
        Path path = dir.resolve("empty.sst");
        SSTableWriter.write(path, new TreeMap<>());

        try (SSTableReader reader = new SSTableReader(path)) {
            assertEquals(0, reader.entryCount());
            assertFalse(reader.mightContain("anything"));
            assertNull(reader.get("anything"));
        }
    }

    @Test
    void aTruncatedFilterIsRejectedRatherThanTrusted() throws IOException {
        Path path = write(500);
        byte[] bytes = Files.readAllBytes(path);
        // Cut into the filter, which sits between the index and the footer.
        byte[] truncated = new byte[bytes.length - SSTableWriter.FOOTER_SIZE - 8];
        System.arraycopy(bytes, 0, truncated, 0, truncated.length);
        System.arraycopy(bytes, bytes.length - SSTableWriter.FOOTER_SIZE,
                truncated, truncated.length - SSTableWriter.FOOTER_SIZE,
                SSTableWriter.FOOTER_SIZE);
        Files.write(path, truncated);

        assertThrows(IOException.class, () -> new SSTableReader(path));
    }
}
