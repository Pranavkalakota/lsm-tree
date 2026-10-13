package lsm.sstable;

import lsm.memtable.Entry;
import lsm.memtable.VersionedKey;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/** Several versions of a key stored in one table, and read back at a version. */
class SSTableVersionTest {

    @TempDir
    Path dir;

    /** Writes the given versions of one key, newest first as the format requires. */
    private Path tableOf(String key, long... versions) throws IOException {
        Path path = dir.resolve("L0_" + System.nanoTime() + ".sst");
        try (SSTableWriter writer = SSTableWriter.create(path, versions.length)) {
            for (long version : versions) {
                writer.add(new VersionedKey(key, version), Entry.put("v" + version));
            }
            writer.finish();
        }
        return path;
    }

    @Test
    void readsBackTheNewestVersion() throws IOException {
        try (SSTableReader reader = new SSTableReader(tableOf("k", 30, 20, 10))) {
            assertEquals("v30", reader.get("k").value().orElseThrow());
        }
    }

    @Test
    void readsBackAVersionAtASnapshot() throws IOException {
        try (SSTableReader reader = new SSTableReader(tableOf("k", 30, 20, 10))) {
            assertEquals("v10", reader.get("k", 10).value().orElseThrow());
            assertEquals("v10", reader.get("k", 19).value().orElseThrow());
            assertEquals("v20", reader.get("k", 20).value().orElseThrow());
            assertEquals("v30", reader.get("k", 500).value().orElseThrow());
        }
    }

    @Test
    void aSnapshotOlderThanTheKeyFindsNothing() throws IOException {
        try (SSTableReader reader = new SSTableReader(tableOf("k", 30, 20, 10))) {
            assertNull(reader.get("k", 9));
            assertNull(reader.get("k", 0));
        }
    }

    @Test
    void aSnapshotDoesNotAnswerWithTheNextKey() throws IOException {
        Path path = dir.resolve("L0_000001.sst");
        try (SSTableWriter writer = SSTableWriter.create(path, 2)) {
            writer.add(new VersionedKey("a", 50), Entry.put("late"));
            writer.add(new VersionedKey("b", 1), Entry.put("early"));
            writer.finish();
        }
        try (SSTableReader reader = new SSTableReader(path)) {
            assertNull(reader.get("a", 1), "\"a\" had no version this old");
            assertEquals("early", reader.get("b", 1).value().orElseThrow());
        }
    }

    @Test
    void tombstoneAtAVersionHidesOnlyLaterReads() throws IOException {
        Path path = dir.resolve("L0_000002.sst");
        try (SSTableWriter writer = SSTableWriter.create(path, 2)) {
            writer.add(new VersionedKey("k", 9), Entry.tombstone());
            writer.add(new VersionedKey("k", 4), Entry.put("alive"));
            writer.finish();
        }
        try (SSTableReader reader = new SSTableReader(path)) {
            assertTrue(reader.get("k").isTombstone());
            assertEquals("alive", reader.get("k", 8).value().orElseThrow());
        }
    }

    @Test
    void metadataRecordsTheHighestVersion() throws IOException {
        try (SSTableReader reader = new SSTableReader(tableOf("k", 77, 20, 10))) {
            assertEquals(77, reader.maxVersion());
        }
    }

    @Test
    void anUnversionedTableReportsVersionZero() throws IOException {
        Path path = dir.resolve("L0_000003.sst");
        try (SSTableWriter writer = SSTableWriter.create(path, 1)) {
            writer.add("k", Entry.put("v"));
            writer.finish();
        }
        try (SSTableReader reader = new SSTableReader(path)) {
            assertEquals(0, reader.maxVersion());
            assertEquals("v", reader.get("k").value().orElseThrow());
        }
    }

    @Test
    void versionsMustArriveNewestFirst() throws IOException {
        Path path = dir.resolve("L0_000004.sst");
        try (SSTableWriter writer = SSTableWriter.create(path, 2)) {
            writer.add(new VersionedKey("k", 5), Entry.put("a"));
            // Ascending version is backwards in the stored order, so it is
            // refused rather than written where no lookup would find it.
            assertThrows(IOException.class,
                    () -> writer.add(new VersionedKey("k", 6), Entry.put("b")));
            assertThrows(IOException.class,
                    () -> writer.add(new VersionedKey("k", 5), Entry.put("b")));
        }
    }

    @Test
    void cursorWalksEveryVersionNewestFirst() throws IOException {
        try (SSTableReader reader = new SSTableReader(tableOf("k", 30, 20, 10))) {
            List<Long> seen = new ArrayList<>();
            SSTableReader.Cursor cursor = reader.cursor();
            while (cursor.hasNext()) {
                seen.add(cursor.next().version());
            }
            assertEquals(List.of(30L, 20L, 10L), seen);
        }
    }

    @Test
    void cursorFromStartsAtTheNewestVersionOfItsKey() throws IOException {
        Path path = dir.resolve("L0_000005.sst");
        try (SSTableWriter writer = SSTableWriter.create(path, 4)) {
            writer.add(new VersionedKey("a", 1), Entry.put("a1"));
            writer.add(new VersionedKey("b", 9), Entry.put("b9"));
            writer.add(new VersionedKey("b", 2), Entry.put("b2"));
            writer.finish();
        }
        try (SSTableReader reader = new SSTableReader(path)) {
            SSTableReader.Cursor cursor = reader.cursorFrom("b");
            assertEquals(9, cursor.current().version());
            assertEquals("b", cursor.current().key());
        }
    }

    @Test
    void theBloomFilterStillWorksOnKeysAlone() throws IOException {
        try (SSTableReader reader = new SSTableReader(tableOf("k", 30, 20, 10))) {
            assertTrue(reader.mightContain("k"));
            assertNull(reader.get("absent"));
        }
    }

    @Test
    void rangeMetadataIgnoresVersions() throws IOException {
        Path path = dir.resolve("L0_000006.sst");
        try (SSTableWriter writer = SSTableWriter.create(path, 4)) {
            writer.add(new VersionedKey("a", 9), Entry.put("1"));
            writer.add(new VersionedKey("a", 3), Entry.put("2"));
            writer.add(new VersionedKey("z", 7), Entry.put("3"));
            writer.finish();
        }
        try (SSTableReader reader = new SSTableReader(path)) {
            assertEquals("a", reader.minKey());
            assertEquals("z", reader.maxKey());
            assertEquals(3, reader.entryCount());
        }
    }

    /**
     * A key with more history than fits in one block, which is the case the
     * index has to get right: a block boundary lands in the middle of the key's
     * versions, so the block whose first record is at or before the seek is not
     * always the block the answer lives in.
     */
    @Test
    void findsVersionsThatStraddleABlockBoundary() throws IOException {
        String padding = "x".repeat(400);
        Path path = dir.resolve("L0_000007.sst");
        try (SSTableWriter writer = SSTableWriter.create(path, 200)) {
            for (long version = 200; version >= 1; version--) {
                writer.add(new VersionedKey("hot", version), Entry.put(version + padding));
            }
            writer.finish();
        }

        try (SSTableReader reader = new SSTableReader(path)) {
            assertTrue(reader.blockCount() > 1,
                    "the point of this test is a key spanning blocks");
            assertEquals(200 + padding, reader.get("hot").value().orElseThrow());
            for (long snapshot = 1; snapshot <= 200; snapshot++) {
                assertEquals(snapshot + padding,
                        reader.get("hot", snapshot).value().orElseThrow(),
                        "snapshot " + snapshot);
            }
            assertNull(reader.get("hot", 0));
        }
    }

    /** The same spanning case, but with keys either side of the long history. */
    @Test
    void neighboursOfASpanningKeyStayReachable() throws IOException {
        String padding = "y".repeat(400);
        Path path = dir.resolve("L0_000008.sst");
        try (SSTableWriter writer = SSTableWriter.create(path, 200)) {
            writer.add(new VersionedKey("aaa", 5), Entry.put("first"));
            for (long version = 150; version >= 1; version--) {
                writer.add(new VersionedKey("mmm", version), Entry.put(version + padding));
            }
            writer.add(new VersionedKey("zzz", 5), Entry.put("last"));
            writer.finish();
        }

        try (SSTableReader reader = new SSTableReader(path)) {
            assertEquals("first", reader.get("aaa").value().orElseThrow());
            assertEquals("last", reader.get("zzz").value().orElseThrow());
            assertEquals("first", reader.get("aaa", 5).value().orElseThrow());
            assertEquals("last", reader.get("zzz", 9).value().orElseThrow());
            assertEquals(75 + padding, reader.get("mmm", 75).value().orElseThrow());
        }
    }

    /**
     * A generated table of many keys with random histories, read at every
     * version against a model. Checks the seek against the one thing it has to
     * agree with: what was written.
     */
    @ParameterizedTest(name = "seed={0}")
    @MethodSource("seeds")
    void everySnapshotReadsWhatWasWritten(long seed) throws IOException {
        Random random = new Random(seed);
        // key -> version -> value, or null for a tombstone
        TreeMap<String, TreeMap<Long, String>> model = new TreeMap<>();
        long version = 0;
        for (int i = 0; i < 25; i++) {
            String key = String.format("key%02d", random.nextInt(12));
            int history = 1 + random.nextInt(4);
            for (int h = 0; h < history; h++) {
                version++;
                boolean tombstone = random.nextInt(5) == 0;
                model.computeIfAbsent(key, k -> new TreeMap<>())
                        .put(version, tombstone ? null : "v" + version);
            }
        }

        Path path = dir.resolve("L0_" + seed + ".sst");
        try (SSTableWriter writer = SSTableWriter.create(path, 200)) {
            for (var entry : model.entrySet()) {
                // Newest first within the key, which is the stored order.
                for (Long at : entry.getValue().descendingKeySet()) {
                    String value = entry.getValue().get(at);
                    writer.add(new VersionedKey(entry.getKey(), at),
                            value == null ? Entry.tombstone() : Entry.put(value));
                }
            }
            writer.finish();
        }

        try (SSTableReader reader = new SSTableReader(path)) {
            assertEquals(version, reader.maxVersion());
            for (String key : model.keySet()) {
                TreeMap<Long, String> history = model.get(key);
                for (long snapshot = 0; snapshot <= version; snapshot++) {
                    Long visible = history.floorKey(snapshot);
                    Entry found = reader.get(key, snapshot);
                    if (visible == null) {
                        assertNull(found, key + " at " + snapshot);
                        continue;
                    }
                    assertNotNull(found, key + " at " + snapshot);
                    String expected = history.get(visible);
                    if (expected == null) {
                        assertTrue(found.isTombstone(), key + " at " + snapshot);
                    } else {
                        assertEquals(expected, found.value().orElseThrow(),
                                key + " at " + snapshot);
                    }
                }
            }
        }
    }

    private static Stream<Long> seeds() {
        return IntStream.range(0, 15).mapToObj(i -> 7700L + i);
    }
}
