package lsm;

import lsm.wal.DurabilityMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.TreeMap;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Reading at a snapshot while the store carries on changing underneath.
 */
class SnapshotTest {

    @TempDir
    Path dir;

    private LSMStoreEngine open() {
        return open(4 * 1024 * 1024, 1);
    }

    private LSMStoreEngine open(long memTableBytes, int shards) {
        return new LSMStoreEngine(dir, memTableBytes, DurabilityMode.BUFFERED, shards);
    }

    private Map<String, String> scanAll(LSMStoreEngine engine, Snapshot at) {
        TreeMap<String, String> rows = new TreeMap<>();
        try (Stream<StorageEngine.Row> stream = engine.scan(null, null, at)) {
            stream.forEach(row -> rows.put(row.key(), row.value()));
        }
        return rows;
    }

    private Map<String, String> scanAll(LSMStoreEngine engine) {
        TreeMap<String, String> rows = new TreeMap<>();
        try (Stream<StorageEngine.Row> stream = engine.scan(null, null)) {
            stream.forEach(row -> rows.put(row.key(), row.value()));
        }
        return rows;
    }

    @Test
    void aSnapshotDoesNotSeeLaterOverwrites() {
        LSMStoreEngine engine = open();
        try {
            engine.put("k", "before");
            try (Snapshot at = engine.snapshot()) {
                engine.put("k", "after");

                assertEquals("before", engine.get("k", at).orElseThrow());
                assertEquals("after", engine.get("k").orElseThrow());
            }
        } finally {
            engine.close();
        }
    }

    @Test
    void aSnapshotDoesNotSeeLaterDeletes() {
        LSMStoreEngine engine = open();
        try {
            engine.put("k", "alive");
            try (Snapshot at = engine.snapshot()) {
                engine.delete("k");

                assertEquals("alive", engine.get("k", at).orElseThrow());
                assertTrue(engine.get("k").isEmpty());
            }
        } finally {
            engine.close();
        }
    }

    @Test
    void aSnapshotDoesNotSeeKeysAddedAfterIt() {
        LSMStoreEngine engine = open();
        try {
            engine.put("old", "1");
            try (Snapshot at = engine.snapshot()) {
                engine.put("new", "2");

                assertTrue(engine.get("new", at).isEmpty());
                assertEquals("1", engine.get("old", at).orElseThrow());
                assertEquals("2", engine.get("new").orElseThrow());
            }
        } finally {
            engine.close();
        }
    }

    @Test
    void aSnapshotSeesADeleteThatHappenedBeforeIt() {
        LSMStoreEngine engine = open();
        try {
            engine.put("k", "v");
            engine.delete("k");
            try (Snapshot at = engine.snapshot()) {
                engine.put("k", "back");
                assertTrue(engine.get("k", at).isEmpty(),
                        "the delete was already committed when the snapshot opened");
                assertEquals("back", engine.get("k").orElseThrow());
            }
        } finally {
            engine.close();
        }
    }

    @Test
    void twoSnapshotsSeeTheirOwnMoments() {
        LSMStoreEngine engine = open();
        try {
            engine.put("k", "one");
            Snapshot first = engine.snapshot();
            engine.put("k", "two");
            Snapshot second = engine.snapshot();
            engine.put("k", "three");

            try (first; second) {
                assertEquals("one", engine.get("k", first).orElseThrow());
                assertEquals("two", engine.get("k", second).orElseThrow());
                assertEquals("three", engine.get("k").orElseThrow());
            }
        } finally {
            engine.close();
        }
    }

    @Test
    void aScanAtASnapshotIgnoresLaterWrites() {
        LSMStoreEngine engine = open();
        try {
            engine.put("a", "1");
            engine.put("b", "2");
            try (Snapshot at = engine.snapshot()) {
                engine.put("c", "3");
                engine.put("a", "changed");
                engine.delete("b");

                assertEquals(Map.of("a", "1", "b", "2"), scanAll(engine, at));
                assertEquals(Map.of("a", "changed", "c", "3"), scanAll(engine));
            }
        } finally {
            engine.close();
        }
    }

    /**
     * The ordering trap in the scan: a version the reader cannot see must not
     * be the record that settles a key, or the visible version underneath it
     * is skipped as stale history and the key vanishes from the scan.
     */
    @Test
    void aScanAtASnapshotStillReturnsAKeyOverwrittenSince() {
        LSMStoreEngine engine = open();
        try {
            engine.put("k", "visible");
            try (Snapshot at = engine.snapshot()) {
                engine.put("k", "invisible");
                assertEquals(Map.of("k", "visible"), scanAll(engine, at));
            }
        } finally {
            engine.close();
        }
    }

    @Test
    void aScanAtASnapshotStillReturnsAKeyDeletedSince() {
        LSMStoreEngine engine = open();
        try {
            engine.put("k", "visible");
            try (Snapshot at = engine.snapshot()) {
                engine.delete("k");
                assertEquals(Map.of("k", "visible"), scanAll(engine, at));
            }
        } finally {
            engine.close();
        }
    }

    @Test
    void aSnapshotSurvivesTheWritesBeingFlushedToDisk() {
        // A tiny MemTable, so the snapshot's version is pushed out to a table
        // while it is still open and has to be read back from there.
        LSMStoreEngine engine = open(64, 1);
        try {
            engine.put("k", "before");
            try (Snapshot at = engine.snapshot()) {
                for (int i = 0; i < 200; i++) {
                    engine.put("filler" + i, "v".repeat(20));
                }
                engine.put("k", "after");

                assertEquals("before", engine.get("k", at).orElseThrow());
                assertEquals("after", engine.get("k").orElseThrow());
            }
        } finally {
            engine.close();
        }
    }

    @Test
    void aSnapshotHoldsBackCompactionThatWouldDiscardItsHistory() {
        LSMStoreEngine engine = open(64, 1);
        try {
            engine.put("k", "before");
            try (Snapshot at = engine.snapshot()) {
                // Enough writing to trigger flushes and compaction, which is
                // exactly what would drop the old version if it were allowed.
                for (int i = 0; i < 400; i++) {
                    engine.put("k", "churn" + i);
                }
                engine.compactIdleNow();

                assertEquals("before", engine.get("k", at).orElseThrow(),
                        "compaction discarded a version a live snapshot needed");
            }
        } finally {
            engine.close();
        }
    }

    @Test
    void closingASnapshotReleasesIt() {
        LSMStoreEngine engine = open();
        try {
            engine.put("k", "v");
            Snapshot at = engine.snapshot();
            assertTrue(at.isOpen());
            at.close();
            assertFalse(at.isOpen());
            // Idempotent, so a second close in a finally block is harmless.
            at.close();
            assertFalse(at.isOpen());
        } finally {
            engine.close();
        }
    }

    /**
     * Two readers can sit on the same version, so closing one must not hand
     * back the history the other is still holding. Counting opens per version
     * is what makes that work; dropping the version outright on the first
     * close would let compaction discard under the survivor.
     */
    @Test
    void closingOneOfTwoSnapshotsAtAVersionLeavesTheOtherHolding() {
        LSMStoreEngine engine = open(64, 1);
        try {
            engine.put("k", "before");
            Snapshot first = engine.snapshot();
            Snapshot second = engine.snapshot();
            assertEquals(first.version(), second.version(),
                    "nothing was written between them, so they share a version");

            first.close();
            try (second) {
                for (int i = 0; i < 400; i++) {
                    engine.put("k", "churn" + i);
                }
                engine.compactIdleNow();

                Optional<String> seen = engine.get("k", second);
                assertTrue(seen.isPresent(),
                        "the surviving snapshot's history went with the closed one");
                assertEquals("before", seen.orElseThrow());
            }
        } finally {
            engine.close();
        }
    }

    @Test
    void readingThroughAClosedSnapshotIsRejected() {
        LSMStoreEngine engine = open();
        try {
            engine.put("k", "v");
            Snapshot at = engine.snapshot();
            at.close();

            // Answering would mean reading at a version whose history the
            // store is free to have reclaimed already.
            assertThrows(IllegalStateException.class, () -> engine.get("k", at));
            assertThrows(IllegalStateException.class, () -> engine.scan(null, null, at));
        } finally {
            engine.close();
        }
    }

    @Test
    void aNullSnapshotIsRejected() {
        LSMStoreEngine engine = open();
        try {
            assertThrows(IllegalArgumentException.class, () -> engine.get("k", null));
        } finally {
            engine.close();
        }
    }

    @Test
    void aSnapshotOfAnEmptyStoreReadsAsEmpty() {
        LSMStoreEngine engine = open();
        try {
            try (Snapshot at = engine.snapshot()) {
                engine.put("k", "after");
                assertTrue(engine.get("k", at).isEmpty());
                assertEquals(Map.of(), scanAll(engine, at));
            }
        } finally {
            engine.close();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 8})
    void snapshotsHoldAcrossShards(int shards) {
        LSMStoreEngine engine = open(4 * 1024 * 1024, shards);
        try {
            for (int i = 0; i < 50; i++) {
                engine.put("key" + i, "before");
            }
            try (Snapshot at = engine.snapshot()) {
                for (int i = 0; i < 50; i++) {
                    engine.put("key" + i, "after");
                }
                for (int i = 0; i < 50; i++) {
                    assertEquals("before", engine.get("key" + i, at).orElseThrow(),
                            "key" + i + " with " + shards + " shards");
                }
            }
        } finally {
            engine.close();
        }
    }

    /**
     * A snapshot taken partway through a random workload, then checked against
     * what the store looked like at that moment. The store keeps changing
     * afterwards, which is the whole point.
     */
    @ParameterizedTest(name = "seed={0}")
    @MethodSource("seeds")
    void aSnapshotMatchesTheStoreAsItWasWhenTaken(long seed) {
        Random random = new Random(seed);
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            keys.add("key" + i);
        }

        LSMStoreEngine engine = open(256, 2);
        try {
            TreeMap<String, String> expected = new TreeMap<>();
            for (int i = 0; i < 60; i++) {
                apply(engine, expected, keys.get(random.nextInt(keys.size())), random);
            }

            try (Snapshot at = engine.snapshot()) {
                Map<String, String> atSnapshot = new TreeMap<>(expected);

                // Keep going, hard enough to flush and compact underneath it.
                for (int i = 0; i < 300; i++) {
                    apply(engine, expected, keys.get(random.nextInt(keys.size())), random);
                }
                engine.compactIdleNow();

                for (String key : keys) {
                    assertEquals(Optional.ofNullable(atSnapshot.get(key)),
                            engine.get(key, at), key + " at the snapshot");
                    assertEquals(Optional.ofNullable(expected.get(key)),
                            engine.get(key), key + " now");
                }
                assertEquals(atSnapshot, scanAll(engine, at), "scan at the snapshot");
                assertEquals(expected, scanAll(engine), "scan now");
            }
        } finally {
            engine.close();
        }
    }

    private void apply(LSMStoreEngine engine, TreeMap<String, String> model,
            String key, Random random) {
        if (random.nextInt(4) == 0) {
            engine.delete(key);
            model.remove(key);
        } else {
            String value = "v" + random.nextInt(1_000_000);
            engine.put(key, value);
            model.put(key, value);
        }
    }

    private static Stream<Long> seeds() {
        return IntStream.range(0, 20).mapToObj(i -> 6100L + i);
    }
}
