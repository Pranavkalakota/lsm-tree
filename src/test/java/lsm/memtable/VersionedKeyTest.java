package lsm.memtable;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class VersionedKeyTest {

    @Test
    void keysAscend() {
        assertTrue(new VersionedKey("a", 0).compareTo(new VersionedKey("b", 0)) < 0);
        assertTrue(new VersionedKey("b", 9).compareTo(new VersionedKey("a", 0)) > 0);
    }

    @Test
    void versionsDescendWithinAKey() {
        assertTrue(new VersionedKey("k", 7).compareTo(new VersionedKey("k", 3)) < 0);
        assertTrue(new VersionedKey("k", 3).compareTo(new VersionedKey("k", 7)) > 0);
    }

    @Test
    void keyOrderBeatsVersionOrder() {
        // Version 0 of "a" still sorts ahead of version 99 of "b": the sorted
        // structures built on this need all of a key's history contiguous.
        assertTrue(new VersionedKey("a", 0).compareTo(new VersionedKey("b", 99)) < 0);
    }

    @Test
    void newestSortsAheadOfEveryRealVersion() {
        VersionedKey seek = VersionedKey.newest("k");
        assertTrue(seek.compareTo(new VersionedKey("k", Long.MAX_VALUE - 1)) < 0);
        assertTrue(seek.compareTo(new VersionedKey("k", 0)) < 0);
        // And no further than its own key, or a seek would answer with another's.
        assertTrue(seek.compareTo(new VersionedKey("l", 0)) < 0);
    }

    @Test
    void equalKeyAndVersionCompareEqual() {
        assertEquals(0, new VersionedKey("k", 4).compareTo(new VersionedKey("k", 4)));
        assertEquals(new VersionedKey("k", 4), new VersionedKey("k", 4));
    }

    @Test
    void rejectsNegativeVersion() {
        assertThrows(IllegalArgumentException.class, () -> new VersionedKey("k", -1));
    }

    @Test
    void rejectsNullKey() {
        assertThrows(IllegalArgumentException.class, () -> new VersionedKey(null, 0));
    }

    @Test
    void hasKeyIgnoresVersion() {
        assertTrue(new VersionedKey("k", 12).hasKey("k"));
        assertFalse(new VersionedKey("k", 12).hasKey("other"));
    }

    /**
     * The ordering exists to serve one question, so this asks it directly: a
     * seek to {@code (key, snapshot)} must land on the newest version of that
     * key at or before the snapshot.
     */
    @Test
    void seekFindsNewestVisibleVersion() {
        TreeMap<VersionedKey, String> map = new TreeMap<>();
        map.put(new VersionedKey("k", 10), "ten");
        map.put(new VersionedKey("k", 20), "twenty");
        map.put(new VersionedKey("k", 30), "thirty");

        assertEquals("thirty", map.ceilingEntry(VersionedKey.newest("k")).getValue());
        assertEquals("thirty", map.ceilingEntry(new VersionedKey("k", 30)).getValue());
        assertEquals("twenty", map.ceilingEntry(new VersionedKey("k", 29)).getValue());
        assertEquals("ten", map.ceilingEntry(new VersionedKey("k", 10)).getValue());
        // Before the key existed: the seek runs off the end of its history.
        assertNull(map.ceilingEntry(new VersionedKey("k", 9)));
    }

    @Test
    void seekStopsAtItsOwnKey() {
        TreeMap<VersionedKey, String> map = new TreeMap<>();
        map.put(new VersionedKey("a", 5), "a5");
        map.put(new VersionedKey("b", 5), "b5");

        // "a" did not exist at version 1, and the answer is not "b"'s value.
        VersionedKey found = map.ceilingKey(new VersionedKey("a", 1));
        assertNotNull(found);
        assertFalse(found.hasKey("a"));
    }

    /**
     * Checks the ordering against an independently sorted model: keys grouped
     * and ascending, versions descending inside each group.
     */
    @ParameterizedTest(name = "seed={0}")
    @MethodSource("seeds")
    void sortsIntoGroupedKeysWithDescendingVersions(long seed) {
        Random random = new Random(seed);
        List<VersionedKey> shuffled = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            shuffled.add(new VersionedKey(
                    String.valueOf((char) ('a' + random.nextInt(6))),
                    random.nextInt(50)));
        }

        List<VersionedKey> sorted = shuffled.stream().distinct().sorted().toList();
        for (int i = 1; i < sorted.size(); i++) {
            VersionedKey before = sorted.get(i - 1);
            VersionedKey current = sorted.get(i);
            if (before.key().equals(current.key())) {
                assertTrue(before.version() > current.version(),
                        "versions must descend within " + current.key());
            } else {
                assertTrue(before.key().compareTo(current.key()) < 0,
                        "keys must ascend across groups");
            }
        }
    }

    /** A seek at every version, against a model built by filtering. */
    @ParameterizedTest(name = "seed={0}")
    @MethodSource("seeds")
    void seekAgreesWithFilteringTheHistory(long seed) {
        Random random = new Random(seed);
        TreeMap<VersionedKey, Long> map = new TreeMap<>();
        List<Long> versions = new ArrayList<>();
        for (int i = 0; i < 40; i++) {
            long version = random.nextInt(500);
            versions.add(version);
            map.put(new VersionedKey("k", version), version);
        }

        for (long snapshot = 0; snapshot < 500; snapshot++) {
            long at = snapshot;
            Long expected = versions.stream().filter(v -> v <= at).max(Long::compare).orElse(null);
            var found = map.ceilingEntry(new VersionedKey("k", snapshot));
            assertEquals(expected, found == null ? null : found.getValue(),
                    "snapshot " + snapshot);
        }
    }

    private static Stream<Long> seeds() {
        return IntStream.range(0, 20).mapToObj(i -> 9000L + i);
    }
}
