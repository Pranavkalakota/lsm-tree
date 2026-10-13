package lsm.memtable;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/** Reading a MemTable at a past version rather than only at the latest one. */
class MemTableVersionTest {

    private MemTable table;

    @BeforeEach
    void setUp() {
        table = new MemTable(1 << 20);
    }

    @Test
    void laterVersionWins() {
        table.put("k", "old", 1);
        table.put("k", "new", 2);
        assertEquals("new", table.get("k").value().orElseThrow());
    }

    @Test
    void writingOutOfOrderStillReadsNewest() {
        // Nothing stops a caller writing a lower version second; the ordering
        // is the map's, not the arrival order's.
        table.put("k", "new", 2);
        table.put("k", "old", 1);
        assertEquals("new", table.get("k").value().orElseThrow());
    }

    @Test
    void readingAtAVersionSeesThatVersion() {
        table.put("k", "a", 10);
        table.put("k", "b", 20);
        table.put("k", "c", 30);

        assertEquals("a", table.get("k", 10).value().orElseThrow());
        assertEquals("a", table.get("k", 19).value().orElseThrow());
        assertEquals("b", table.get("k", 20).value().orElseThrow());
        assertEquals("c", table.get("k", 99).value().orElseThrow());
    }

    @Test
    void readingBeforeTheKeyExistedSeesNothing() {
        table.put("k", "a", 10);
        assertNull(table.get("k", 9));
        assertNull(table.get("k", 0));
    }

    @Test
    void aSeekPastAKeysHistoryDoesNotAnswerWithAnotherKey() {
        table.put("a", "first", 10);
        table.put("b", "second", 1);
        // "a" had no value at version 1, and "b" is the next record in the map.
        assertNull(table.get("a", 1));
    }

    @Test
    void deleteHidesTheValueOnlyFromLaterReaders() {
        table.put("k", "v", 5);
        table.delete("k", 8);

        assertTrue(table.get("k").isTombstone());
        assertTrue(table.get("k", 8).isTombstone());
        assertEquals("v", table.get("k", 7).value().orElseThrow());
    }

    @Test
    void versionsAccumulateWhileKeysDoNot() {
        table.put("k", "a", 1);
        table.put("k", "b", 2);
        table.put("k", "c", 3);

        assertEquals(3, table.versionCount());
        assertEquals(1, table.entryCount());
    }

    @Test
    void everyVersionCountsTowardTheSizeBound() {
        table.put("k", "value", 1);
        long afterOne = table.getSizeBytes();
        table.put("k", "value", 2);

        assertTrue(table.getSizeBytes() > afterOne,
                "a second version is more data held, so it has to be charged for");
    }

    @Test
    void entriesHoldsOnlyTheCurrentValues() {
        table.put("a", "a1", 1);
        table.put("a", "a2", 4);
        table.put("b", "b1", 2);

        Map<String, Entry> entries = table.entries();
        assertEquals(List.of("a", "b"), List.copyOf(entries.keySet()));
        assertEquals("a2", entries.get("a").value().orElseThrow());
    }

    @Test
    void rangeAtAVersionCollapsesToWhatWasVisible() {
        table.put("a", "a-old", 1);
        table.put("a", "a-new", 9);
        table.put("b", "b-only", 5);
        table.put("c", "c-later", 12);

        Map<String, Entry> atSix = table.range(null, null, 6);
        assertEquals(List.of("a", "b"), List.copyOf(atSix.keySet()));
        assertEquals("a-old", atSix.get("a").value().orElseThrow());
    }

    @Test
    void rangeBoundsCoverEveryVersionOfTheBoundingKeys() {
        table.put("a", "1", 1);
        table.put("b", "1", 1);
        table.put("b", "2", 7);
        table.put("c", "1", 1);

        // The lower bound must admit all of "b"'s versions and the upper bound
        // must exclude all of "c"'s, even though both keys carry several.
        Map<String, Entry> range = table.range("b", "c");
        assertEquals(List.of("b"), List.copyOf(range.keySet()));
        assertEquals("2", range.get("b").value().orElseThrow());
    }

    /**
     * A one-sided range takes a different path to a two-sided one, and the
     * bound still has to cover every version of the key it names.
     */
    @Test
    void openEndedRangeCoversEveryVersionOfItsLowerBound() {
        table.put("a", "1", 1);
        table.put("b", "old", 1);
        table.put("b", "new", 7);

        Map<String, Entry> range = table.range("b", null);
        assertEquals(List.of("b"), List.copyOf(range.keySet()));
        assertEquals("new", range.get("b").value().orElseThrow());
    }

    @Test
    void openStartedRangeExcludesEveryVersionOfItsUpperBound() {
        table.put("a", "old", 1);
        table.put("a", "new", 7);
        table.put("b", "1", 1);
        table.put("b", "2", 9);

        Map<String, Entry> range = table.range(null, "b");
        assertEquals(List.of("a"), List.copyOf(range.keySet()));
        assertEquals("new", range.get("a").value().orElseThrow());
    }

    @Test
    void rangeKeepsTombstonesSoCallersCanSeeTheDelete() {
        table.put("a", "v", 1);
        table.delete("a", 2);
        assertTrue(table.range(null, null).get("a").isTombstone());
    }

    @Test
    void invertedRangeIsEmpty() {
        table.put("b", "v", 1);
        assertTrue(table.range("c", "a").isEmpty());
    }

    @Test
    void clearDropsEveryVersion() {
        table.put("k", "a", 1);
        table.put("k", "b", 2);
        table.clear();

        assertEquals(0, table.versionCount());
        assertEquals(0, table.getSizeBytes());
        assertNull(table.get("k", 2));
    }

    /**
     * A random write history read back at every version, against a model built
     * by replaying the writes in order. This is the invariant that matters: a
     * reader at a version sees exactly the writes committed at or before it.
     */
    @ParameterizedTest(name = "seed={0}")
    @MethodSource("seeds")
    void everyVersionSeesExactlyTheWritesUpToIt(long seed) {
        Random random = new Random(seed);
        List<String> keys = List.of("alpha", "beta", "gamma", "delta");
        MemTable subject = new MemTable(Long.MAX_VALUE);

        // version -> key -> expected value, null meaning deleted
        List<Map<String, String>> history = new ArrayList<>();
        Map<String, String> state = new TreeMap<>();
        history.add(new TreeMap<>(state));

        for (long version = 1; version <= 60; version++) {
            String key = keys.get(random.nextInt(keys.size()));
            if (random.nextInt(4) == 0) {
                subject.delete(key, version);
                state.remove(key);
            } else {
                String value = "v" + version;
                subject.put(key, value, version);
                state.put(key, value);
            }
            history.add(new TreeMap<>(state));
        }

        for (int version = 0; version < history.size(); version++) {
            Map<String, String> expected = history.get(version);
            for (String key : keys) {
                Entry entry = subject.get(key, version);
                String actual = entry == null || entry.isTombstone()
                        ? null
                        : entry.value().orElseThrow();
                assertEquals(expected.get(key), actual,
                        "key " + key + " read at version " + version);
            }

            Map<String, Entry> range = subject.range(null, null, version);
            Map<String, String> live = new TreeMap<>();
            range.forEach((key, entry) -> {
                if (!entry.isTombstone()) {
                    live.put(key, entry.value().orElseThrow());
                }
            });
            assertEquals(expected, live, "range read at version " + version);
        }
    }

    /** The same history, but checking a key range rather than whole keys. */
    @ParameterizedTest(name = "seed={0}")
    @MethodSource("seeds")
    void rangeAtAVersionAgreesWithReadingEachKey(long seed) {
        Random random = new Random(seed);
        MemTable subject = new MemTable(Long.MAX_VALUE);
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            keys.add(String.format("key%02d", i));
        }

        for (long version = 1; version <= 80; version++) {
            subject.put(keys.get(random.nextInt(keys.size())), "v" + version, version);
        }

        for (long snapshot : new long[] {1, 7, 23, 40, 80}) {
            String from = keys.get(2);
            String to = keys.get(9);
            Map<String, Entry> range = subject.range(from, to, snapshot);

            for (String key : keys) {
                Entry direct = subject.get(key, snapshot);
                boolean inRange = key.compareTo(from) >= 0 && key.compareTo(to) < 0;
                if (inRange && direct != null) {
                    assertEquals(direct, range.get(key), key + " at " + snapshot);
                } else {
                    assertNull(range.get(key), key + " should be outside the range");
                }
            }
        }
    }

    private static Stream<Long> seeds() {
        return IntStream.range(0, 25).mapToObj(i -> 4200L + i);
    }
}
