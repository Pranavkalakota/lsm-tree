package lsm.compaction;

import lsm.memtable.Entry;
import lsm.memtable.VersionedKey;
import lsm.sstable.BlockCache;
import lsm.sstable.SSTableReader;
import lsm.sstable.SSTableWriter;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Which versions of a key survive a compaction.
 *
 * <p>The rule under test: every version newer than the oldest live snapshot
 * survives, plus the newest version at or below it, and nothing else. The
 * consequence that actually matters is the invariant at the bottom of this
 * file — a compaction must not change what any live snapshot can see.
 */
class VersionRetentionTest {

    @TempDir
    Path dir;

    private final AtomicLong sequence = new AtomicLong(500);

    /** One record: which version of which key, and its value or a tombstone. */
    private record Write(String key, long version, String value) {
        static Write put(String key, long version, String value) {
            return new Write(key, version, value);
        }

        static Write delete(String key, long version) {
            return new Write(key, version, null);
        }

        Entry entry() {
            return value == null ? Entry.tombstone() : Entry.put(value);
        }
    }

    /** Writes one table at a level, records sorted into the stored order. */
    private SSTableReader table(int level, long seq, List<Write> writes) throws IOException {
        Path path = dir.resolve(String.format("L%d_%06d.sst", level, seq));
        TreeMap<VersionedKey, Entry> sorted = new TreeMap<>();
        for (Write write : writes) {
            sorted.put(new VersionedKey(write.key(), write.version()), write.entry());
        }
        SSTableWriter.writeVersioned(path, sorted);
        return new SSTableReader(path);
    }

    private Compactor compactorWithWatermark(long oldestSnapshot) {
        return new Compactor(dir, new BlockCache(1 << 20),
                sequence::getAndIncrement, () -> oldestSnapshot);
    }

    /** Runs a compaction of the given inputs and opens whatever it produced. */
    private List<SSTableReader> compact(long watermark, boolean bottomLevel,
            List<SSTableReader> inputs) throws IOException {
        Compactor compactor = compactorWithWatermark(watermark);
        Compactor.Job job = new Compactor.Job(1, inputs, bottomLevel);
        return compactor.open(compactor.run(job));
    }

    /** Every version of a key present across the outputs, newest first. */
    private List<Long> versionsOf(List<SSTableReader> tables, String key) throws IOException {
        List<Long> versions = new ArrayList<>();
        for (SSTableReader table : tables) {
            SSTableReader.Cursor cursor = table.cursor();
            while (cursor.hasNext()) {
                SSTableReader.Row row = cursor.next();
                if (row.key().equals(key)) {
                    versions.add(row.version());
                }
            }
        }
        return versions;
    }

    @Test
    void withNoSnapshotsOnlyTheCurrentValueSurvives() throws IOException {
        SSTableReader input = table(0, 1, List.of(
                Write.put("k", 30, "c"),
                Write.put("k", 20, "b"),
                Write.put("k", 10, "a")));

        // Long.MAX_VALUE is what the engine reports when nothing holds a
        // snapshot: no reader is entitled to history, so none is kept.
        List<SSTableReader> out = compact(Long.MAX_VALUE, true, List.of(input));
        assertEquals(List.of(30L), versionsOf(out, "k"));
        assertEquals("c", out.get(0).get("k").value().orElseThrow());
    }

    @Test
    void versionsNewerThanTheWatermarkAllSurvive() throws IOException {
        SSTableReader input = table(0, 2, List.of(
                Write.put("k", 40, "d"),
                Write.put("k", 30, "c"),
                Write.put("k", 20, "b"),
                Write.put("k", 10, "a")));

        // A reader at 15 is open, so anything above it may still be asked for
        // by a reader at a later snapshot, and 10 is the one 15 itself lands on.
        List<SSTableReader> out = compact(15, true, List.of(input));
        assertEquals(List.of(40L, 30L, 20L, 10L), versionsOf(out, "k"));
    }

    @Test
    void theNewestVersionAtOrBelowTheWatermarkSurvives() throws IOException {
        SSTableReader input = table(0, 3, List.of(
                Write.put("k", 30, "c"),
                Write.put("k", 20, "b"),
                Write.put("k", 10, "a")));

        // 25 lands on 20, so 20 is kept and 10 is unreachable history.
        List<SSTableReader> out = compact(25, true, List.of(input));
        assertEquals(List.of(30L, 20L), versionsOf(out, "k"));
        assertEquals("b", out.get(0).get("k", 25).value().orElseThrow());
    }

    /**
     * A watermark landing exactly on a stored version. That version is the one
     * the oldest reader sees, so it is kept and everything under it goes.
     *
     * <p>Treating it as "above the watermark" instead would keep it AND the
     * next version down, since nothing would have claimed the watermark slot.
     * The answers stay right, so nothing looks broken — but one extra version
     * of every such key is retained forever and never becomes collectable.
     */
    @Test
    void aWatermarkExactlyOnAVersionKeepsThatOneAndNothingOlder() throws IOException {
        SSTableReader input = table(0, 30, List.of(
                Write.put("k", 30, "c"),
                Write.put("k", 20, "b"),
                Write.put("k", 10, "a")));

        List<SSTableReader> out = compact(20, true, List.of(input));
        assertEquals(List.of(30L, 20L), versionsOf(out, "k"));
        assertEquals("b", out.get(0).get("k", 20).value().orElseThrow());
    }

    @Test
    void historyOlderThanAnyReaderIsDropped() throws IOException {
        SSTableReader input = table(0, 4, List.of(
                Write.put("k", 50, "e"),
                Write.put("k", 40, "d"),
                Write.put("k", 30, "c"),
                Write.put("k", 20, "b"),
                Write.put("k", 10, "a")));

        List<SSTableReader> out = compact(45, true, List.of(input));
        assertEquals(List.of(50L, 40L), versionsOf(out, "k"),
                "30, 20 and 10 sit under the version 45 lands on");
    }

    @Test
    void aWatermarkBelowEverythingKeepsTheWholeHistory() throws IOException {
        SSTableReader input = table(0, 5, List.of(
                Write.put("k", 30, "c"),
                Write.put("k", 20, "b"),
                Write.put("k", 10, "a")));

        List<SSTableReader> out = compact(1, true, List.of(input));
        assertEquals(List.of(30L, 20L, 10L), versionsOf(out, "k"));
    }

    @Test
    void aTombstoneAboveTheWatermarkSurvivesTheBottomLevel() throws IOException {
        SSTableReader input = table(0, 6, List.of(
                Write.delete("k", 30),
                Write.put("k", 10, "a")));

        // Dropping the delete would let a reader at 30 fall through to the
        // value at 10 and watch a deleted key come back to life.
        List<SSTableReader> out = compact(5, true, List.of(input));
        assertEquals(List.of(30L, 10L), versionsOf(out, "k"));
        assertTrue(out.get(0).get("k", 30).isTombstone());
        assertEquals("a", out.get(0).get("k", 10).value().orElseThrow());
    }

    @Test
    void aTombstoneNoReaderNeedsIsDroppedAtTheBottomLevel() throws IOException {
        SSTableReader input = table(0, 7, List.of(
                Write.delete("k", 30),
                Write.put("k", 10, "a")));

        List<SSTableReader> out = compact(Long.MAX_VALUE, true, List.of(input));
        assertTrue(versionsOf(out, "k").isEmpty(),
                "with no reader to serve, a deleted key leaves nothing behind");
    }

    @Test
    void aTombstoneIsKeptWhenALevelBelowCouldStillHoldAValue() throws IOException {
        SSTableReader input = table(0, 8, List.of(Write.delete("k", 30)));

        // Not the bottom level, so something underneath may still answer for
        // this key and the tombstone has to go on hiding it.
        List<SSTableReader> out = compact(Long.MAX_VALUE, false, List.of(input));
        assertEquals(List.of(30L), versionsOf(out, "k"));
        assertTrue(out.get(0).get("k").isTombstone());
    }

    @Test
    void historyIsKeptPerKeyNotAcrossKeys() throws IOException {
        SSTableReader input = table(0, 9, List.of(
                Write.put("a", 40, "a40"),
                Write.put("a", 10, "a10"),
                Write.put("b", 35, "b35"),
                Write.put("b", 5, "b5")));

        List<SSTableReader> out = compact(20, true, List.of(input));
        // Each key keeps what 20 lands on plus everything above, decided
        // independently: "a" keeps 10, "b" keeps 5.
        assertEquals(List.of(40L, 10L), versionsOf(out, "a"));
        assertEquals(List.of(35L, 5L), versionsOf(out, "b"));
    }

    @Test
    void historySpreadAcrossTwoTablesIsMergedNotLost() throws IOException {
        SSTableReader newer = table(0, 20, List.of(Write.put("k", 30, "c")));
        SSTableReader older = table(0, 10, List.of(
                Write.put("k", 20, "b"),
                Write.put("k", 10, "a")));

        List<SSTableReader> out = compact(15, true, List.of(newer, older));
        assertEquals(List.of(30L, 20L, 10L), versionsOf(out, "k"),
                "versions living in different tables must all come through");
    }

    @Test
    void thesameVersionInTwoTablesIsStoredOnce() throws IOException {
        SSTableReader newer = table(0, 21, List.of(Write.put("k", 30, "newer")));
        SSTableReader older = table(0, 11, List.of(Write.put("k", 30, "older")));

        List<SSTableReader> out = compact(1, true, List.of(newer, older));
        assertEquals(List.of(30L), versionsOf(out, "k"));
        assertEquals("newer", out.get(0).get("k").value().orElseThrow(),
                "the newer table wins a straight duplicate");
    }

    /**
     * The invariant the whole rule exists to serve: for any snapshot a live
     * reader could hold, reading the compacted output gives exactly what
     * reading the inputs gave. Anything else means compaction changed history
     * under a reader's feet.
     */
    @ParameterizedTest(name = "seed={0}")
    @MethodSource("seeds")
    void compactionNeverChangesWhatALiveSnapshotSees(long seed) throws IOException {
        Random random = new Random(seed);
        List<String> keys = List.of("alpha", "beta", "gamma");

        // Two tables, so the merge has to interleave histories.
        List<Write> newer = new ArrayList<>();
        List<Write> older = new ArrayList<>();
        long version = 0;
        for (int i = 0; i < 30; i++) {
            version += 1 + random.nextInt(3);
            String key = keys.get(random.nextInt(keys.size()));
            Write write = random.nextInt(5) == 0
                    ? Write.delete(key, version)
                    : Write.put(key, version, "v" + version);
            (random.nextBoolean() ? newer : older).add(write);
        }
        long highest = version;

        SSTableReader newerTable = table(0, 100 + seed % 50, newer);
        SSTableReader olderTable = table(0, 40 + seed % 50, older);
        List<SSTableReader> inputs = List.of(newerTable, olderTable);

        long watermark = 1 + random.nextInt((int) highest);

        // What every reachable snapshot sees BEFORE the compaction.
        TreeMap<Long, TreeMap<String, String>> before = new TreeMap<>();
        for (long snapshot = watermark; snapshot <= highest + 2; snapshot++) {
            before.put(snapshot, readAll(inputs, keys, snapshot));
        }

        // The bottom level, which is where tombstones are allowed to go and so
        // the hardest case for this invariant.
        List<SSTableReader> out = compact(watermark, true, inputs);

        for (var entry : before.entrySet()) {
            assertEquals(entry.getValue(), readAll(out, keys, entry.getKey()),
                    "snapshot " + entry.getKey() + " changed, watermark " + watermark);
        }
    }

    /**
     * What a reader at {@code snapshot} is entitled to see, worked out from the
     * stored records directly rather than through {@code get}: for each key,
     * the newest version at or below the snapshot, wherever it lives.
     *
     * <p>Deliberately not "the first table that answers". A table holding only
     * versions newer than the snapshot answers nothing for that key while
     * still containing it, so stopping at the first non-null would skip the
     * older table that actually holds the visible version.
     */
    private TreeMap<String, String> readAll(List<SSTableReader> tables, List<String> keys,
            long snapshot) throws IOException {
        TreeMap<String, Long> bestVersion = new TreeMap<>();
        TreeMap<String, Entry> best = new TreeMap<>();

        // Tables newest first, and only a strictly higher version displaces a
        // find, so the newer table wins when both hold the same version.
        for (SSTableReader table : tables) {
            SSTableReader.Cursor cursor = table.cursor();
            while (cursor.hasNext()) {
                SSTableReader.Row row = cursor.next();
                if (row.version() > snapshot || !keys.contains(row.key())) {
                    continue;
                }
                Long found = bestVersion.get(row.key());
                if (found == null || row.version() > found) {
                    bestVersion.put(row.key(), row.version());
                    best.put(row.key(), row.value());
                }
            }
        }

        TreeMap<String, String> seen = new TreeMap<>();
        best.forEach((key, entry) -> {
            if (!entry.isTombstone()) {
                seen.put(key, entry.value().orElseThrow());
            }
        });
        return seen;
    }

    private static Stream<Long> seeds() {
        return IntStream.range(0, 30).mapToObj(i -> 3300L + i);
    }
}
