package lsm.sstable;

import lsm.memtable.Entry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.*;

/** Starting a scan part-way through a table, which is what a range read needs. */
class SeekableCursorTest {

    @TempDir
    Path dir;

    /** Keys key_00000 through key_01999, every one present. */
    private Path table() throws IOException {
        TreeMap<String, Entry> entries = new TreeMap<>();
        for (int i = 0; i < 2_000; i++) {
            entries.put(String.format("key_%05d", i), Entry.put("value_" + i));
        }
        Path path = dir.resolve("table.sst");
        SSTableWriter.write(path, entries);
        return path;
    }

    private List<String> keysFrom(SSTableReader reader, String from) throws IOException {
        List<String> keys = new ArrayList<>();
        SSTableReader.Cursor cursor = reader.cursorFrom(from);
        while (cursor.hasNext()) {
            keys.add(cursor.next().key());
        }
        return keys;
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 17, 500, 1023, 1999})
    void seekingLandsOnTheKeyItself(int start) throws IOException {
        Path path = table();
        String from = String.format("key_%05d", start);

        try (SSTableReader reader = new SSTableReader(path)) {
            List<String> keys = keysFrom(reader, from);
            assertEquals(from, keys.get(0), "cursor did not land on the requested key");
            assertEquals(2_000 - start, keys.size(), "wrong number of keys after the seek");
        }
    }

    @Test
    void seekingToAnAbsentKeyLandsOnTheNextOneAfterIt() throws IOException {
        Path path = table();
        try (SSTableReader reader = new SSTableReader(path)) {
            // Sorts between key_00499 and key_00500.
            List<String> keys = keysFrom(reader, "key_00499x");
            assertEquals("key_00500", keys.get(0));
            assertEquals(1_500, keys.size());
        }
    }

    @Test
    void seekingBeforeEverythingReadsTheWholeTable() throws IOException {
        Path path = table();
        try (SSTableReader reader = new SSTableReader(path)) {
            assertEquals(2_000, keysFrom(reader, "aaa").size());
        }
    }

    @Test
    void seekingPastEverythingYieldsNothing() throws IOException {
        Path path = table();
        try (SSTableReader reader = new SSTableReader(path)) {
            assertTrue(keysFrom(reader, "zzz").isEmpty());
        }
    }

    @Test
    void seekingIntoALaterBlockSkipsTheEarlierOnes() throws IOException {
        Path path = table();
        BlockCache cache = new BlockCache(64 * 1024 * 1024);

        try (SSTableReader reader = new SSTableReader(path, cache)) {
            assertTrue(reader.blockCount() > 4, "test needs a multi-block table");
            SSTableReader.Cursor cursor = reader.cursorFrom("key_01999");
            assertTrue(cursor.hasNext());
            assertEquals("key_01999", cursor.current().key());

            // The whole point of seeking: one block read, not the whole file.
            assertEquals(1, cache.blockCount(),
                    "seeking should not have walked the blocks before the target");
        }
    }

    @Test
    void seekingOnAnEmptyTableIsHarmless() throws IOException {
        Path path = dir.resolve("empty.sst");
        SSTableWriter.write(path, new TreeMap<>());

        try (SSTableReader reader = new SSTableReader(path)) {
            assertFalse(reader.cursorFrom("anything").hasNext());
        }
    }

    @Test
    void anUnseekedCursorStillReadsEverything() throws IOException {
        Path path = table();
        try (SSTableReader reader = new SSTableReader(path)) {
            SSTableReader.Cursor cursor = reader.cursor();
            int count = 0;
            while (cursor.hasNext()) {
                cursor.next();
                count++;
            }
            assertEquals(2_000, count);
        }
    }
}
