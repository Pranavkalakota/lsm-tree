package lsm.sstable;

import lsm.memtable.Entry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.*;

/** Streaming writes and full-table scans, which is what compaction runs on. */
class SSTableCursorTest {

    @TempDir
    Path dir;

    private List<SSTableReader.Row> scan(Path path) throws IOException {
        List<SSTableReader.Row> rows = new ArrayList<>();
        try (SSTableReader reader = new SSTableReader(path)) {
            SSTableReader.Cursor cursor = reader.cursor();
            while (cursor.hasNext()) {
                rows.add(cursor.next());
            }
        }
        return rows;
    }

    // --- streaming writer ---

    @Test
    void streamedEntriesMatchTheMapForm() throws IOException {
        TreeMap<String, Entry> map = new TreeMap<>();
        for (int i = 0; i < 500; i++) {
            map.put(String.format("key_%04d", i), Entry.put("value_" + i));
        }

        Path viaMap = dir.resolve("map.sst");
        SSTableWriter.write(viaMap, map);

        Path viaStream = dir.resolve("stream.sst");
        try (SSTableWriter writer = SSTableWriter.create(viaStream)) {
            for (var entry : map.entrySet()) {
                writer.add(entry.getKey(), entry.getValue());
            }
            writer.finish();
        }

        assertArrayEquals(Files.readAllBytes(viaMap), Files.readAllBytes(viaStream),
                "the two paths should produce byte-identical tables");
    }

    @Test
    void keysGoingBackwardsAreRejected() throws IOException {
        Path path = dir.resolve("table.sst");
        try (SSTableWriter writer = SSTableWriter.create(path)) {
            writer.add("b", Entry.put("1"));
            // Compaction feeding a merge out of order would otherwise write a
            // table whose index silently cannot find half its keys.
            assertThrows(IOException.class, () -> writer.add("a", Entry.put("2")));
            assertThrows(IOException.class, () -> writer.add("b", Entry.put("3")));
        }
    }

    @Test
    void abandoningAWriterLeavesNothingBehind() throws IOException {
        Path path = dir.resolve("table.sst");
        try (SSTableWriter writer = SSTableWriter.create(path)) {
            writer.add("key", Entry.put("value"));
            // no finish()
        }
        assertFalse(Files.exists(path), "unfinished table should not be published");
        try (var files = Files.list(dir)) {
            assertEquals(0, files.count(), "temp file should be gone too");
        }
    }

    @Test
    void finishingTwiceIsHarmless() throws IOException {
        Path path = dir.resolve("table.sst");
        try (SSTableWriter writer = SSTableWriter.create(path)) {
            writer.add("key", Entry.put("value"));
            writer.finish();
            assertDoesNotThrow(writer::finish);
        }
        assertTrue(Files.exists(path));
    }

    @Test
    void addingAfterFinishFails() throws IOException {
        Path path = dir.resolve("table.sst");
        try (SSTableWriter writer = SSTableWriter.create(path)) {
            writer.add("a", Entry.put("1"));
            writer.finish();
            assertThrows(IOException.class, () -> writer.add("b", Entry.put("2")));
        }
    }

    // --- cursor ---

    @Test
    void walksEveryEntryInOrder() throws IOException {
        TreeMap<String, Entry> map = new TreeMap<>();
        for (int i = 0; i < 1_000; i++) {
            map.put(String.format("key_%04d", i), Entry.put("value_" + i));
        }
        Path path = dir.resolve("table.sst");
        SSTableWriter.write(path, map);

        List<SSTableReader.Row> rows = scan(path);
        assertEquals(1_000, rows.size());
        for (int i = 0; i < 1_000; i++) {
            assertEquals(String.format("key_%04d", i), rows.get(i).key());
            assertEquals("value_" + i, rows.get(i).value().value().orElseThrow());
        }
    }

    @Test
    void crossesBlockBoundaries() throws IOException {
        TreeMap<String, Entry> map = new TreeMap<>();
        for (int i = 0; i < 2_000; i++) {
            map.put(String.format("key_%04d", i), Entry.put("value_" + i));
        }
        Path path = dir.resolve("table.sst");
        SSTableWriter.write(path, map);

        try (SSTableReader reader = new SSTableReader(path)) {
            assertTrue(reader.blockCount() > 1, "test needs a multi-block table");
        }
        assertEquals(2_000, scan(path).size());
    }

    @Test
    void surfacesTombstones() throws IOException {
        TreeMap<String, Entry> map = new TreeMap<>();
        map.put("alive", Entry.put("value"));
        map.put("dead", Entry.tombstone());
        Path path = dir.resolve("table.sst");
        SSTableWriter.write(path, map);

        List<SSTableReader.Row> rows = scan(path);
        assertEquals(2, rows.size());
        assertFalse(rows.get(0).value().isTombstone());
        // Compaction has to see the tombstone; only it knows whether an older
        // table underneath still needs shadowing.
        assertTrue(rows.get(1).value().isTombstone());
    }

    @Test
    void handlesAnEmptyTable() throws IOException {
        Path path = dir.resolve("empty.sst");
        SSTableWriter.write(path, new TreeMap<>());

        try (SSTableReader reader = new SSTableReader(path)) {
            assertFalse(reader.cursor().hasNext());
        }
        assertEquals(0, scan(path).size());
    }

    @Test
    void handlesRecordsWiderThanABlock() throws IOException {
        TreeMap<String, Entry> map = new TreeMap<>();
        map.put("a", Entry.put("x".repeat(64 * 1024)));
        map.put("b", Entry.put("small"));
        Path path = dir.resolve("wide.sst");
        SSTableWriter.write(path, map);

        List<SSTableReader.Row> rows = scan(path);
        assertEquals(2, rows.size());
        assertEquals(64 * 1024, rows.get(0).value().value().orElseThrow().length());
        assertEquals("small", rows.get(1).value().value().orElseThrow());
    }

    @Test
    void scanningAgreesWithPointLookups() throws IOException {
        TreeMap<String, Entry> map = new TreeMap<>();
        for (int i = 0; i < 600; i++) {
            map.put(String.format("key_%04d", i),
                    i % 5 == 0 ? Entry.tombstone() : Entry.put("value_" + i));
        }
        Path path = dir.resolve("table.sst");
        SSTableWriter.write(path, map);

        try (SSTableReader reader = new SSTableReader(path)) {
            SSTableReader.Cursor cursor = reader.cursor();
            while (cursor.hasNext()) {
                SSTableReader.Row row = cursor.next();
                Entry direct = reader.get(row.key());
                assertNotNull(direct, "point lookup lost " + row.key());
                assertEquals(row.value().isTombstone(), direct.isTombstone());
                assertEquals(row.value().value(), direct.value());
            }
        }
    }
}
