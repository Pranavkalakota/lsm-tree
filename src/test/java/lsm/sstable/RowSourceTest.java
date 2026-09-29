package lsm.sstable;

import lsm.memtable.Entry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Merging an in-memory source with an on-disk one.
 *
 * <p>This is the combination a range scan needs and compaction never did: the
 * MemTable holds the newest version of a key, and the tables below hold older
 * ones. If the merge cannot treat both as the same kind of thing, a scan
 * either misses recent writes or serves stale ones.
 */
class RowSourceTest {

    @TempDir
    Path dir;

    private final List<SSTableReader> open = new ArrayList<>();

    private static TreeMap<String, Entry> map(String... pairs) {
        TreeMap<String, Entry> entries = new TreeMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            entries.put(pairs[i], pairs[i + 1] == null
                    ? Entry.tombstone() : Entry.put(pairs[i + 1]));
        }
        return entries;
    }

    private RowSource memory(TreeMap<String, Entry> entries) {
        return RowSource.of(entries.entrySet().iterator());
    }

    private RowSource table(String name, TreeMap<String, Entry> entries) throws IOException {
        Path path = dir.resolve(name + ".sst");
        SSTableWriter.write(path, entries);
        SSTableReader reader = new SSTableReader(path);
        open.add(reader);
        return reader.cursor();
    }

    private List<SSTableReader.Row> drain(RowSource... sources) throws IOException {
        List<SSTableReader.Row> rows = new ArrayList<>();
        try (MergingCursor merged = new MergingCursor(List.of(sources))) {
            while (merged.hasNext()) {
                rows.add(merged.next());
            }
        }
        open.forEach(SSTableReader::close);
        return rows;
    }

    @Test
    void anEmptyInMemorySourceYieldsNothing() throws IOException {
        assertEquals(0, drain(memory(map())).size());
    }

    @Test
    void anInMemorySourceAloneReadsInOrder() throws IOException {
        List<SSTableReader.Row> rows = drain(memory(map("b", "2", "a", "1", "c", "3")));
        assertEquals(List.of("a", "b", "c"),
                rows.stream().map(SSTableReader.Row::key).toList());
    }

    @Test
    void memoryBeatsDiskForAKeyInBoth() throws IOException {
        RowSource newer = memory(map("shared", "from memory"));
        RowSource older = table("older", map("shared", "from disk"));

        List<SSTableReader.Row> rows = drain(newer, older);
        assertEquals(1, rows.size());
        assertEquals("from memory", rows.get(0).value().value().orElseThrow());
    }

    @Test
    void aTombstoneInMemoryShadowsTheValueOnDisk() throws IOException {
        RowSource newer = memory(map("gone", null));
        RowSource older = table("older", map("gone", "still here"));

        List<SSTableReader.Row> rows = drain(newer, older);
        assertEquals(1, rows.size());
        // Surfaced rather than dropped; a scan filters it, compaction may not.
        assertTrue(rows.get(0).value().isTombstone());
    }

    @Test
    void disjointMemoryAndDiskInterleave() throws IOException {
        RowSource newer = memory(map("a", "1", "c", "3"));
        RowSource older = table("older", map("b", "2", "d", "4"));

        assertEquals(List.of("a", "b", "c", "d"),
                drain(newer, older).stream().map(SSTableReader.Row::key).toList());
    }

    @Test
    void severalGenerationsCollapseToTheNewest() throws IOException {
        RowSource memory = memory(map("k", "newest"));
        RowSource recent = table("recent", map("k", "middle"));
        RowSource ancient = table("ancient", map("k", "oldest"));

        List<SSTableReader.Row> rows = drain(memory, recent, ancient);
        assertEquals(1, rows.size());
        assertEquals("newest", rows.get(0).value().value().orElseThrow());
    }

    @Test
    void aLargeMixStaysSortedAndUnduplicated() throws IOException {
        TreeMap<String, Entry> memoryEntries = new TreeMap<>();
        TreeMap<String, Entry> diskEntries = new TreeMap<>();
        for (int i = 0; i < 600; i++) {
            diskEntries.put(String.format("key_%04d", i), Entry.put("disk_" + i));
            if (i % 3 == 0) {
                memoryEntries.put(String.format("key_%04d", i), Entry.put("memory_" + i));
            }
        }

        List<SSTableReader.Row> rows = drain(memory(memoryEntries), table("disk", diskEntries));

        assertEquals(600, rows.size(), "every key once, no duplicates");
        String previous = null;
        for (SSTableReader.Row row : rows) {
            if (previous != null) {
                assertTrue(row.key().compareTo(previous) > 0, "keys stopped ascending");
            }
            previous = row.key();
            int index = Integer.parseInt(row.key().substring(4));
            String expected = (index % 3 == 0 ? "memory_" : "disk_") + index;
            assertEquals(expected, row.value().value().orElseThrow(),
                    "wrong source won for " + row.key());
        }
    }
}
