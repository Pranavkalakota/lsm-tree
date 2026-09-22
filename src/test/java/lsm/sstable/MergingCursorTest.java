package lsm.sstable;

import lsm.memtable.Entry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.*;

/** The k-way merge compaction is built on: ascending keys, newest source wins. */
class MergingCursorTest {

    @TempDir
    Path dir;

    private final List<SSTableReader> open = new ArrayList<>();

    /** Writes a table and returns a cursor over it. Sources are given newest first. */
    private SSTableReader.Cursor table(String name, TreeMap<String, Entry> entries)
            throws IOException {
        Path path = dir.resolve(name + ".sst");
        SSTableWriter.write(path, entries);
        SSTableReader reader = new SSTableReader(path);
        open.add(reader);
        return reader.cursor();
    }

    private static TreeMap<String, Entry> of(String... pairs) {
        TreeMap<String, Entry> map = new TreeMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            map.put(pairs[i], pairs[i + 1] == null ? Entry.tombstone() : Entry.put(pairs[i + 1]));
        }
        return map;
    }

    private List<SSTableReader.Row> drain(SSTableReader.Cursor... sources) throws IOException {
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
    void interleavesDisjointSources() throws IOException {
        var a = table("a", of("a", "1", "c", "3"));
        var b = table("b", of("b", "2", "d", "4"));

        List<SSTableReader.Row> rows = drain(a, b);
        assertEquals(List.of("a", "b", "c", "d"), rows.stream().map(SSTableReader.Row::key).toList());
    }

    @Test
    void newestSourceWinsAKeyHeldByBoth() throws IOException {
        var newer = table("newer", of("shared", "new"));
        var older = table("older", of("shared", "old"));

        List<SSTableReader.Row> rows = drain(newer, older);
        assertEquals(1, rows.size(), "the key should appear once, not twice");
        assertEquals("new", rows.get(0).value().value().orElseThrow());
    }

    @Test
    void newestWinsEvenWhenItIsTheTombstone() throws IOException {
        var newer = table("newer", of("gone", null));
        var older = table("older", of("gone", "value"));

        List<SSTableReader.Row> rows = drain(newer, older);
        assertEquals(1, rows.size());
        // The merge surfaces the tombstone rather than dropping it: whether it
        // is safe to discard depends on levels the merge cannot see.
        assertTrue(rows.get(0).value().isTombstone());
    }

    @Test
    void newestWinsEvenWhenTheTombstoneIsOlder() throws IOException {
        var newer = table("newer", of("back", "resurrected"));
        var older = table("older", of("back", null));

        List<SSTableReader.Row> rows = drain(newer, older);
        assertEquals(1, rows.size());
        assertEquals("resurrected", rows.get(0).value().value().orElseThrow());
    }

    @Test
    void collapsesAKeyHeldByEverySource() throws IOException {
        var a = table("a", of("k", "1"));
        var b = table("b", of("k", "2"));
        var c = table("c", of("k", "3"));
        var d = table("d", of("k", "4"));

        List<SSTableReader.Row> rows = drain(a, b, c, d);
        assertEquals(1, rows.size());
        assertEquals("1", rows.get(0).value().value().orElseThrow(), "first source is newest");
    }

    @Test
    void toleratesEmptySources() throws IOException {
        var empty = table("empty", of());
        var full = table("full", of("a", "1", "b", "2"));

        assertEquals(2, drain(empty, full).size());
    }

    @Test
    void producesNothingFromNothing() throws IOException {
        var one = table("one", of());
        var two = table("two", of());
        assertEquals(0, drain(one, two).size());
    }

    @Test
    void outputStaysSortedAcrossManyOverlappingSources() throws IOException {
        Random random = new Random(7);
        TreeMap<String, String> expected = new TreeMap<>();
        List<SSTableReader.Cursor> sources = new ArrayList<>();

        // Build oldest first so that writing them in reverse gives newest-first
        // order, with later tables deliberately overwriting earlier keys.
        List<TreeMap<String, Entry>> generations = new ArrayList<>();
        for (int gen = 0; gen < 6; gen++) {
            TreeMap<String, Entry> table = new TreeMap<>();
            for (int i = 0; i < 300; i++) {
                String key = String.format("key_%04d", random.nextInt(800));
                String value = "gen" + gen + "_" + i;
                table.put(key, Entry.put(value));
                expected.put(key, value);
            }
            generations.add(table);
        }
        for (int gen = generations.size() - 1; gen >= 0; gen--) {
            sources.add(table("gen" + gen, generations.get(gen)));
        }

        List<SSTableReader.Row> rows = drain(sources.toArray(SSTableReader.Cursor[]::new));

        assertEquals(expected.size(), rows.size(), "each key should survive exactly once");
        String previous = null;
        for (SSTableReader.Row row : rows) {
            if (previous != null) {
                assertTrue(row.key().compareTo(previous) > 0,
                        "keys must ascend, got " + row.key() + " after " + previous);
            }
            previous = row.key();
            assertEquals(expected.get(row.key()), row.value().value().orElseThrow(),
                    "wrong generation won for " + row.key());
        }
    }
}
