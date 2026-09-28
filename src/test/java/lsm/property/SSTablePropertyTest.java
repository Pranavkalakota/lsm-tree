package lsm.property;

import lsm.memtable.Entry;
import lsm.sstable.SSTableReader;
import lsm.sstable.SSTableWriter;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Invariants that should hold for any table, checked against generated data.
 *
 * <p>Each case is one dataset built from a seed named in the case title, so a
 * failure is reproducible by re-running that case alone rather than by
 * guessing what input broke it.
 */
class SSTablePropertyTest {

    @TempDir
    Path dir;

    static Stream<Generators.Shape> shapes() {
        return Generators.shapes(25);
    }

    private Path write(Generators.Shape shape, Map<String, Entry> entries) throws IOException {
        Path path = dir.resolve("table.sst");
        SSTableWriter.write(path, entries);
        return path;
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("shapes")
    void everyStoredEntryReadsBackIdentically(Generators.Shape shape) throws IOException {
        TreeMap<String, Entry> expected = shape.build();
        Path path = write(shape, expected);

        try (SSTableReader reader = new SSTableReader(path)) {
            for (var entry : expected.entrySet()) {
                Entry found = reader.get(entry.getKey());
                assertNotNull(found, "lost key: " + entry.getKey());
                assertEquals(entry.getValue().isTombstone(), found.isTombstone(),
                        "tombstone flag flipped for: " + entry.getKey());
                assertEquals(entry.getValue().value(), found.value(),
                        "value changed for: " + entry.getKey());
            }
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("shapes")
    void scanningYieldsExactlyWhatWasWrittenInOrder(Generators.Shape shape) throws IOException {
        TreeMap<String, Entry> expected = shape.build();
        Path path = write(shape, expected);

        try (SSTableReader reader = new SSTableReader(path)) {
            var iterator = expected.entrySet().iterator();
            SSTableReader.Cursor cursor = reader.cursor();
            String previous = null;

            while (cursor.hasNext()) {
                SSTableReader.Row row = cursor.next();
                assertTrue(iterator.hasNext(), "scan produced more rows than were written");
                var wanted = iterator.next();

                assertEquals(wanted.getKey(), row.key(), "scan order diverged");
                assertEquals(wanted.getValue().value(), row.value().value());
                if (previous != null) {
                    assertTrue(row.key().compareTo(previous) > 0, "keys stopped ascending");
                }
                previous = row.key();
            }
            assertFalse(iterator.hasNext(), "scan ended early");
            assertEquals(expected.size(), reader.entryCount());
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("shapes")
    void absentKeysAreNeverInvented(Generators.Shape shape) throws IOException {
        TreeMap<String, Entry> expected = shape.build();
        Path path = write(shape, expected);

        try (SSTableReader reader = new SSTableReader(path)) {
            shape.absentKeys(expected, 40).forEach(key ->
                    assertDoesNotThrow(() -> assertNull(reader.get(key),
                            "invented a value for absent key: " + key)));
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("shapes")
    void theBloomFilterNeverDeniesAStoredKey(Generators.Shape shape) throws IOException {
        TreeMap<String, Entry> expected = shape.build();
        Path path = write(shape, expected);

        try (SSTableReader reader = new SSTableReader(path)) {
            // A false negative is the one failure a bloom filter must never
            // have: it would hide live data behind a table that holds it.
            for (String key : expected.keySet()) {
                assertTrue(reader.mightContain(key), "filter denied stored key: " + key);
            }
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("shapes")
    void theRecordedKeyRangeBoundsEveryKey(Generators.Shape shape) throws IOException {
        TreeMap<String, Entry> expected = shape.build();
        Path path = write(shape, expected);

        try (SSTableReader reader = new SSTableReader(path)) {
            if (expected.isEmpty()) {
                assertNull(reader.minKey());
                return;
            }
            assertEquals(expected.firstKey(), reader.minKey());
            assertEquals(expected.lastKey(), reader.maxKey());
            // Compaction decides what overlaps what from these two values, so a
            // key outside them would be one compaction could silently drop.
            for (String key : expected.keySet()) {
                assertTrue(key.compareTo(reader.minKey()) >= 0
                        && key.compareTo(reader.maxKey()) <= 0, "key outside range: " + key);
            }
        }
    }
}
