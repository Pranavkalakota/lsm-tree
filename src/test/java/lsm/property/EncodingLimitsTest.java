package lsm.property;

import lsm.LSMStoreEngine;
import lsm.memtable.Entry;
import lsm.sstable.SSTableReader;
import lsm.sstable.SSTableWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Optional;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What the store does with strings that UTF-8 cannot represent.
 *
 * <p>Java strings are UTF-16 and can hold an unpaired surrogate, which is not
 * a valid character and has no UTF-8 encoding. Everything here goes to disk as
 * UTF-8, so such a key cannot survive the trip. These tests pin down what
 * actually happens rather than leaving it as a surprise, and they exist
 * because a generated round-trip case tripped over it.
 */
class EncodingLimitsTest {

    @TempDir
    Path dir;

    /** An unpaired high surrogate: legal in a String, not encodable as UTF-8. */
    private static final String LONE = "\uD83D";

    /** The same code point, correctly paired, which UTF-8 handles fine. */
    private static final String PAIRED = "\uD83D\uDE80";

    private static final String LONE_SURROGATE = "key_" + LONE;

    @Test
    void aLoneSurrogateHasNoFaithfulUtf8Form() {
        byte[] encoded = LONE_SURROGATE.getBytes(StandardCharsets.UTF_8);
        String decoded = new String(encoded, StandardCharsets.UTF_8);
        // Java substitutes rather than failing, so the loss is silent at the
        // language level before the store is involved at all.
        assertNotEquals(LONE_SURROGATE, decoded);
    }

    @Test
    void aLoneSurrogateKeyDoesNotSurviveATable() throws IOException {
        TreeMap<String, Entry> entries = new TreeMap<>();
        entries.put(LONE_SURROGATE, Entry.put("value"));
        Path path = dir.resolve("table.sst");
        SSTableWriter.write(path, entries);

        try (SSTableReader reader = new SSTableReader(path)) {
            // The key comes back substituted, so looking it up by the original
            // finds nothing. Worth knowing; a caller needing byte-exact keys
            // would have to encode them itself.
            assertNull(reader.get(LONE_SURROGATE));
            assertEquals(1, reader.entryCount(), "the record is stored, just under a changed key");
        }
    }

    @Test
    void aLoneSurrogateValueIsAlsoSubstituted() throws IOException {
        TreeMap<String, Entry> entries = new TreeMap<>();
        entries.put("key", Entry.put("value_\uD83D"));
        Path path = dir.resolve("values.sst");
        SSTableWriter.write(path, entries);

        try (SSTableReader reader = new SSTableReader(path)) {
            assertNotEquals("value_\uD83D", reader.get("key").value().orElseThrow());
        }
    }

    @Test
    void wellFormedSupplementaryCharactersAreUnaffected() throws IOException {
        // The same code point, correctly paired, round trips exactly.
        String paired = "key_🚀";
        TreeMap<String, Entry> entries = new TreeMap<>();
        entries.put(paired, Entry.put("value_🚀"));
        Path path = dir.resolve("paired.sst");
        SSTableWriter.write(path, entries);

        try (SSTableReader reader = new SSTableReader(path)) {
            assertEquals("value_🚀", reader.get(paired).value().orElseThrow());
        }
    }

    @Test
    void theEngineHoldsALoneSurrogateInMemoryButLosesItOnDisk() {
        Path db = dir.resolve("db");
        LSMStoreEngine engine = new LSMStoreEngine(db, 4 * 1024 * 1024);
        try {
            engine.put(PAIRED + " paired", "survives");
            engine.put("lone " + LONE, "does not");

            // Nothing has been encoded yet, so the MemTable still holds the
            // key exactly as it was handed over.
            assertEquals(Optional.of("does not"), engine.get("lone " + LONE));
        } finally {
            engine.close();
        }

        // Reopening replays the log, which is UTF-8, so what comes back is the
        // substituted key and the original no longer matches it.
        LSMStoreEngine reopened = new LSMStoreEngine(db, 4 * 1024 * 1024);
        try {
            assertEquals(Optional.of("survives"), reopened.get(PAIRED + " paired"));
            assertEquals(Optional.empty(), reopened.get("lone " + LONE));
        } finally {
            reopened.close();
        }
    }
}
