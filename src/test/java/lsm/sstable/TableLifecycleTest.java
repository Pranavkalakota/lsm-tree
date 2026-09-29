package lsm.sstable;

import lsm.memtable.Entry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Reference counting on a table, which is what lets compaction retire a file
 * while a scan is still reading it.
 *
 * <p>These drive the lifecycle directly. An end-to-end test cannot see it:
 * the block cache serves a small store entirely from memory, so a scan never
 * notices that the channel underneath it was closed.
 */
class TableLifecycleTest {

    @TempDir
    Path dir;

    private Path table() throws IOException {
        TreeMap<String, Entry> entries = new TreeMap<>();
        for (int i = 0; i < 50; i++) {
            entries.put(String.format("key_%03d", i), Entry.put("value_" + i));
        }
        Path path = dir.resolve("L0_000000.sst");
        SSTableWriter.write(path, entries);
        return path;
    }

    @Test
    void retiringWithNoReadersDeletesTheFile() throws IOException {
        Path path = table();
        SSTableReader reader = new SSTableReader(path);

        reader.retire();
        assertFalse(Files.exists(path), "nothing was holding it, so it should be gone");
    }

    @Test
    void aHeldTableSurvivesBeingRetired() throws IOException {
        Path path = table();
        SSTableReader reader = new SSTableReader(path);

        assertTrue(reader.acquire(), "a live table should hand out a reference");
        reader.retire();

        // Compaction has finished with it, but this reader has not.
        assertTrue(Files.exists(path), "file was deleted out from under a reader");
        assertEquals("value_7", reader.get("key_007").value().orElseThrow(),
                "a retired but held table must still serve reads");

        reader.release();
        assertFalse(Files.exists(path), "last reference went, so the file should follow");
    }

    @Test
    void severalReadersAllHaveToFinishFirst() throws IOException {
        Path path = table();
        SSTableReader reader = new SSTableReader(path);

        assertTrue(reader.acquire());
        assertTrue(reader.acquire());
        reader.retire();

        reader.release();
        assertTrue(Files.exists(path), "one reader is still working");
        reader.release();
        assertFalse(Files.exists(path));
    }

    @Test
    void aDeadTableRefusesNewReaders() throws IOException {
        Path path = table();
        SSTableReader reader = new SSTableReader(path);
        reader.retire();

        // A scan reading a stale table list has to be told to skip this one
        // rather than handed a closed channel.
        assertFalse(reader.acquire(), "a retired table should not hand out references");
    }

    @Test
    void retiringTwiceIsHarmless() throws IOException {
        Path path = table();
        SSTableReader reader = new SSTableReader(path);

        reader.retire();
        assertDoesNotThrow(reader::retire);
        assertFalse(Files.exists(path));
    }

    @Test
    void closingLeavesTheFileInPlace() throws IOException {
        Path path = table();
        SSTableReader reader = new SSTableReader(path);

        // Shutdown, not compaction: the table is still part of the store.
        reader.close();
        assertTrue(Files.exists(path), "closing the engine must not delete tables");
    }

    @Test
    void closingTwiceDoesNotDoubleRelease() throws IOException {
        Path path = table();
        SSTableReader reader = new SSTableReader(path);

        reader.close();
        assertDoesNotThrow(reader::close);
        // A double release would drop the count below zero and could delete a
        // file another reader still holds.
        assertTrue(Files.exists(path));
    }
}
