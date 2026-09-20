package lsm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Engine level behaviour once the MemTable starts spilling to disk.
 *
 * <p>Most cases run with a one byte MemTable bound so that every write flushes
 * immediately. That makes table ordering exact rather than dependent on how
 * many bytes of padding happen to cross a threshold.
 */
class SSTableIntegrationTest {

    @TempDir
    Path dir;

    private static final long FLUSH_EVERY_WRITE = 1;

    private long tableCount() throws IOException {
        try (Stream<Path> files = Files.list(dir)) {
            return files.filter(p -> p.toString().endsWith(".sst")).count();
        }
    }

    // --- flushing ---

    @Test
    void crossingTheBoundWritesATable() throws IOException {
        LSMStoreEngine engine = new LSMStoreEngine(dir, FLUSH_EVERY_WRITE);
        engine.put("key", "value");
        engine.close();

        assertEquals(1, tableCount());
    }

    @Test
    void staysBelowTheBoundWithoutWritingATable() throws IOException {
        LSMStoreEngine engine = new LSMStoreEngine(dir, 10 * 1024 * 1024);
        engine.put("key", "value");
        engine.close();

        assertEquals(0, tableCount());
    }

    @Test
    void readsFlushedDataBackFromDisk() {
        LSMStoreEngine engine = new LSMStoreEngine(dir, FLUSH_EVERY_WRITE);
        engine.put("key", "value");
        assertEquals(Optional.of("value"), engine.get("key"));
        engine.close();
    }

    @Test
    void flushingTrimsTheWriteAheadLog() throws IOException {
        LSMStoreEngine engine = new LSMStoreEngine(dir, FLUSH_EVERY_WRITE);
        engine.put("key", "a-reasonably-long-value-to-make-the-log-grow");
        engine.close();

        // The table now owns the data, so the log must not still be carrying it.
        assertEquals(0, Files.size(dir.resolve("wal.log")));
    }

    // --- precedence between the MemTable and tables ---

    @Test
    void memTableShadowsAnOlderTable() {
        LSMStoreEngine engine = new LSMStoreEngine(dir, FLUSH_EVERY_WRITE);
        engine.put("key", "old");
        engine.close();

        LSMStoreEngine reopened = new LSMStoreEngine(dir, 10 * 1024 * 1024);
        reopened.put("key", "new");
        assertEquals(Optional.of("new"), reopened.get("key"));
        reopened.close();
    }

    @Test
    void deleteInTheMemTableHidesATableValue() {
        LSMStoreEngine engine = new LSMStoreEngine(dir, FLUSH_EVERY_WRITE);
        engine.put("key", "value");
        engine.close();

        LSMStoreEngine reopened = new LSMStoreEngine(dir, 10 * 1024 * 1024);
        reopened.delete("key");
        assertEquals(Optional.empty(), reopened.get("key"));
        reopened.close();
    }

    // --- precedence between tables ---

    @Test
    void newerTableShadowsOlderTable() {
        LSMStoreEngine engine = new LSMStoreEngine(dir, FLUSH_EVERY_WRITE);
        engine.put("key", "first");
        engine.put("key", "second");
        engine.put("key", "third");

        assertEquals(Optional.of("third"), engine.get("key"));
        engine.close();
    }

    @Test
    void tombstoneInANewerTableHidesAnOlderTableValue() {
        LSMStoreEngine engine = new LSMStoreEngine(dir, FLUSH_EVERY_WRITE);
        engine.put("key", "value");
        engine.delete("key");

        assertEquals(Optional.empty(), engine.get("key"));
        engine.close();
    }

    @Test
    void valueWrittenAfterATombstoneWinsAgain() {
        LSMStoreEngine engine = new LSMStoreEngine(dir, FLUSH_EVERY_WRITE);
        engine.put("key", "first");
        engine.delete("key");
        engine.put("key", "resurrected");

        assertEquals(Optional.of("resurrected"), engine.get("key"));
        engine.close();
    }

    @Test
    void precedenceSurvivesAReopen() {
        LSMStoreEngine engine = new LSMStoreEngine(dir, FLUSH_EVERY_WRITE);
        engine.put("kept", "v1");
        engine.put("kept", "v2");
        engine.put("dropped", "v1");
        engine.delete("dropped");
        engine.close();

        LSMStoreEngine reopened = new LSMStoreEngine(dir);
        assertEquals(Optional.of("v2"), reopened.get("kept"));
        assertEquals(Optional.empty(), reopened.get("dropped"));
        reopened.close();
    }

    // --- restart handling ---

    @Test
    void combinesFlushedTablesWithReplayedLogEntries() {
        LSMStoreEngine engine = new LSMStoreEngine(dir, 64);
        for (int i = 0; i < 40; i++) {
            engine.put("key_" + i, "value_" + i);
        }
        engine.close();

        // Some of those writes are in tables, the tail is still only in the log.
        LSMStoreEngine reopened = new LSMStoreEngine(dir, 64);
        for (int i = 0; i < 40; i++) {
            assertEquals(Optional.of("value_" + i), reopened.get("key_" + i),
                    "lost key_" + i);
        }
        reopened.close();
    }

    @Test
    void sequenceNumbersKeepClimbingAcrossRestarts() throws IOException {
        LSMStoreEngine first = new LSMStoreEngine(dir, FLUSH_EVERY_WRITE);
        first.put("a", "1");
        first.close();

        LSMStoreEngine second = new LSMStoreEngine(dir, FLUSH_EVERY_WRITE);
        second.put("b", "2");
        second.close();

        // A reused sequence would have overwritten the first table.
        assertEquals(2, tableCount());
        assertTrue(Files.exists(dir.resolve("L0_000000.sst")));
        assertTrue(Files.exists(dir.resolve("L0_000001.sst")));

        LSMStoreEngine third = new LSMStoreEngine(dir);
        assertEquals(Optional.of("1"), third.get("a"));
        assertEquals(Optional.of("2"), third.get("b"));
        third.close();
    }

    @Test
    void startupClearsTempFilesLeftByACrashedFlush() throws IOException {
        LSMStoreEngine engine = new LSMStoreEngine(dir, FLUSH_EVERY_WRITE);
        engine.put("key", "value");
        engine.close();

        Path orphan = dir.resolve("L0_000007.sst.tmp");
        Files.write(orphan, new byte[] {1, 2, 3});

        LSMStoreEngine reopened = new LSMStoreEngine(dir);
        assertFalse(Files.exists(orphan));
        assertEquals(Optional.of("value"), reopened.get("key"));
        reopened.close();
    }

    @Test
    void refusesToStartOnAnUnreadableTable() throws IOException {
        Files.write(dir.resolve("L0_000000.sst"), new byte[] {9, 9, 9});
        assertThrows(RuntimeException.class, () -> new LSMStoreEngine(dir));
    }
}
