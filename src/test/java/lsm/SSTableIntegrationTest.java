package lsm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.*;
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

    // --- concurrency ---

    @Test
    void concurrentReadsAgainstTablesAgree() throws Exception {
        LSMStoreEngine engine = new LSMStoreEngine(dir, 512);
        for (int i = 0; i < 400; i++) {
            engine.put("key_" + i, "value_" + i);
        }

        ExecutorService pool = Executors.newFixedThreadPool(8);
        try {
            CountDownLatch start = new CountDownLatch(1);
            Callable<Void> reader = () -> {
                start.await();
                for (int round = 0; round < 20; round++) {
                    for (int i = 0; i < 400; i++) {
                        assertEquals(Optional.of("value_" + i), engine.get("key_" + i));
                    }
                }
                return null;
            };

            // submit rather than invokeAll: invokeAll blocks until every task
            // finishes, so the latch below would never be released.
            var futures = new java.util.ArrayList<Future<Void>>();
            for (int i = 0; i < 8; i++) {
                futures.add(pool.submit(reader));
            }
            start.countDown();
            for (Future<Void> future : futures) {
                future.get(60, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
            engine.close();
        }
    }

    // --- scale and randomised behaviour ---

    @Test
    void servesManyKeysSpreadOverManyTables() {
        // Kept modest on purpose: every write costs an fsync in the WAL, which
        // bounds this at a few hundred writes a second. Larger runs belong in a
        // benchmark, not the unit suite.
        LSMStoreEngine engine = new LSMStoreEngine(dir, 4 * 1024);
        for (int i = 0; i < 2_000; i++) {
            engine.put("key_" + i, "value_" + i);
        }
        engine.close();

        LSMStoreEngine reopened = new LSMStoreEngine(dir);
        for (int i = 0; i < 2_000; i++) {
            assertEquals(Optional.of("value_" + i), reopened.get("key_" + i),
                    "lost key_" + i);
        }
        reopened.close();
    }

    @Test
    void matchesAReferenceMapUnderRandomisedRestarts() {
        Map<String, String> expected = new HashMap<>();
        Random random = new Random(20250920L);

        for (int session = 0; session < 4; session++) {
            LSMStoreEngine engine = new LSMStoreEngine(dir, 512);
            for (int op = 0; op < 150; op++) {
                String key = "key_" + random.nextInt(150);
                if (random.nextDouble() < 0.3) {
                    engine.delete(key);
                    expected.remove(key);
                } else {
                    String value = "value_" + random.nextInt(1_000_000);
                    engine.put(key, value);
                    expected.put(key, value);
                }
            }
            engine.close();
        }

        LSMStoreEngine engine = new LSMStoreEngine(dir);
        for (int i = 0; i < 150; i++) {
            String key = "key_" + i;
            assertEquals(Optional.ofNullable(expected.get(key)), engine.get(key),
                    "disagreement on " + key);
        }
        engine.close();
    }
}
