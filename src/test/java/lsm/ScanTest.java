package lsm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/** Ordered range reads across the MemTable and every level. */
class ScanTest {

    @TempDir
    Path dir;

    private List<String> keys(LSMStoreEngine engine, String from, String to) {
        try (Stream<StorageEngine.Row> rows = engine.scan(from, to)) {
            return rows.map(StorageEngine.Row::key).toList();
        }
    }

    private List<String> values(LSMStoreEngine engine, String from, String to) {
        try (Stream<StorageEngine.Row> rows = engine.scan(from, to)) {
            return rows.map(StorageEngine.Row::value).toList();
        }
    }

    // --- bounds ---

    @Test
    void returnsTheRangeInOrder() {
        LSMStoreEngine engine = new LSMStoreEngine(dir, 10 * 1024 * 1024);
        try {
            for (int i = 0; i < 100; i++) {
                engine.put(String.format("key_%03d", i), "value_" + i);
            }
            assertEquals(List.of("key_010", "key_011", "key_012"),
                    keys(engine, "key_010", "key_013"));
        } finally {
            engine.close();
        }
    }

    @Test
    void theLowerBoundIsInclusiveAndTheUpperExclusive() {
        LSMStoreEngine engine = new LSMStoreEngine(dir, 10 * 1024 * 1024);
        try {
            engine.put("a", "1");
            engine.put("b", "2");
            engine.put("c", "3");
            assertEquals(List.of("a", "b"), keys(engine, "a", "c"));
        } finally {
            engine.close();
        }
    }

    @Test
    void nullBoundsMeanUnbounded() {
        LSMStoreEngine engine = new LSMStoreEngine(dir, 10 * 1024 * 1024);
        try {
            engine.put("a", "1");
            engine.put("b", "2");
            engine.put("c", "3");

            assertEquals(List.of("a", "b", "c"), keys(engine, null, null));
            assertEquals(List.of("b", "c"), keys(engine, "b", null));
            assertEquals(List.of("a", "b"), keys(engine, null, "c"));
        } finally {
            engine.close();
        }
    }

    @Test
    void anInvertedOrEmptyRangeYieldsNothing() {
        LSMStoreEngine engine = new LSMStoreEngine(dir, 10 * 1024 * 1024);
        try {
            engine.put("b", "2");
            assertTrue(keys(engine, "z", "a").isEmpty());
            assertTrue(keys(engine, "b", "b").isEmpty());
            assertTrue(keys(engine, "x", "y").isEmpty());
        } finally {
            engine.close();
        }
    }

    // --- correctness across the levels ---

    @Test
    void deletedKeysAreSkipped() {
        LSMStoreEngine engine = new LSMStoreEngine(dir, 10 * 1024 * 1024);
        try {
            for (int i = 0; i < 10; i++) {
                engine.put("key_" + i, "value_" + i);
            }
            engine.delete("key_3");
            engine.delete("key_7");

            List<String> found = keys(engine, null, null);
            assertEquals(8, found.size());
            assertFalse(found.contains("key_3"));
            assertFalse(found.contains("key_7"));
        } finally {
            engine.close();
        }
    }

    @Test
    void theNewestValueWinsAcrossMemoryAndDisk() throws IOException {
        // Small bound so early writes are flushed and later ones are not.
        LSMStoreEngine engine = new LSMStoreEngine(dir, 128);
        try {
            for (int i = 0; i < 200; i++) {
                engine.put(String.format("key_%03d", i), "old");
            }
            for (int i = 0; i < 200; i++) {
                engine.put(String.format("key_%03d", i), "new");
            }

            assertTrue(Files.list(dir).anyMatch(p -> p.toString().endsWith(".sst")),
                    "test needs data on disk as well as in memory");
            List<String> found = values(engine, null, null);
            assertEquals(200, found.size(), "each key should appear exactly once");
            assertTrue(found.stream().allMatch("new"::equals), "a stale value surfaced");
        } finally {
            engine.close();
        }
    }

    @Test
    void aDeleteInMemoryHidesAValueOnDisk() {
        LSMStoreEngine engine = new LSMStoreEngine(dir, 128);
        try {
            for (int i = 0; i < 200; i++) {
                engine.put(String.format("key_%03d", i), "value");
            }
            engine.delete("key_000");
            engine.delete("key_150");

            List<String> found = keys(engine, null, null);
            assertEquals(198, found.size());
            assertFalse(found.contains("key_000"));
            assertFalse(found.contains("key_150"));
        } finally {
            engine.close();
        }
    }

    @Test
    void scansAgreeWithPointLookupsOverALargeStore() {
        LSMStoreEngine engine = new LSMStoreEngine(dir, 2048);
        try {
            for (int i = 0; i < 3_000; i++) {
                engine.put(String.format("key_%05d", i), "value_" + i);
            }
            for (int i = 0; i < 3_000; i += 4) {
                engine.delete(String.format("key_%05d", i));
            }

            try (Stream<StorageEngine.Row> rows = engine.scan(null, null)) {
                long seen = rows.peek(row ->
                        assertEquals(java.util.Optional.of(row.value()), engine.get(row.key()),
                                "scan and get disagreed on " + row.key())).count();
                assertEquals(2_250, seen, "wrong number of surviving keys");
            }
        } finally {
            engine.close();
        }
    }

    @Test
    void scanningSurvivesCompactionRetiringTablesUnderneath() throws Exception {
        LSMStoreEngine engine = new LSMStoreEngine(dir, 1024);
        try {
            for (int i = 0; i < 4_000; i++) {
                engine.put(String.format("key_%05d", i), "value_" + i);
            }

            // Hold a scan open, driving it slowly, while writes keep compaction
            // busy retiring the very tables the scan is reading.
            try (Stream<StorageEngine.Row> rows = engine.scan(null, null)) {
                var iterator = rows.iterator();
                int seen = 0;
                while (iterator.hasNext()) {
                    iterator.next();
                    seen++;
                    if (seen % 400 == 0) {
                        for (int i = 0; i < 300; i++) {
                            engine.put(String.format("churn_%05d", i), "x".repeat(40));
                        }
                    }
                }
                assertTrue(seen >= 4_000, "scan lost rows to a compaction, saw " + seen);
            }
        } finally {
            engine.close();
        }
    }
}
