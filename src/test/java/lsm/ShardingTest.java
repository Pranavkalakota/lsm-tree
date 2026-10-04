package lsm;

import lsm.memtable.MemTable;
import lsm.wal.DurabilityMode;
import lsm.wal.WriteAheadLog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The striped write path.
 *
 * <p>Sharding is a performance change, so the thing to pin down is that it
 * changes nothing a caller can observe: the same keys, the same values, the
 * same recovery, whatever the shard count happens to be.
 */
class ShardingTest {

    @TempDir
    Path dir;

    private long logCount() throws IOException {
        try (Stream<Path> files = Files.list(dir)) {
            return files.filter(p -> p.getFileName().toString().startsWith("wal_")).count();
        }
    }

    // --- correctness is independent of shard count ---

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3, 8, 16})
    void everyShardCountStoresAndReadsTheSame(int shards) {
        LSMStoreEngine engine = new LSMStoreEngine(
                dir, 4 * 1024 * 1024, DurabilityMode.BUFFERED, shards);
        try {
            for (int i = 0; i < 500; i++) {
                engine.put("key_" + i, "value_" + i);
            }
            for (int i = 0; i < 500; i += 3) {
                engine.delete("key_" + i);
            }
            for (int i = 0; i < 500; i++) {
                Optional<String> expected =
                        i % 3 == 0 ? Optional.empty() : Optional.of("value_" + i);
                assertEquals(expected, engine.get("key_" + i), "key_" + i);
            }
        } finally {
            engine.close();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3, 8, 16})
    void everyShardCountScansInOneSortedOrder(int shards) {
        LSMStoreEngine engine = new LSMStoreEngine(
                dir, 4 * 1024 * 1024, DurabilityMode.BUFFERED, shards);
        try {
            for (int i = 0; i < 300; i++) {
                engine.put(String.format("key_%03d", i), "value_" + i);
            }
            // Keys scatter across shards, so a scan has to merge all of them
            // back into one ascending run.
            try (Stream<StorageEngine.Row> rows = engine.scan(null, null)) {
                String previous = null;
                int seen = 0;
                for (StorageEngine.Row row : rows.toList()) {
                    if (previous != null) {
                        assertTrue(row.key().compareTo(previous) > 0,
                                "scan out of order at " + row.key());
                    }
                    previous = row.key();
                    seen++;
                }
                assertEquals(300, seen);
            }
        } finally {
            engine.close();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 4, 8})
    void dataSurvivesAReopenAtTheSameShardCount(int shards) {
        LSMStoreEngine engine = new LSMStoreEngine(
                dir, 1024, DurabilityMode.BUFFERED, shards);
        try {
            for (int i = 0; i < 400; i++) {
                engine.put("key_" + i, "value_" + i);
            }
        } finally {
            engine.close();
        }

        LSMStoreEngine reopened = new LSMStoreEngine(
                dir, 1024, DurabilityMode.BUFFERED, shards);
        try {
            for (int i = 0; i < 400; i++) {
                assertEquals(Optional.of("value_" + i), reopened.get("key_" + i),
                        "lost key_" + i);
            }
        } finally {
            reopened.close();
        }
    }

    // --- the logs themselves ---

    @Test
    void eachShardKeepsItsOwnLog() throws IOException {
        LSMStoreEngine engine = new LSMStoreEngine(
                dir, 4 * 1024 * 1024, DurabilityMode.BUFFERED, 4);
        try {
            engine.put("key", "value");
            assertEquals(4, logCount(), "one log per shard");
        } finally {
            engine.close();
        }
    }

    @Test
    void syncModeStaysUnshardedByDefault() throws IOException {
        // Splitting the log splits what group commit batches, so SYNC keeps one.
        LSMStoreEngine engine = new LSMStoreEngine(dir, 4 * 1024 * 1024, DurabilityMode.SYNC);
        try {
            engine.put("key", "value");
            assertEquals(1, logCount());
        } finally {
            engine.close();
        }
    }

    @Test
    void bufferedModeShardsByDefault() throws IOException {
        LSMStoreEngine engine = new LSMStoreEngine(dir, 4 * 1024 * 1024, DurabilityMode.BUFFERED);
        try {
            engine.put("key", "value");
            assertTrue(logCount() > 1, "buffered writes should be striped");
        } finally {
            engine.close();
        }
    }

    // --- opening a store written before the log was sharded ---

    @Test
    void aStoreWithOneOldLogIsRecovered() throws IOException {
        // Exactly what an earlier version of the engine left behind.
        try (WriteAheadLog legacy = new WriteAheadLog(dir.resolve("wal.log"))) {
            for (int i = 0; i < 200; i++) {
                legacy.appendPut("key_" + i, "value_" + i);
            }
            legacy.appendDelete("key_7");
        }

        LSMStoreEngine engine = new LSMStoreEngine(
                dir, 4 * 1024 * 1024, DurabilityMode.BUFFERED, 8);
        try {
            for (int i = 0; i < 200; i++) {
                Optional<String> expected =
                        i == 7 ? Optional.empty() : Optional.of("value_" + i);
                assertEquals(expected, engine.get("key_" + i), "key_" + i);
            }
        } finally {
            engine.close();
        }

        assertFalse(Files.exists(dir.resolve("wal.log")),
                "the old log should be gone once its records are spread across shards");
    }

    @Test
    void theOldLogIsOnlyRemovedAfterTheShardLogsHaveIt() throws IOException {
        try (WriteAheadLog legacy = new WriteAheadLog(dir.resolve("wal.log"))) {
            legacy.appendPut("survivor", "value");
        }

        LSMStoreEngine engine = new LSMStoreEngine(
                dir, 4 * 1024 * 1024, DurabilityMode.BUFFERED, 8);
        engine.close();

        // Reopening finds it in a shard log, not the one that was deleted.
        LSMStoreEngine reopened = new LSMStoreEngine(
                dir, 4 * 1024 * 1024, DurabilityMode.BUFFERED, 8);
        try {
            assertEquals(Optional.of("value"), reopened.get("survivor"));
        } finally {
            reopened.close();
        }
    }

    // --- routing ---

    @Test
    void aKeyAlwaysLandsInTheSameShard() {
        // Not directly observable, so checked by behaviour: a key overwritten
        // many times must never end up split across two shards' MemTables.
        LSMStoreEngine engine = new LSMStoreEngine(
                dir, 4 * 1024 * 1024, DurabilityMode.BUFFERED, 8);
        try {
            for (int round = 0; round < 50; round++) {
                engine.put("stable", "round_" + round);
                assertEquals(Optional.of("round_" + round), engine.get("stable"));
            }
        } finally {
            engine.close();
        }
    }

    @Test
    void writesSpreadAcrossShardsRatherThanPilingOnOne() throws IOException {
        // Short similar keys are exactly what String.hashCode clusters on, so
        // this is the case the hash spreading exists for. The MemTable bound is
        // large on purpose: nothing should flush, so every write stays visible
        // in its shard's log and the spread can actually be counted.
        LSMStoreEngine engine = new LSMStoreEngine(
                dir, 64 * 1024 * 1024, DurabilityMode.BUFFERED, 8);
        try {
            for (int i = 0; i < 2_000; i++) {
                engine.put("k" + i, "v");
            }

            long[] sizes;
            try (Stream<Path> files = Files.list(dir)) {
                sizes = files.filter(p -> p.getFileName().toString().startsWith("wal_"))
                        .mapToLong(p -> p.toFile().length()).sorted().toArray();
            }
            assertEquals(8, sizes.length);
            assertTrue(sizes[0] > 0, "every shard should have taken some of the load");

            // Perfect balance is not the claim; not collapsing onto one shard is.
            // A hash that clustered would leave the largest many times the smallest.
            assertTrue(sizes[7] < sizes[0] * 2,
                    "load is lopsided: smallest log " + sizes[0]
                            + " bytes, largest " + sizes[7]);
        } finally {
            engine.close();
        }
    }

    @Test
    void mixedOperationsMatchAReferenceMapUnderSharding() {
        Map<String, String> expected = new HashMap<>();
        Random random = new Random(20261003L);
        LSMStoreEngine engine = new LSMStoreEngine(dir, 512, DurabilityMode.BUFFERED, 8);
        try {
            for (int op = 0; op < 4_000; op++) {
                String key = "key_" + random.nextInt(400);
                if (random.nextDouble() < 0.3) {
                    engine.delete(key);
                    expected.remove(key);
                } else {
                    String value = "v" + random.nextInt(100_000);
                    engine.put(key, value);
                    expected.put(key, value);
                }
            }
            for (int i = 0; i < 400; i++) {
                String key = "key_" + i;
                assertEquals(Optional.ofNullable(expected.get(key)), engine.get(key), key);
            }
        } finally {
            engine.close();
        }
    }
}
