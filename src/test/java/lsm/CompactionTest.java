package lsm;

import lsm.compaction.Compactor;
import lsm.sstable.SSTableReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.*;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Compaction at the engine level.
 *
 * <p>Compaction runs on a background thread, so these wait for a condition
 * rather than sleeping a fixed amount: a fixed sleep is either flaky on a busy
 * machine or slow on an idle one.
 */
class CompactionTest {

    @TempDir
    Path dir;

    private static final long TINY_MEMTABLE = 2048;

    private List<String> tableNames() throws IOException {
        try (Stream<Path> files = Files.list(dir)) {
            return files.map(p -> p.getFileName().toString())
                    .filter(n -> n.endsWith(".sst"))
                    .sorted()
                    .toList();
        }
    }

    private long countAtLevel(int level) throws IOException {
        return tableNames().stream().filter(n -> n.startsWith("L" + level + "_")).count();
    }

    private long totalTableBytes() throws IOException {
        try (Stream<Path> files = Files.list(dir)) {
            return files.filter(p -> p.toString().endsWith(".sst"))
                    .mapToLong(p -> p.toFile().length()).sum();
        }
    }

    /** Polls until the condition holds, failing with a message if it never does. */
    private void eventually(String what, Callable<Boolean> condition) {
        assertTimeoutPreemptively(Duration.ofSeconds(60), () -> {
            while (!condition.call()) {
                Thread.sleep(20);
            }
        }, what);
    }

    /**
     * Waits until compaction has run and level 0 is back within its trigger.
     *
     * <p>Level 0 emptying completely is not the steady state: compaction only
     * fires at {@link Compactor#L0_TRIGGER} files, so a few are expected to sit
     * there afterwards. Waiting for zero would hang on correct behaviour.
     */
    private void compacted() {
        eventually("compaction should run and settle",
                () -> countAtLevel(1) > 0 && countAtLevel(0) < Compactor.L0_TRIGGER);
    }

    // --- it happens at all ---

    @Test
    void levelZeroGetsDrainedIntoLevelOne() throws IOException {
        LSMStoreEngine engine = new LSMStoreEngine(dir, TINY_MEMTABLE);
        try {
            for (int i = 0; i < 2_000; i++) {
                engine.put(String.format("key_%05d", i), "value_" + i);
            }
            compacted();
            assertTrue(countAtLevel(1) > 0, "the data has to have gone somewhere");
            assertTrue(countAtLevel(0) < Compactor.L0_TRIGGER,
                    "level 0 should be back under its trigger");
        } finally {
            engine.close();
        }
    }

    @Test
    void levelZeroStaysUnderItsTriggerCount() {
        LSMStoreEngine engine = new LSMStoreEngine(dir, TINY_MEMTABLE);
        try {
            for (int i = 0; i < 3_000; i++) {
                engine.put(String.format("key_%05d", i), "value_" + i);
            }
            eventually("level 0 should settle below its trigger",
                    () -> countAtLevel(0) < Compactor.L0_TRIGGER);
        } finally {
            engine.close();
        }
    }

    // --- nothing is lost ---

    @Test
    void everyKeySurvivesCompaction() {
        LSMStoreEngine engine = new LSMStoreEngine(dir, TINY_MEMTABLE);
        try {
            for (int i = 0; i < 2_000; i++) {
                engine.put(String.format("key_%05d", i), "value_" + i);
            }
            compacted();

            for (int i = 0; i < 2_000; i++) {
                assertEquals(Optional.of("value_" + i), engine.get(String.format("key_%05d", i)),
                        "compaction lost key_" + String.format("%05d", i));
            }
        } finally {
            engine.close();
        }
    }

    @Test
    void theNewestValueWinsAfterCompaction() {
        LSMStoreEngine engine = new LSMStoreEngine(dir, TINY_MEMTABLE);
        try {
            for (int round = 0; round < 8; round++) {
                for (int i = 0; i < 200; i++) {
                    engine.put(String.format("key_%03d", i), "round_" + round);
                }
            }
            compacted();

            for (int i = 0; i < 200; i++) {
                assertEquals(Optional.of("round_7"), engine.get(String.format("key_%03d", i)),
                        "an older version resurfaced for key_" + i);
            }
        } finally {
            engine.close();
        }
    }

    @Test
    void compactedDataSurvivesAReopen() {
        LSMStoreEngine engine = new LSMStoreEngine(dir, TINY_MEMTABLE);
        try {
            for (int i = 0; i < 2_000; i++) {
                engine.put(String.format("key_%05d", i), "value_" + i);
            }
            compacted();
        } finally {
            engine.close();
        }

        LSMStoreEngine reopened = new LSMStoreEngine(dir, TINY_MEMTABLE);
        try {
            for (int i = 0; i < 2_000; i++) {
                assertEquals(Optional.of("value_" + i),
                        reopened.get(String.format("key_%05d", i)), "lost key_" + i);
            }
        } finally {
            reopened.close();
        }
    }

    // --- deletes actually reclaim space ---

    @Test
    void deletedDataIsEventuallyReclaimed() throws IOException {
        LSMStoreEngine engine = new LSMStoreEngine(dir, TINY_MEMTABLE);
        try {
            for (int i = 0; i < 3_000; i++) {
                engine.put(String.format("key_%05d", i), "value_" + i);
            }
            compacted();
            long before = totalTableBytes();

            for (int i = 0; i < 2_900; i++) {
                engine.delete(String.format("key_%05d", i));
            }
            eventually("bytes on disk should fall once tombstones are dropped",
                    () -> totalTableBytes() < before / 2);

            for (int i = 0; i < 2_900; i++) {
                assertEquals(Optional.empty(), engine.get(String.format("key_%05d", i)),
                        "a deleted key came back");
            }
            for (int i = 2_900; i < 3_000; i++) {
                assertEquals(Optional.of("value_" + i), engine.get(String.format("key_%05d", i)),
                        "a live key was reclaimed");
            }
        } finally {
            engine.close();
        }
    }

    @Test
    void aTombstoneIsNotDroppedWhileALowerLevelStillHoldsTheValue() {
        // The trap: discard a tombstone during an upper-level merge while an
        // older table below still has the value, and the delete undoes itself.
        LSMStoreEngine engine = new LSMStoreEngine(dir, TINY_MEMTABLE);
        try {
            for (int i = 0; i < 4_000; i++) {
                engine.put(String.format("key_%05d", i), "value_" + i);
            }
            compacted();

            for (int i = 0; i < 4_000; i += 2) {
                engine.delete(String.format("key_%05d", i));
            }
            compacted();

            for (int i = 0; i < 4_000; i++) {
                Optional<String> found = engine.get(String.format("key_%05d", i));
                if (i % 2 == 0) {
                    assertEquals(Optional.empty(), found, "key_" + i + " was resurrected");
                } else {
                    assertEquals(Optional.of("value_" + i), found, "key_" + i + " was lost");
                }
            }
        } finally {
            engine.close();
        }
    }

    // --- the tombstone-dropping decision itself ---

    /** Writes a small table straight into a named level, bypassing the engine. */
    private SSTableReader tableAt(int level, int sequence, String... keys) throws IOException {
        Path path = dir.resolve(String.format("L%d_%06d.sst", level, sequence));
        java.util.TreeMap<String, lsm.memtable.Entry> entries = new java.util.TreeMap<>();
        for (String key : keys) {
            entries.put(key, lsm.memtable.Entry.put("value"));
        }
        lsm.sstable.SSTableWriter.write(path, entries);
        return new SSTableReader(path);
    }

    /**
     * The end-to-end delete tests cannot see this distinction: with only two
     * levels in play, every compaction is into the bottom level and dropping is
     * always correct. These drive the decision directly so a regression that
     * always drops actually fails something.
     */
    @Test
    void tombstonesAreKeptWhenADeeperLevelCouldHoldTheValue() throws IOException {
        List<SSTableReader> tables = new ArrayList<>();
        try {
            for (int i = 0; i < Compactor.L0_TRIGGER; i++) {
                tables.add(tableAt(0, i, "key_a", "key_b"));
            }
            tables.add(tableAt(1, 100, "key_a", "key_b"));
            tables.add(tableAt(2, 200, "key_a", "key_b"));

            Compactor compactor = new Compactor(dir, null, () -> 900);
            Compactor.Job job = compactor.choose(tables);

            assertNotNull(job, "four level 0 tables should trigger a compaction");
            assertEquals(1, job.outputLevel());
            assertFalse(job.mayDropTombstones(),
                    "level 2 sits below the output, so a dropped tombstone would "
                            + "let the value underneath it come back");
        } finally {
            tables.forEach(SSTableReader::close);
        }
    }

    @Test
    void tombstonesAreDroppedWhenNothingSitsBelow() throws IOException {
        List<SSTableReader> tables = new ArrayList<>();
        try {
            for (int i = 0; i < Compactor.L0_TRIGGER; i++) {
                tables.add(tableAt(0, i, "key_a", "key_b"));
            }
            tables.add(tableAt(1, 100, "key_a", "key_b"));

            Compactor compactor = new Compactor(dir, null, () -> 900);
            Compactor.Job job = compactor.choose(tables);

            assertNotNull(job);
            assertEquals(1, job.outputLevel());
            assertTrue(job.mayDropTombstones(),
                    "nothing below level 1, so the tombstones are free to go");
        } finally {
            tables.forEach(SSTableReader::close);
        }
    }

    @Test
    void anOverlappingLowerTableIsPulledIntoTheMerge() throws IOException {
        List<SSTableReader> tables = new ArrayList<>();
        try {
            for (int i = 0; i < Compactor.L0_TRIGGER; i++) {
                tables.add(tableAt(0, i, "key_m"));
            }
            SSTableReader overlapping = tableAt(1, 100, "key_a", "key_z");
            SSTableReader disjoint = tableAt(1, 101, "zzz_1", "zzz_2");
            tables.add(overlapping);
            tables.add(disjoint);

            Compactor compactor = new Compactor(dir, null, () -> 900);
            Compactor.Job job = compactor.choose(tables);

            assertNotNull(job);
            assertTrue(job.inputs().contains(overlapping),
                    "a level 1 table covering the key range has to be rewritten too, "
                            + "or the level stops being disjoint");
            assertFalse(job.inputs().contains(disjoint),
                    "a table outside the range is untouched work");
        } finally {
            tables.forEach(SSTableReader::close);
        }
    }

    // --- levels keep their shape ---

    @Test
    void tablesBelowLevelZeroDoNotOverlap() throws IOException {
        LSMStoreEngine engine = new LSMStoreEngine(dir, TINY_MEMTABLE);
        try {
            for (int i = 0; i < 4_000; i++) {
                engine.put(String.format("key_%05d", i), "value_" + i);
            }
            compacted();
        } finally {
            engine.close();
        }

        // Disjoint ranges per level are what bound a lookup to one table per
        // level; overlapping tables would quietly make reads O(files) again.
        Map<Integer, List<SSTableReader>> byLevel = new HashMap<>();
        List<SSTableReader> opened = new ArrayList<>();
        try {
            for (String name : tableNames()) {
                SSTableReader reader = new SSTableReader(dir.resolve(name));
                opened.add(reader);
                byLevel.computeIfAbsent(reader.level(), k -> new ArrayList<>()).add(reader);
            }
            for (var entry : byLevel.entrySet()) {
                if (entry.getKey() == 0) {
                    continue;
                }
                List<SSTableReader> level = entry.getValue();
                level.sort((a, b) -> a.minKey().compareTo(b.minKey()));
                for (int i = 1; i < level.size(); i++) {
                    assertTrue(level.get(i - 1).maxKey().compareTo(level.get(i).minKey()) < 0,
                            "level " + entry.getKey() + " tables overlap: "
                                    + level.get(i - 1).path() + " and " + level.get(i).path());
                }
            }
        } finally {
            opened.forEach(SSTableReader::close);
        }
    }

    @Test
    void compactionLeavesNoTempFilesBehind() throws IOException {
        LSMStoreEngine engine = new LSMStoreEngine(dir, TINY_MEMTABLE);
        try {
            for (int i = 0; i < 3_000; i++) {
                engine.put(String.format("key_%05d", i), "value_" + i);
            }
            compacted();
        } finally {
            engine.close();
        }

        try (Stream<Path> files = Files.list(dir)) {
            assertTrue(files.noneMatch(p -> p.toString().endsWith(".tmp")));
        }
    }

    // --- it is safe to read while it runs ---

    @Test
    void readsStaySaneWhileCompactionReplacesTablesUnderneath() throws Exception {
        LSMStoreEngine engine = new LSMStoreEngine(dir, TINY_MEMTABLE);
        ExecutorService pool = Executors.newFixedThreadPool(4);
        try {
            for (int i = 0; i < 500; i++) {
                engine.put(String.format("stable_%04d", i), "value_" + i);
            }

            CountDownLatch gate = new CountDownLatch(1);
            List<Future<?>> readers = new ArrayList<>();
            for (int r = 0; r < 4; r++) {
                readers.add(pool.submit(() -> {
                    gate.await();
                    for (int round = 0; round < 60; round++) {
                        for (int i = 0; i < 500; i++) {
                            assertEquals(Optional.of("value_" + i),
                                    engine.get(String.format("stable_%04d", i)),
                                    "stable_" + i + " vanished mid-compaction");
                        }
                    }
                    return null;
                }));
            }

            gate.countDown();
            // Churn hard enough that compaction retires tables out from under
            // the readers while they are part-way through a scan.
            for (int i = 0; i < 4_000; i++) {
                engine.put(String.format("churn_%05d", i), "x".repeat(30));
            }
            for (Future<?> reader : readers) {
                reader.get(120, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
            engine.close();
        }
    }

    // --- randomised ---

    @Test
    void agreesWithAReferenceMapThroughManyCompactions() {
        Map<String, String> expected = new HashMap<>();
        Random random = new Random(20260922L);
        LSMStoreEngine engine = new LSMStoreEngine(dir, TINY_MEMTABLE);

        try {
            for (int op = 0; op < 8_000; op++) {
                String key = String.format("key_%04d", random.nextInt(1_200));
                if (random.nextDouble() < 0.3) {
                    engine.delete(key);
                    expected.remove(key);
                } else {
                    String value = "v" + random.nextInt(1_000_000);
                    engine.put(key, value);
                    expected.put(key, value);
                }
            }
            compacted();

            for (int i = 0; i < 1_200; i++) {
                String key = String.format("key_%04d", i);
                assertEquals(Optional.ofNullable(expected.get(key)), engine.get(key),
                        "disagreement on " + key);
            }
        } finally {
            engine.close();
        }
    }
}
