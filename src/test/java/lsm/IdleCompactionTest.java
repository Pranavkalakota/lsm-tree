package lsm;

import lsm.compaction.Compactor;
import lsm.sstable.SSTableReader;
import lsm.sstable.SSTableWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Reclaiming space once writing stops.
 *
 * <p>Compaction normally waits for four files in level 0, which is the right
 * trade while a store is busy. It leaves a quiet store holding tombstones
 * forever, and the values they shadow with them, so an idle store relaxes that
 * trigger.
 */
class IdleCompactionTest {

    @TempDir
    Path dir;

    private long countAtLevel(int level) throws IOException {
        try (Stream<Path> files = Files.list(dir)) {
            return files.map(p -> p.getFileName().toString())
                    .filter(n -> n.startsWith("L" + level + "_")).count();
        }
    }

    private long tableBytes() throws IOException {
        try (Stream<Path> files = Files.list(dir)) {
            return files.filter(p -> p.toString().endsWith(".sst"))
                    .mapToLong(p -> p.toFile().length()).sum();
        }
    }

    private void eventually(String what, Callable<Boolean> condition) {
        assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
            while (!condition.call()) {
                Thread.sleep(20);
            }
        }, what);
    }

    // --- the decision itself ---

    private SSTableReader tableAt(int level, int sequence, String... keys) throws IOException {
        Path path = dir.resolve(String.format("L%d_%06d.sst", level, sequence));
        TreeMap<String, lsm.memtable.Entry> entries = new TreeMap<>();
        for (String key : keys) {
            entries.put(key, lsm.memtable.Entry.put("value"));
        }
        SSTableWriter.write(path, entries);
        return new SSTableReader(path);
    }

    @Test
    void oneFileIsLeftAloneWhileTheStoreIsBusy() throws IOException {
        List<SSTableReader> tables = new ArrayList<>();
        try {
            tables.add(tableAt(0, 1, "key_a"));
            Compactor compactor = new Compactor(dir, null, () -> 900);

            // A busy store would rather spend its write bandwidth on writes.
            assertNull(compactor.choose(tables, false),
                    "one file should not be worth a compaction while writing");
        } finally {
            tables.forEach(SSTableReader::close);
        }
    }

    @Test
    void oneFileIsCompactedOnceTheStoreIsIdle() throws IOException {
        List<SSTableReader> tables = new ArrayList<>();
        try {
            SSTableReader lone = tableAt(0, 1, "key_a");
            tables.add(lone);
            Compactor compactor = new Compactor(dir, null, () -> 900);

            Compactor.Job job = compactor.choose(tables, true);
            assertNotNull(job, "an idle store should clear level 0");
            assertEquals(1, job.outputLevel());
            assertTrue(job.inputs().contains(lone));
        } finally {
            tables.forEach(SSTableReader::close);
        }
    }

    @Test
    void anEmptyLevelZeroStillHasNothingToDo() throws IOException {
        List<SSTableReader> tables = new ArrayList<>();
        try {
            tables.add(tableAt(1, 100, "key_a"));
            Compactor compactor = new Compactor(dir, null, () -> 900);
            assertNull(compactor.choose(tables, true),
                    "idle is not a reason to rewrite a level that is already settled");
        } finally {
            tables.forEach(SSTableReader::close);
        }
    }

    // --- end to end ---

    @Test
    void levelZeroDrainsWithoutAnyFurtherWrites() throws IOException {
        LSMStoreEngine engine = new LSMStoreEngine(dir, 1024);
        try {
            for (int i = 0; i < 400; i++) {
                engine.put(String.format("key_%04d", i), "value_" + i);
            }
            eventually("some files should land in level 0", () -> countAtLevel(0) > 0);

            // Nothing writes from here on; the trigger has to come from idleness.
            engine.compactIdleNow();
            eventually("level 0 should drain once idle", () -> countAtLevel(0) == 0);
            assertTrue(countAtLevel(1) > 0, "the data has to have gone somewhere");
        } finally {
            engine.close();
        }
    }

    @Test
    void deletesAreReclaimedWithoutAnyFurtherWrites() throws IOException {
        LSMStoreEngine engine = new LSMStoreEngine(dir, 1024);
        try {
            for (int i = 0; i < 2_000; i++) {
                engine.put(String.format("key_%05d", i), "value_" + i);
            }
            engine.compactIdleNow();
            eventually("initial compaction", () -> countAtLevel(0) == 0);
            long before = tableBytes();

            for (int i = 0; i < 1_900; i++) {
                engine.delete(String.format("key_%05d", i));
            }
            // This is the case that used to stall: deletes land in a level 0
            // that never reaches four files, and nothing else is coming.
            engine.compactIdleNow();
            eventually("space should be reclaimed while idle",
                    () -> tableBytes() < before);

            for (int i = 0; i < 1_900; i++) {
                assertEquals(Optional.empty(), engine.get(String.format("key_%05d", i)),
                        "a deleted key came back");
            }
            for (int i = 1_900; i < 2_000; i++) {
                assertEquals(Optional.of("value_" + i),
                        engine.get(String.format("key_%05d", i)), "a live key was lost");
            }
        } finally {
            engine.close();
        }
    }

    @Test
    void idleCompactionLosesNothing() {
        LSMStoreEngine engine = new LSMStoreEngine(dir, 512);
        try {
            for (int i = 0; i < 1_000; i++) {
                engine.put(String.format("key_%04d", i), "value_" + i);
            }
            engine.compactIdleNow();
            eventually("compaction should settle", () -> countAtLevel(0) == 0);

            for (int i = 0; i < 1_000; i++) {
                assertEquals(Optional.of("value_" + i),
                        engine.get(String.format("key_%04d", i)), "lost key_" + i);
            }
        } finally {
            engine.close();
        }
    }

    @Test
    void theTimerFiresOnItsOwn() {
        // The one case that waits on the real clock rather than poking the
        // engine, so the scheduling itself is covered and not just the logic.
        // The quiet period is shortened here; the real one is half a minute.
        LSMStoreEngine engine = new LSMStoreEngine(
                dir, 1024, lsm.wal.DurabilityMode.BUFFERED, 8, 16 * 1024 * 1024, 300);
        try {
            for (int i = 0; i < 400; i++) {
                engine.put(String.format("key_%04d", i), "value_" + i);
            }
            eventually("some files should land in level 0", () -> countAtLevel(0) > 0);
            eventually("the idle timer should drain level 0 unprompted",
                    () -> countAtLevel(0) == 0);
        } finally {
            engine.close();
        }
    }

    @Test
    void closingDoesNotWaitForTheIdleTimer() {
        LSMStoreEngine engine = new LSMStoreEngine(dir, 1024);
        engine.put("key", "value");
        // A daemon timer that outlived close would keep the store's files open.
        assertTimeoutPreemptively(Duration.ofSeconds(30), engine::close);
    }
}
