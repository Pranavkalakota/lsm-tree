package lsm.wal;

import lsm.LSMStoreEngine;
import lsm.memtable.MemTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Durability modes and group commit.
 *
 * <p>Whether an fsync physically happened is not observable from inside the
 * JVM, so nothing here asserts it directly. What is observable, and what these
 * cover, is that both modes record and replay identically, that group commit
 * genuinely batches rather than serializing, and that the machinery around the
 * commit point never strands a waiting writer.
 */
class DurabilityTest {

    @TempDir
    Path dir;

    // --- both modes record the same thing ---

    @ParameterizedTest
    @EnumSource(DurabilityMode.class)
    void everyModeReplaysWhatItRecorded(DurabilityMode mode) throws IOException {
        Path log = dir.resolve("wal.log");
        try (WriteAheadLog wal = new WriteAheadLog(log, mode)) {
            wal.syncTo(wal.appendPut("alpha", "one"));
            wal.syncTo(wal.appendPut("beta", "two"));
            wal.syncTo(wal.appendDelete("alpha"));
        }

        MemTable replayed = new MemTable(1024 * 1024);
        WriteAheadLog.replay(log, replayed);

        assertTrue(replayed.get("alpha").isTombstone());
        assertEquals("two", replayed.get("beta").value().orElseThrow());
    }

    @ParameterizedTest
    @EnumSource(DurabilityMode.class)
    void everyModeSurvivesAnEngineReopen(DurabilityMode mode) {
        LSMStoreEngine engine = new LSMStoreEngine(dir, 4 * 1024 * 1024, mode);
        engine.put("kept", "value");
        engine.put("dropped", "value");
        engine.delete("dropped");
        engine.close();

        LSMStoreEngine reopened = new LSMStoreEngine(dir, 4 * 1024 * 1024, mode);
        assertEquals(Optional.of("value"), reopened.get("kept"));
        assertEquals(Optional.empty(), reopened.get("dropped"));
        reopened.close();
    }

    @ParameterizedTest
    @EnumSource(DurabilityMode.class)
    void everyModeSurvivesAReopenAfterFlushing(DurabilityMode mode) {
        LSMStoreEngine engine = new LSMStoreEngine(dir, 256, mode);
        for (int i = 0; i < 200; i++) {
            engine.put("key_" + i, "value_" + i);
        }
        engine.close();

        LSMStoreEngine reopened = new LSMStoreEngine(dir, 256, mode);
        for (int i = 0; i < 200; i++) {
            assertEquals(Optional.of("value_" + i), reopened.get("key_" + i), "lost key_" + i);
        }
        reopened.close();
    }

    @Test
    void bufferedIsTheDefault() throws IOException {
        try (WriteAheadLog wal = new WriteAheadLog(dir.resolve("wal.log"))) {
            assertEquals(DurabilityMode.BUFFERED, wal.mode());
        }
    }

    // --- the commit point itself ---

    @ParameterizedTest
    @EnumSource(DurabilityMode.class)
    void committingAnAlreadyCommittedRecordReturnsImmediately(DurabilityMode mode)
            throws IOException {
        try (WriteAheadLog wal = new WriteAheadLog(dir.resolve("wal.log"), mode)) {
            long seq = wal.appendPut("key", "value");
            wal.syncTo(seq);
            wal.syncTo(seq);
            wal.syncTo(seq - 1);
            assertDoesNotThrow(() -> wal.syncTo(seq));
        }
    }

    @Test
    void resetReleasesAWriterWaitingOnAnOlderRecord() throws IOException {
        try (WriteAheadLog wal = new WriteAheadLog(dir.resolve("wal.log"), DurabilityMode.SYNC)) {
            long seq = wal.appendPut("key", "value");
            wal.reset();
            // The flush that prompted the reset already made this durable, so
            // waiting on it must not block against the channel that just went away.
            assertTimeoutPreemptively(java.time.Duration.ofSeconds(5), () -> wal.syncTo(seq));
        }
    }

    @Test
    void closingReleasesAWriterWaitingOnAnOlderRecord() throws IOException {
        WriteAheadLog wal = new WriteAheadLog(dir.resolve("wal.log"), DurabilityMode.SYNC);
        long seq = wal.appendPut("key", "value");
        wal.close();
        assertTimeoutPreemptively(java.time.Duration.ofSeconds(5), () -> wal.syncTo(seq));
    }

    // --- group commit ---

    @Test
    void concurrentSyncWritesAreAllDurable() throws Exception {
        int threads = 8;
        int perThread = 60;
        LSMStoreEngine engine = new LSMStoreEngine(dir, 4 * 1024 * 1024, DurabilityMode.SYNC);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        AtomicInteger next = new AtomicInteger();

        try {
            CountDownLatch gate = new CountDownLatch(1);
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                futures.add(pool.submit(() -> {
                    int id = next.getAndIncrement();
                    gate.await();
                    for (int i = 0; i < perThread; i++) {
                        engine.put("t" + id + "_k" + i, "v" + i);
                    }
                    return null;
                }));
            }
            gate.countDown();
            for (Future<?> future : futures) {
                future.get(120, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
            engine.close();
        }

        LSMStoreEngine reopened = new LSMStoreEngine(dir, 4 * 1024 * 1024, DurabilityMode.SYNC);
        try {
            for (int id = 0; id < threads; id++) {
                for (int i = 0; i < perThread; i++) {
                    assertEquals(Optional.of("v" + i), reopened.get("t" + id + "_k" + i),
                            "thread " + id + " lost write " + i);
                }
            }
        } finally {
            reopened.close();
        }
    }

    @Test
    void concurrentWritersShareFsyncsRatherThanQueueingForThem() throws Exception {
        int threads = 8;
        int perThread = 50;
        int total = threads * perThread;

        // One fsync was measured at roughly 3.8ms. Without batching this run
        // costs at least total * 3.8ms; the bound below sits far under that, so
        // passing it means writers really did share commits. It is deliberately
        // loose, because a timing assertion that is tight is a flaky one.
        long serialisedFloorMillis = (long) (total * 3.8);
        long budget = serialisedFloorMillis / 2;

        LSMStoreEngine engine = new LSMStoreEngine(dir, 8 * 1024 * 1024, DurabilityMode.SYNC);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        AtomicInteger next = new AtomicInteger();

        try {
            CountDownLatch ready = new CountDownLatch(threads);
            CountDownLatch gate = new CountDownLatch(1);
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                futures.add(pool.submit(() -> {
                    int id = next.getAndIncrement();
                    ready.countDown();
                    gate.await();
                    for (int i = 0; i < perThread; i++) {
                        engine.put("t" + id + "_k" + i, "v" + i);
                    }
                    return null;
                }));
            }
            ready.await();

            long start = System.nanoTime();
            gate.countDown();
            for (Future<?> future : futures) {
                future.get(120, TimeUnit.SECONDS);
            }
            long elapsed = (System.nanoTime() - start) / 1_000_000;

            assertTrue(elapsed < budget,
                    "expected group commit to finish " + total + " synced writes in under "
                            + budget + "ms, took " + elapsed + "ms; a fully serialised run "
                            + "would need at least " + serialisedFloorMillis + "ms");
        } finally {
            pool.shutdownNow();
            engine.close();
        }
    }
}
