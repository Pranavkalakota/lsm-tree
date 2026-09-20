package lsm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Several threads writing to one store at once.
 *
 * <p>Writes are serialized internally, so these do not measure throughput.
 * What they establish is that concurrency cannot corrupt the store: no lost
 * write, no torn log record, and no reader observing a key that vanishes
 * because a flush happened to run underneath it.
 */
class ConcurrentWriterTest {

    @TempDir
    Path dir;

    private static <T> List<Future<T>> startAll(ExecutorService pool, CountDownLatch gate,
            int count, Callable<T> task) {
        List<Future<T>> futures = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            futures.add(pool.submit(() -> {
                gate.await();
                return task.call();
            }));
        }
        gate.countDown();
        return futures;
    }

    @Test
    void everyWriteFromEveryThreadSurvives() throws Exception {
        int threads = 8;
        int perThread = 150;
        LSMStoreEngine engine = new LSMStoreEngine(dir, 2 * 1024);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        AtomicInteger next = new AtomicInteger();

        try {
            CountDownLatch gate = new CountDownLatch(1);
            var futures = startAll(pool, gate, threads, () -> {
                int id = next.getAndIncrement();
                for (int i = 0; i < perThread; i++) {
                    engine.put("t" + id + "_k" + i, "t" + id + "_v" + i);
                }
                return null;
            });
            for (Future<?> future : futures) {
                future.get(120, TimeUnit.SECONDS);
            }

            for (int id = 0; id < threads; id++) {
                for (int i = 0; i < perThread; i++) {
                    assertEquals(Optional.of("t" + id + "_v" + i),
                            engine.get("t" + id + "_k" + i),
                            "lost the write from thread " + id + " at " + i);
                }
            }
        } finally {
            pool.shutdownNow();
            engine.close();
        }
    }

    @Test
    void concurrentWritesSurviveAReopen() throws Exception {
        int threads = 4;
        int perThread = 100;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        AtomicInteger next = new AtomicInteger();

        LSMStoreEngine engine = new LSMStoreEngine(dir, 1024);
        try {
            CountDownLatch gate = new CountDownLatch(1);
            var futures = startAll(pool, gate, threads, () -> {
                int id = next.getAndIncrement();
                for (int i = 0; i < perThread; i++) {
                    engine.put("t" + id + "_k" + i, "v" + i);
                }
                return null;
            });
            for (Future<?> future : futures) {
                future.get(120, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
            engine.close();
        }

        // Whatever interleaving happened, the log and the tables together must
        // still account for all of it.
        LSMStoreEngine reopened = new LSMStoreEngine(dir);
        try {
            for (int id = 0; id < threads; id++) {
                for (int i = 0; i < perThread; i++) {
                    assertEquals(Optional.of("v" + i), reopened.get("t" + id + "_k" + i),
                            "lost the write from thread " + id + " at " + i);
                }
            }
        } finally {
            reopened.close();
        }
    }

    @Test
    void readersNeverSeeAKeyDisappearDuringAFlush() throws Exception {
        LSMStoreEngine engine = new LSMStoreEngine(dir, 512);
        ExecutorService pool = Executors.newFixedThreadPool(5);

        try {
            for (int i = 0; i < 100; i++) {
                engine.put("stable_" + i, "value_" + i);
            }

            CountDownLatch gate = new CountDownLatch(1);
            List<Future<?>> futures = new ArrayList<>();

            // One writer churning hard enough to flush repeatedly underneath.
            futures.add(pool.submit(() -> {
                gate.await();
                for (int i = 0; i < 600; i++) {
                    engine.put("churn_" + i, "x".repeat(40));
                }
                return null;
            }));

            // Readers on keys that were already durable before any of it began.
            for (int r = 0; r < 4; r++) {
                futures.add(pool.submit(() -> {
                    gate.await();
                    for (int round = 0; round < 40; round++) {
                        for (int i = 0; i < 100; i++) {
                            assertEquals(Optional.of("value_" + i), engine.get("stable_" + i),
                                    "stable_" + i + " vanished mid-flush");
                        }
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
    }

    @Test
    void mixedWritesAndDeletesLeaveNoHalfAppliedKeys() throws Exception {
        LSMStoreEngine engine = new LSMStoreEngine(dir, 1024);
        ExecutorService pool = Executors.newFixedThreadPool(6);

        try {
            CountDownLatch gate = new CountDownLatch(1);
            AtomicInteger next = new AtomicInteger();

            // Each thread owns a disjoint key range, so the expected end state
            // is exact rather than dependent on who won a race.
            var futures = startAll(pool, gate, 6, () -> {
                int id = next.getAndIncrement();
                for (int i = 0; i < 100; i++) {
                    engine.put("t" + id + "_k" + i, "v" + i);
                    if (i % 3 == 0) {
                        engine.delete("t" + id + "_k" + i);
                    }
                }
                return null;
            });
            for (Future<?> future : futures) {
                future.get(120, TimeUnit.SECONDS);
            }

            for (int id = 0; id < 6; id++) {
                for (int i = 0; i < 100; i++) {
                    Optional<String> expected =
                            i % 3 == 0 ? Optional.empty() : Optional.of("v" + i);
                    assertEquals(expected, engine.get("t" + id + "_k" + i),
                            "thread " + id + " key " + i);
                }
            }
        } finally {
            pool.shutdownNow();
            engine.close();
        }
    }
}
