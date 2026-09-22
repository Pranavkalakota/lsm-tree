package lsm;

import lsm.wal.DurabilityMode;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Write throughput across durability modes and writer counts.
 *
 * <pre>
 *   mvn -q compile dependency:build-classpath -Dmdep.outputFile=target/cp.txt
 *   java -cp "target/classes:$(cat target/cp.txt)" lsm.Benchmark
 * </pre>
 *
 * Accepts -Dbench.writes, -Dbench.sync.writes, -Dbench.memtable and
 * -Dbench.dir. Shrinking the memtable brings flushing into the measurement.
 *
 * Reports what each durability choice actually costs rather than quoting one
 * headline figure. The comparison that matters is SYNC at one writer against
 * SYNC at eight: that gap is group commit, and without it the two are equal
 * because every writer waits out its own disk round trip.
 */
public final class Benchmark {

    private static final int WRITES = Integer.getInteger("bench.writes", 20_000);

    /**
     * SYNC is roughly three orders of magnitude slower, so it runs a smaller
     * sample. Handing it the same count would mean waiting minutes to learn
     * something the first few thousand writes already established.
     */
    private static final int SYNC_WRITES = Integer.getInteger("bench.sync.writes", 3_000);

    /**
     * Large by default so the run measures the write path rather than flushes.
     * Lower it (-Dbench.memtable=4194304) to include flushing in the figure.
     */
    private static final long MEMTABLE = Long.getLong("bench.memtable", 64L * 1024 * 1024);

    private static final Path DIR = Path.of(System.getProperty(
            "bench.dir", System.getProperty("java.io.tmpdir") + "/lsm-benchmark"));

    public static void main(String[] args) throws Exception {
        System.out.printf("%,d byte memtable, store at %s%n%n", MEMTABLE, DIR);
        System.out.printf("%-12s %8s %9s   %12s   %10s   %s%n",
                "mode", "writers", "writes", "writes/sec", "ms/write", "survives");
        System.out.println("-".repeat(80));

        run(DurabilityMode.BUFFERED, 1);
        run(DurabilityMode.BUFFERED, 8);
        run(DurabilityMode.SYNC, 1);
        run(DurabilityMode.SYNC, 8);

        System.out.println("""

                BUFFERED returns once the bytes reach the operating system, so a
                killed process loses nothing but a power cut can lose the tail.
                SYNC waits for the disk every time.

                SYNC with eight writers against SYNC with one is the group commit
                measurement: one fsync commits every record appended before it,
                so writers that arrive during a commit ride along with it.""");

        delete(DIR);
    }

    private static void run(DurabilityMode mode, int writers) throws Exception {
        int writes = mode == DurabilityMode.SYNC ? SYNC_WRITES : WRITES;
        delete(DIR);
        LSMStoreEngine engine = new LSMStoreEngine(DIR, MEMTABLE, mode);
        try {
            drive(engine, writers, Math.max(writes / 10, 1), "warm");
            long nanos = drive(engine, writers, writes, "run");
            double seconds = nanos / 1e9;
            System.out.printf("%-12s %8d %,9d   %,12.0f   %10.3f   %s%n",
                    mode, writers, writes, writes / seconds, seconds * 1000 / writes,
                    mode == DurabilityMode.SYNC ? "power loss" : "process crash");
        } finally {
            engine.close();
        }
    }

    private static long drive(LSMStoreEngine engine, int writers, int total, String tag)
            throws Exception {
        if (writers == 1) {
            long start = System.nanoTime();
            for (int i = 0; i < total; i++) {
                engine.put(tag + "_key_" + i, "value_" + i);
            }
            return System.nanoTime() - start;
        }

        ExecutorService pool = Executors.newFixedThreadPool(writers);
        AtomicInteger next = new AtomicInteger();
        CountDownLatch ready = new CountDownLatch(writers);
        CountDownLatch gate = new CountDownLatch(1);
        int per = total / writers;

        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < writers; t++) {
                futures.add(pool.submit(() -> {
                    int id = next.getAndIncrement();
                    ready.countDown();
                    gate.await();
                    for (int i = 0; i < per; i++) {
                        engine.put(tag + "_t" + id + "_k" + i, "value_" + i);
                    }
                    return null;
                }));
            }
            ready.await();
            long start = System.nanoTime();
            gate.countDown();
            for (Future<?> future : futures) {
                future.get();
            }
            return System.nanoTime() - start;
        } finally {
            pool.shutdownNow();
        }
    }

    private static void delete(Path dir) throws IOException {
        if (!Files.exists(dir)) {
            return;
        }
        try (var walk = Files.walk(dir)) {
            walk.sorted((a, b) -> b.getNameCount() - a.getNameCount()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (IOException ignored) {
                    // Best effort; a leftover benchmark directory harms nothing.
                }
            });
        }
    }

    private Benchmark() {
    }
}
