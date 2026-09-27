package lsm;

import lsm.memtable.Entry;
import lsm.sstable.SSTableReader;
import lsm.sstable.SSTableWriter;
import lsm.wal.DurabilityMode;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.IntConsumer;
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

    private static final int READ_KEYS = Integer.getInteger("bench.read.keys", 100_000);
    private static final int READ_OPS = Integer.getInteger("bench.read.ops", 100_000);

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

        reads();
        bloomComparison();

        System.out.println("""

                BUFFERED returns once the bytes reach the operating system, so a
                killed process loses nothing but a power cut can lose the tail.
                SYNC waits for the disk every time.

                SYNC with eight writers against SYNC with one is the group commit
                measurement: one fsync commits every record appended before it,
                so writers that arrive during a commit ride along with it.""");

        delete(DIR);
    }

    /** Read throughput and tail latency against a compacted store. */
    private static void reads() throws Exception {
        System.out.printf("%n%,d keys, read after compaction%n%n", READ_KEYS);
        System.out.printf("%-28s %12s   %10s   %10s%n",
                "lookup", "reads/sec", "p50", "p99");
        System.out.println("-".repeat(68));

        delete(DIR);
        LSMStoreEngine engine = new LSMStoreEngine(DIR, 4L * 1024 * 1024);
        try {
            for (int i = 0; i < READ_KEYS; i++) {
                engine.put(key(i), "value_" + i);
            }
            // Compaction is asynchronous; measuring mid-merge would report the
            // shape of a store nobody actually queries.
            Thread.sleep(3_000);

            sample("hit", i -> engine.get(key(i % READ_KEYS)));
            sample("miss (in key range)", i -> {
                Optional<String> found = engine.get(absentKey(i));
                if (found.isPresent()) {
                    throw new IllegalStateException("that key should not exist");
                }
            });
        } finally {
            engine.close();
        }
        delete(DIR);
    }

    /**
     * What the bloom filter is worth, measured rather than asserted.
     *
     * <p>The baseline is the same data under a filter deliberately sized for
     * one key. Guava saturates it, so nearly every lookup says "maybe" and
     * falls through to a block read, which is what a table with no filter at
     * all would do. Both tables are otherwise identical.
     */
    private static void bloomComparison() throws Exception {
        Path dir = DIR.resolve("bloom");
        Files.createDirectories(dir);

        TreeMap<String, Entry> entries = new TreeMap<>();
        for (int i = 0; i < READ_KEYS; i++) {
            entries.put(key(i), Entry.put("value_" + i));
        }

        Path sized = dir.resolve("sized.sst");
        try (SSTableWriter writer = SSTableWriter.create(sized, entries.size())) {
            for (var entry : entries.entrySet()) {
                writer.add(entry.getKey(), entry.getValue());
            }
            writer.finish();
        }
        Path saturated = dir.resolve("saturated.sst");
        try (SSTableWriter writer = SSTableWriter.create(saturated, 1)) {
            for (var entry : entries.entrySet()) {
                writer.add(entry.getKey(), entry.getValue());
            }
            writer.finish();
        }

        System.out.printf("%n%,d keys, misses against one table%n%n", READ_KEYS);
        System.out.printf("%-28s %12s   %10s   %10s%n",
                "filter", "reads/sec", "p50", "p99");
        System.out.println("-".repeat(68));

        try (SSTableReader with = new SSTableReader(sized);
             SSTableReader without = new SSTableReader(saturated)) {
            double withRate = sample("sized for the table", i -> {
                try {
                    with.get(absentKey(i));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            double withoutRate = sample("saturated (no help)", i -> {
                try {
                    without.get(absentKey(i));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            System.out.printf("%nbloom filter is worth %.1fx on lookups that miss%n",
                    withRate / withoutRate);
        }
        delete(dir);
    }

    /** Times an operation, printing throughput and percentiles, returning ops/sec. */
    private static double sample(String label, IntConsumer operation) {
        for (int i = 0; i < READ_OPS / 10; i++) {
            operation.accept(i);
        }
        long[] nanos = new long[READ_OPS];
        for (int i = 0; i < READ_OPS; i++) {
            long start = System.nanoTime();
            operation.accept(i);
            nanos[i] = System.nanoTime() - start;
        }

        long total = Arrays.stream(nanos).sum();
        Arrays.sort(nanos);
        double rate = READ_OPS / (total / 1e9);
        System.out.printf("%-28s %,12.0f   %8.2fus   %8.2fus%n", label, rate,
                nanos[READ_OPS / 2] / 1e3, nanos[(int) (READ_OPS * 0.99)] / 1e3);
        return rate;
    }

    private static String key(int i) {
        return String.format("key_%07d", i);
    }

    /**
     * A key that is absent but sorts between two stored keys.
     *
     * <p>This matters more than it looks. A key outside the table's range is
     * rejected by the min/max check before the filter is consulted, so
     * measuring with one reports nothing about the filter at all.
     */
    private static String absentKey(int i) {
        return key(i % READ_KEYS) + "x";
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
