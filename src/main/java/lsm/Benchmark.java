package lsm;

import lsm.memtable.Entry;
import lsm.sstable.BlockCache;
import lsm.sstable.CompressionType;
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

    private static final int COMPRESSION_KEYS =
            Integer.getInteger("bench.compression.keys", 50_000);

    private static final int READ_KEYS = Integer.getInteger("bench.read.keys", 100_000);
    private static final int READ_OPS = Integer.getInteger("bench.read.ops", 100_000);

    /** Negative means let the engine pick, which depends on durability mode. */
    private static final int SHARDS = Integer.getInteger("bench.shards", -1);

    private static final Path DIR = Path.of(System.getProperty(
            "bench.dir", System.getProperty("java.io.tmpdir") + "/lsm-benchmark"));

    public static void main(String[] args) throws Exception {
        System.out.printf("%,d byte memtable, shards=%s, store at %s%n%n",
                MEMTABLE, SHARDS > 0 ? String.valueOf(SHARDS) : "per mode", DIR);
        System.out.printf("%-12s %8s %9s   %12s   %10s   %s%n",
                "mode", "writers", "writes", "writes/sec", "ms/write", "survives");
        System.out.println("-".repeat(80));

        run(DurabilityMode.BUFFERED, 1);
        run(DurabilityMode.BUFFERED, 8);
        run(DurabilityMode.SYNC, 1);
        run(DurabilityMode.SYNC, 8);

        reads();
        coldReads();
        bloomComparison();
        compression();

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

            String[] present = new String[READ_OPS];
            String[] absent = new String[READ_OPS];
            for (int i = 0; i < READ_OPS; i++) {
                present[i] = key(i % READ_KEYS);
                absent[i] = absentKey(i);
            }

            sample("hit", i -> engine.get(present[i]));
            sample("miss (in key range)", i -> {
                Optional<String> found = engine.get(absent[i]);
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

        String[] absent = new String[READ_OPS];
        for (int i = 0; i < READ_OPS; i++) {
            absent[i] = absentKey(i);
        }

        try (SSTableReader with = new SSTableReader(sized);
             SSTableReader without = new SSTableReader(saturated)) {
            double withRate = sample("sized for the table", i -> {
                try {
                    with.get(absent[i]);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            double withoutRate = sample("saturated (no help)", i -> {
                try {
                    without.get(absent[i]);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            System.out.printf("%nbloom filter is worth %.1fx on lookups that miss%n",
                    withRate / withoutRate);
        }
        delete(dir);
    }

    /**
     * Reads once the working set no longer fits in memory.
     *
     * <p>The figures above this are all cache hits: a hundred thousand small
     * keys is a couple of megabytes against a sixteen megabyte cache, so
     * almost nothing reaches a file. That is a real number for a small store
     * and a misleading one for anything else, so the same reads are repeated
     * against caches too small to hold the data.
     *
     * <p>What this still does not measure is a cold operating system. The
     * files stay in the page cache throughout, so these are reads that pay for
     * block lookup, checksum and decompression but not for a disk seek. A
     * genuinely cold number would need a dataset larger than RAM.
     */
    private static void coldReads() throws Exception {
        System.out.printf("%n%,d keys, cache sized against the working set%n%n", READ_KEYS);
        System.out.printf("%-28s %12s   %10s   %10s%n",
                "block cache", "reads/sec", "p50", "p99");
        System.out.println("-".repeat(68));

        for (long cache : new long[] {64L * 1024 * 1024, 4L * 1024 * 1024,
                512L * 1024, 64L * 1024}) {
            delete(DIR);
            LSMStoreEngine engine = new LSMStoreEngine(
                    DIR, 4L * 1024 * 1024, DurabilityMode.BUFFERED, 8, cache);
            try {
                for (int i = 0; i < READ_KEYS; i++) {
                    engine.put(key(i), "value_" + i);
                }
                Thread.sleep(3_000);

                String[] keys = new String[READ_OPS];
                java.util.Random random = new java.util.Random(7);
                for (int i = 0; i < READ_OPS; i++) {
                    // Random rather than sequential: sequential access walks a
                    // block at a time and hides a small cache behind locality.
                    keys[i] = key(random.nextInt(READ_KEYS));
                }
                sample(describeBytes(cache), i -> engine.get(keys[i]));
            } finally {
                engine.close();
            }
        }
        delete(DIR);
    }

    private static String describeBytes(long bytes) {
        return bytes >= 1024 * 1024
                ? (bytes / (1024 * 1024)) + " MB"
                : (bytes / 1024) + " KB";
    }

    /**
     * What compression costs and what it saves.
     *
     * <p>The saving depends entirely on the data, so several shapes are
     * reported rather than one headline ratio. The cost is paid on reads that
     * miss the block cache, since the cache holds blocks already decompressed,
     * so the read comparison deliberately uses a cache far too small to help.
     */
    private static void compression() throws Exception {
        Path dir = DIR.resolve("compression");
        Files.createDirectories(dir);

        System.out.printf("%n%,d keys per dataset%n%n", COMPRESSION_KEYS);
        System.out.printf("%-26s %12s %12s %8s%n", "dataset", "raw", "lz4", "saving");
        System.out.println("-".repeat(62));

        long rawTotal = shape(dir, "repetitive", i ->
                "status=active;tier=free;region=us-east-1");
        shape(dir, "json-like", i ->
                "{\"id\":" + i + ",\"type\":\"click\",\"ok\":true}");
        shape(dir, "tiny", i -> "1");
        shape(dir, "high entropy", Benchmark::noise);

        readCost(dir);
        delete(dir);
        if (rawTotal < 0) {
            throw new IllegalStateException("unreachable");
        }
    }

    /** Writes one dataset both ways and reports the saving. */
    private static long shape(Path dir, String label, java.util.function.IntFunction<String> value)
            throws Exception {
        TreeMap<String, Entry> entries = new TreeMap<>();
        for (int i = 0; i < COMPRESSION_KEYS; i++) {
            entries.put(String.format("key_%08d", i), Entry.put(value.apply(i)));
        }
        long raw = writeTable(dir.resolve(label + "-raw.sst"), entries, CompressionType.NONE);
        long packed = writeTable(dir.resolve(label + "-lz4.sst"), entries, CompressionType.LZ4);

        System.out.printf("%-26s %,12d %,12d %7.0f%%%n",
                label, raw, packed, 100.0 * (raw - packed) / raw);
        return raw;
    }

    /** Read throughput against a cache too small to hide decompression. */
    private static void readCost(Path dir) throws Exception {
        TreeMap<String, Entry> entries = new TreeMap<>();
        for (int i = 0; i < COMPRESSION_KEYS; i++) {
            entries.put(String.format("key_%08d", i),
                    Entry.put("status=active;tier=free;region=us-east-1"));
        }
        Path raw = dir.resolve("cost-raw.sst");
        Path packed = dir.resolve("cost-lz4.sst");
        writeTable(raw, entries, CompressionType.NONE);
        writeTable(packed, entries, CompressionType.LZ4);

        String[] keys = new String[READ_OPS];
        for (int i = 0; i < READ_OPS; i++) {
            keys[i] = String.format("key_%08d", i % COMPRESSION_KEYS);
        }

        System.out.printf("%n%-28s %12s   %10s   %10s%n",
                "cold reads", "reads/sec", "p50", "p99");
        System.out.println("-".repeat(68));
        try (SSTableReader plain = new SSTableReader(raw, new BlockCache(1));
             SSTableReader compressed = new SSTableReader(packed, new BlockCache(1))) {
            sample("uncompressed blocks", i -> {
                try {
                    plain.get(keys[i]);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            sample("lz4 blocks", i -> {
                try {
                    compressed.get(keys[i]);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    private static long writeTable(Path path, TreeMap<String, Entry> entries,
            CompressionType type) throws Exception {
        Files.deleteIfExists(path);
        try (SSTableWriter writer = SSTableWriter.create(path, entries.size(), type)) {
            for (var entry : entries.entrySet()) {
                writer.add(entry.getKey(), entry.getValue());
            }
            writer.finish();
        }
        return Files.size(path);
    }

    /** Pseudo-random text, so LZ4 has nothing to find. */
    private static String noise(int seed) {
        java.util.Random random = new java.util.Random(seed);
        StringBuilder out = new StringBuilder(40);
        for (int i = 0; i < 40; i++) {
            out.append((char) (0x21 + random.nextInt(94)));
        }
        return out.toString();
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
        LSMStoreEngine engine = SHARDS > 0
                ? new LSMStoreEngine(DIR, MEMTABLE, mode, SHARDS)
                : new LSMStoreEngine(DIR, MEMTABLE, mode);
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

    /**
     * Runs {@code total} writes and returns how long they took.
     *
     * <p>Keys and values are built before the clock starts. Formatting them
     * inside the timed loop is not free at these rates: doing so understated
     * single-writer throughput by around 20% and hid the effect of sharding
     * entirely, which is how an earlier version of this harness concluded that
     * splitting the write path achieved nothing.
     */
    private static long drive(LSMStoreEngine engine, int writers, int total, String tag)
            throws Exception {
        if (writers == 1) {
            String[] keys = new String[total];
            String[] values = new String[total];
            for (int i = 0; i < total; i++) {
                keys[i] = tag + "_key_" + i;
                values[i] = "value_" + i;
            }
            long start = System.nanoTime();
            for (int i = 0; i < total; i++) {
                engine.put(keys[i], values[i]);
            }
            return System.nanoTime() - start;
        }

        int per = total / writers;
        String[][] keys = new String[writers][per];
        String[] values = new String[per];
        for (int t = 0; t < writers; t++) {
            for (int i = 0; i < per; i++) {
                keys[t][i] = tag + "_t" + t + "_k" + i;
            }
        }
        for (int i = 0; i < per; i++) {
            values[i] = "value_" + i;
        }

        ExecutorService pool = Executors.newFixedThreadPool(writers);
        AtomicInteger next = new AtomicInteger();
        CountDownLatch ready = new CountDownLatch(writers);
        CountDownLatch gate = new CountDownLatch(1);

        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < writers; t++) {
                futures.add(pool.submit(() -> {
                    int id = next.getAndIncrement();
                    ready.countDown();
                    gate.await();
                    for (int i = 0; i < per; i++) {
                        engine.put(keys[id][i], values[i]);
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
