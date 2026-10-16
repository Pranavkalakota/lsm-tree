package lsm;

import lsm.memtable.Entry;
import lsm.net.StoreServer;
import lsm.sstable.BlockCache;
import lsm.sstable.CompressionType;
import lsm.sstable.SSTableReader;
import lsm.sstable.SSTableWriter;
import lsm.wal.DurabilityMode;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
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

    /**
     * Gigabytes to write before the cold read measurement, or 0 to skip it.
     *
     * <p>Off by default because it is the one measurement here that cannot be
     * taken cheaply: every other number in this harness comes from data the
     * page cache is holding, and the only ways past that are a dataset larger
     * than memory or a privileged cache drop. Both are the operator's call,
     * not something a benchmark should do unasked.
     */
    private static final int COLD_GB = Integer.getInteger("bench.cold.gb", 0);

    /** Requests sent before reading any reply, when measuring pipelining. */
    private static final int PIPELINE_DEPTH =
            Integer.getInteger("bench.pipeline", 32);

    /** Overwrites piled on one key to build history under a held snapshot. */
    private static final int SNAPSHOT_CHURN =
            Integer.getInteger("bench.snapshot.churn", 200_000);
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
        snapshots();
        server();
        trulyColdReads();

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

    /**
     * What a snapshot costs: nothing much to read through, and space to hold.
     *
     * <p>Worth measuring rather than asserting, because the shape of the cost
     * is not obvious. A snapshot read is a seek to {@code (key, version)}
     * instead of to the newest version, which should be the same work; the
     * part that is genuinely expensive is that compaction may not discard
     * anything an open snapshot could still reach.
     */
    private static void snapshots() throws Exception {
        snapshotReadCost();
        snapshotSpaceCost();
        snapshotCreationCost();
    }

    /** Reading now, versus reading through a snapshot with history beneath it. */
    private static void snapshotReadCost() throws Exception {
        System.out.printf("%n%,d keys, reading through a snapshot%n%n", READ_KEYS);
        System.out.printf("%-28s %12s   %10s   %10s%n",
                "lookup", "reads/sec", "p50", "p99");
        System.out.println("-".repeat(68));

        delete(DIR);
        LSMStoreEngine engine = new LSMStoreEngine(DIR, 4L * 1024 * 1024);
        try {
            for (int i = 0; i < READ_KEYS; i++) {
                engine.put(key(i), "value_" + i);
            }
            Thread.sleep(3_000);

            String[] keys = new String[READ_OPS];
            for (int i = 0; i < READ_OPS; i++) {
                keys[i] = key(i % READ_KEYS);
            }

            // Warmed before anything is timed. The first sample otherwise
            // pays for cold blocks and makes whatever runs second look
            // faster, which is how a snapshot read can appear to beat a
            // plain one.
            for (int round = 0; round < 3; round++) {
                for (String k : keys) {
                    engine.get(k);
                }
            }

            double first = sample("current value", i -> engine.get(keys[i]));

            double atSnapshot;
            try (Snapshot fresh = engine.snapshot()) {
                atSnapshot = sample("through a snapshot", i -> engine.get(keys[i], fresh));
            }

            // The baseline again, after the snapshot run, so any drift over
            // the course of the measurement is visible rather than hidden.
            double second = sample("current value again", i -> engine.get(keys[i]));
            double current = (first + second) / 2;

            // Now with every key overwritten underneath the snapshot, so the
            // version it wants is no longer the first one a lookup meets.
            double buried;
            try (Snapshot held = engine.snapshot()) {
                for (int round = 0; round < 3; round++) {
                    for (int i = 0; i < READ_KEYS; i++) {
                        engine.put(key(i), "rewritten_" + round + "_" + i);
                    }
                }
                Thread.sleep(3_000);
                for (String k : keys) {
                    engine.get(k, held);
                }
                buried = sample("snapshot, 3 versions newer", i -> engine.get(keys[i], held));
            }

            // Reported as a ratio and nothing more. Reading at a snapshot
            // runs the same code as reading now -- get(key) delegates to
            // get(key, NEWEST) -- so there is no cost here to find, and the
            // baseline is measured twice to show how much the number moves
            // on its own. Either side of 1.00 is noise, not a result.
            double spread = Math.max(first, second) / Math.min(first, second) - 1;
            System.out.printf("%nsnapshot read vs current read: %.2fx, against %.0f%% "
                            + "drift in the baseline itself -- the same seek either way%n",
                    current / atSnapshot, spread * 100);
            System.out.printf("reading a version three overwrites old: %.2fx, "
                            + "which is the real cost%n", current / buried);
        } finally {
            engine.close();
        }
        delete(DIR);
    }

    /**
     * The real cost: history a held snapshot will not let compaction discard.
     *
     * <p>The same workload runs twice, once with a snapshot open throughout
     * and once without, and the store is measured after compaction settles.
     */
    private static void snapshotSpaceCost() throws Exception {
        System.out.printf("%n%,d overwrites of one key, measured after compaction%n%n",
                SNAPSHOT_CHURN);
        System.out.printf("%-28s %14s %14s%n", "", "bytes on disk", "versions kept");
        System.out.println("-".repeat(60));

        long free = churnAndMeasure(false);
        long held = churnAndMeasure(true);

        System.out.printf("%na held snapshot kept %.1fx the bytes of the same "
                        + "workload with none open%n", (double) held / Math.max(free, 1));
    }

    /** Runs the churn with or without a snapshot open, returning bytes on disk. */
    private static long churnAndMeasure(boolean holdSnapshot) throws Exception {
        delete(DIR);
        // Deliberately small. The point is what survives on DISK, and a
        // MemTable large enough to hold the whole churn would flush nothing,
        // leaving both runs measuring an empty directory.
        LSMStoreEngine engine = new LSMStoreEngine(DIR, 64L * 1024);
        Snapshot snapshot = null;
        try {
            engine.put("hot", "first");
            if (holdSnapshot) {
                snapshot = engine.snapshot();
            }
            for (int i = 0; i < SNAPSHOT_CHURN; i++) {
                engine.put("hot", "value_" + i);
            }
            // Compaction is what collapses history, so this waits for it to
            // stop changing the answer rather than sleeping a fixed amount.
            // A fixed sleep makes the headline number depend on whether the
            // machine happened to finish in time.
            long versions = awaitSettled();
            long bytes = storeBytes(DIR);
            System.out.printf("%-28s %,14d %,14d%n",
                    holdSnapshot ? "snapshot held throughout" : "no snapshot open",
                    bytes, versions);
            return bytes;
        } finally {
            if (snapshot != null) {
                snapshot.close();
            }
            engine.close();
            delete(DIR);
        }
    }

    /**
     * Waits for compaction to stop changing how much history is stored,
     * returning the figure it settled on.
     */
    private static long awaitSettled() throws Exception {
        long previous = -1;
        long stableFor = 0;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (System.nanoTime() < deadline) {
            Thread.sleep(500);
            long current = versionsOf(DIR, "hot");
            stableFor = current == previous ? stableFor + 1 : 0;
            previous = current;
            if (stableFor >= 3) {
                return current;
            }
        }
        return previous;
    }

    /** How long taking a snapshot blocks, given it waits for every shard lock. */
    private static void snapshotCreationCost() throws Exception {
        delete(DIR);
        LSMStoreEngine engine = new LSMStoreEngine(DIR, 64L * 1024 * 1024,
                DurabilityMode.BUFFERED, 8);
        try {
            for (int i = 0; i < 1_000; i++) {
                engine.put(key(i), "value_" + i);
            }

            int samples = 2_000;
            long[] nanos = new long[samples];
            for (int i = 0; i < samples; i++) {
                long start = System.nanoTime();
                engine.snapshot().close();
                nanos[i] = System.nanoTime() - start;
            }
            Arrays.sort(nanos);

            System.out.printf("%ntaking a snapshot: %.2fus median, %.2fus p99 "
                            + "(it waits for all 8 shard locks)%n",
                    nanos[samples / 2] / 1e3, nanos[(int) (samples * 0.99)] / 1e3);
        } finally {
            engine.close();
        }
        delete(DIR);
    }

    /**
     * Reads that genuinely touch the disk, rather than the page cache.
     *
     * <p>Every other read figure in this harness is a memory-speed number
     * wearing a storage-engine label: the files are small enough to sit in
     * the operating system's cache throughout, so the reads pay for block
     * lookup, checksum and decompression but never for a seek. The only
     * honest ways around that are to write more data than the machine has
     * memory, or to drop the cache, which needs privileges this process does
     * not have and should not ask for.
     *
     * <p>So this runs only when asked: {@code -Dbench.cold.gb=32} on a
     * machine with less memory than that. The alternative, if you would
     * rather not write tens of gigabytes, is to drop the cache by hand
     * between the write and the read -- {@code sudo purge} on macOS,
     * {@code echo 3 | sudo tee /proc/sys/vm/drop_caches} on Linux -- which
     * the README spells out.
     */
    private static void trulyColdReads() throws Exception {
        if (COLD_GB <= 0) {
            long ram = Runtime.getRuntime().maxMemory();
            System.out.printf("%ncold reads against the disk: skipped. Every read "
                    + "figure above is served from the page cache.%n"
                    + "Run -Dbench.cold.gb=N with N above the machine's RAM to "
                    + "measure one, or see the README for the purge recipe.%n"
                    + "(this JVM's heap ceiling is %,d MB; the machine's RAM is "
                    + "what matters)%n", ram / (1024 * 1024));
            return;
        }

        long target = COLD_GB * 1024L * 1024L * 1024L;
        System.out.printf("%nwriting %,d GB so the dataset outgrows the page cache%n",
                COLD_GB);

        delete(DIR);
        LSMStoreEngine engine = new LSMStoreEngine(DIR, 64L * 1024 * 1024);
        try {
            // Incompressible on purpose. Repetitive padding compresses by
            // better than 85%, so a run using it would write gigabytes and
            // still leave a store small enough to sit entirely in the cache,
            // measuring exactly the thing this is trying to avoid. Slices of
            // one random pool give every value different content at no
            // per-value cost.
            java.util.Random filler = new java.util.Random(7);
            char[] pool = new char[1 << 20];
            for (int i = 0; i < pool.length; i++) {
                pool[i] = (char) (0x21 + filler.nextInt(94));
            }
            int valueSize = 900;

            int written = 0;
            long onDisk = 0;
            while (onDisk < target) {
                for (int i = 0; i < 200_000; i++) {
                    int offset = (written * 907) % (pool.length - valueSize);
                    engine.put(key(written++), new String(pool, offset, valueSize));
                }
                onDisk = storeBytes(DIR);
                System.out.printf("  %,d keys, %,d MB on disk%n", written,
                        onDisk / (1024 * 1024));
            }

            // Reads scattered across the whole range, so the page cache
            // cannot hold the working set however large it is.
            java.util.Random random = new java.util.Random(42);
            String[] keys = new String[READ_OPS];
            for (int i = 0; i < READ_OPS; i++) {
                keys[i] = key(random.nextInt(written));
            }

            System.out.printf("%n%-28s %12s   %10s   %10s%n",
                    "cold read", "reads/sec", "p50", "p99");
            System.out.println("-".repeat(68));
            sample("random, dataset > RAM", i -> engine.get(keys[i]));
        } finally {
            engine.close();
        }
        delete(DIR);
    }

    /** Total size of every table in the store. */
    private static final class BenchClient implements Closeable {
        private final Socket socket;
        private final BufferedReader in;
        private final Writer out;

        BenchClient(int port) throws IOException {
            socket = new Socket("127.0.0.1", port);
            // Without this a request-response protocol waits on Nagle's
            // algorithm to decide a small write is worth sending.
            socket.setTcpNoDelay(true);
            socket.setSoTimeout(30_000);
            in = new BufferedReader(new InputStreamReader(
                    socket.getInputStream(), StandardCharsets.UTF_8));
            out = new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8);
        }

        String command(String line) throws IOException {
            send(line);
            out.flush();
            return in.readLine();
        }

        void send(String line) throws IOException {
            out.write(line);
            out.write('\n');
        }

        void flush() throws IOException {
            out.flush();
        }

        String read() throws IOException {
            return in.readLine();
        }

        @Override
        public void close() throws IOException {
            socket.close();
        }
    }

    /**
     * What the network costs, which is the only thing this section measures.
     *
     * <p>Every other figure in this harness is an in-process call. A server
     * puts a socket, a line protocol and a round trip in front of the same
     * engine, and the interesting question is how much of the result is then
     * the store at all.
     */
    private static void server() throws Exception {
        delete(DIR);
        LSMStoreEngine engine = new LSMStoreEngine(DIR, MEMTABLE);
        StoreServer server = null;
        try {
            for (int i = 0; i < READ_KEYS; i++) {
                engine.put(key(i), "value_" + i);
            }
            Thread.sleep(2_000);

            server = new StoreServer(engine, 0);
            String[] gets = new String[READ_OPS];
            for (int i = 0; i < READ_OPS; i++) {
                gets[i] = "GET " + key(i % READ_KEYS);
            }

            System.out.printf("%n%,d keys, served over loopback%n%n", READ_KEYS);
            System.out.printf("%-28s %12s   %10s   %10s%n",
                    "request", "ops/sec", "p50", "p99");
            System.out.println("-".repeat(68));

            double inProcess = sample("in process, no socket",
                    i -> engine.get(key(i % READ_KEYS)));

            double overTcp;
            try (BenchClient client = new BenchClient(server.port())) {
                overTcp = sample("GET over tcp", i -> {
                    try {
                        client.command(gets[i]);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
                sample("PUT over tcp", i -> {
                    try {
                        client.command("PUT bench_" + i + " value");
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
            }

            double pipelined = pipelinedReads(server.port(), gets);
            System.out.printf("%na round trip costs %.0fx an in-process call; "
                            + "pipelining %d deep wins %.1fx of it back%n",
                    inProcess / overTcp, PIPELINE_DEPTH, pipelined / overTcp);

            concurrentClients(server.port(), gets);
        } finally {
            if (server != null) {
                server.close();
            }
            engine.close();
        }
        delete(DIR);
    }

    /**
     * The same reads with several in flight at once.
     *
     * <p>A request-response client spends most of its time waiting for the
     * wire rather than for the store, so this separates the two: if filling
     * the pipe makes it much faster, the round trip was the bottleneck.
     */
    private static double pipelinedReads(int port, String[] gets) throws Exception {
        try (BenchClient client = new BenchClient(port)) {
            int batches = READ_OPS / PIPELINE_DEPTH;
            for (int warm = 0; warm < batches / 10; warm++) {
                runBatch(client, gets, warm);
            }

            long start = System.nanoTime();
            for (int batch = 0; batch < batches; batch++) {
                runBatch(client, gets, batch);
            }
            long nanos = System.nanoTime() - start;

            double rate = (batches * (double) PIPELINE_DEPTH) / (nanos / 1e9);
            System.out.printf("%-28s %,12.0f   %10s   %10s%n",
                    "GET pipelined x" + PIPELINE_DEPTH, rate, "-", "-");
            return rate;
        }
    }

    private static void runBatch(BenchClient client, String[] gets, int batch)
            throws IOException {
        for (int i = 0; i < PIPELINE_DEPTH; i++) {
            client.send(gets[(batch * PIPELINE_DEPTH + i) % gets.length]);
        }
        client.flush();
        for (int i = 0; i < PIPELINE_DEPTH; i++) {
            client.read();
        }
    }

    /** Throughput as clients are added, each on its own connection. */
    private static void concurrentClients(int port, String[] gets) throws Exception {
        System.out.printf("%n%-28s %12s%n", "concurrent clients", "ops/sec");
        System.out.println("-".repeat(44));
        for (int clients : new int[] {1, 2, 4, 8, 16}) {
            System.out.printf("%-28d %,12.0f%n", clients,
                    clientThroughput(port, gets, clients));
        }
    }

    private static double clientThroughput(int port, String[] gets, int clients)
            throws Exception {
        int per = Math.max(READ_OPS / clients, 1);
        ExecutorService pool = Executors.newFixedThreadPool(clients);
        CountDownLatch ready = new CountDownLatch(clients);
        CountDownLatch gate = new CountDownLatch(1);

        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int c = 0; c < clients; c++) {
                futures.add(pool.submit(() -> {
                    try (BenchClient client = new BenchClient(port)) {
                        for (int i = 0; i < per / 10; i++) {
                            client.command(gets[i % gets.length]);
                        }
                        ready.countDown();
                        gate.await();
                        for (int i = 0; i < per; i++) {
                            client.command(gets[i % gets.length]);
                        }
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
            long nanos = System.nanoTime() - start;
            return (clients * (double) per) / (nanos / 1e9);
        } finally {
            pool.shutdownNow();
        }
    }

    /** Total size of every table in the store. */
    private static long storeBytes(Path dir) throws IOException {
        try (var files = Files.list(dir)) {
            return files.filter(path -> path.getFileName().toString().endsWith(".sst"))
                    .mapToLong(path -> {
                        try {
                            return Files.size(path);
                        } catch (IOException e) {
                            return 0;
                        }
                    }).sum();
        }
    }

    /** How many versions of one key survive across every table. */
    private static long versionsOf(Path dir, String key) throws IOException {
        List<Path> tables;
        try (var files = Files.list(dir)) {
            tables = files.filter(p -> p.getFileName().toString().endsWith(".sst")).toList();
        }
        long found = 0;
        for (Path path : tables) {
            try (SSTableReader reader = new SSTableReader(path, new BlockCache(1 << 16))) {
                SSTableReader.Cursor cursor = reader.cursor();
                while (cursor.hasNext()) {
                    if (cursor.next().key().equals(key)) {
                        found++;
                    }
                }
            } catch (IOException e) {
                // Retired mid-walk by compaction; its rows live elsewhere now.
            }
        }
        return found;
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
