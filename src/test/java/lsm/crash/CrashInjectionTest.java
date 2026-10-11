package lsm.crash;

import lsm.LSMStoreEngine;
import lsm.StorageEngine;
import lsm.wal.DurabilityMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Killing a writer outright and checking what the store has afterwards.
 *
 * <p>A child JVM writes keys in order and is sent SIGKILL part way through,
 * so there is no close, no flush and no shutdown hook — the process simply
 * stops. The store is then opened here and held to the guarantee buffered
 * mode actually makes: bytes handed to the operating system survive the
 * process dying, so every write that had already returned must still be
 * there.
 *
 * <p>Because the child writes strictly in order, that guarantee has a sharp
 * form: the surviving keys must be a contiguous run from zero. A hole means a
 * write was acknowledged and then lost.
 */
class CrashInjectionTest {

    @TempDir
    Path dir;

    /** Starts a writer in its own JVM so it can be killed for real. */
    private Process startWriter(Path store, long memTableBytes, DurabilityMode mode,
            boolean deleting) throws IOException {
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        ProcessBuilder builder = new ProcessBuilder(java,
                "-cp", System.getProperty("java.class.path"),
                CrashWriter.class.getName(),
                store.toString(), String.valueOf(memTableBytes), mode.name(),
                String.valueOf(deleting));
        builder.redirectErrorStream(true);
        builder.redirectOutput(ProcessBuilder.Redirect.DISCARD);
        return builder.start();
    }

    /**
     * Lets a writer run for a while, then kills it without warning.
     *
     * <p>The delay is counted from when the child is observably writing, not
     * from when it was launched. A JVM takes a couple of hundred milliseconds
     * to start, so timing from launch means the shorter cases can kill a
     * process that has not written anything yet, which proves nothing and
     * makes the test flaky.
     */
    private void crashAfter(Path store, long millis, long memTableBytes,
            DurabilityMode mode, boolean deleting) throws Exception {
        Process writer = startWriter(store, memTableBytes, mode, deleting);
        try {
            awaitFirstWrite(store, writer);
            Thread.sleep(millis);
            assertTrue(writer.isAlive(), "writer died on its own; check its arguments");
            writer.destroyForcibly();
            assertTrue(writer.waitFor(30, TimeUnit.SECONDS), "writer would not die");
        } finally {
            writer.destroyForcibly();
        }
    }

    /** Blocks until the child has put something on disk. */
    private void awaitFirstWrite(Path store, Process writer) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline) {
            if (!writer.isAlive()) {
                fail("writer exited before writing anything");
            }
            if (Files.isDirectory(store) && hasContent(store)) {
                return;
            }
            Thread.sleep(5);
        }
        fail("writer never started writing");
    }

    private boolean hasContent(Path store) throws IOException {
        try (Stream<Path> files = Files.list(store)) {
            return files.anyMatch(p -> {
                String name = p.getFileName().toString();
                return (name.startsWith("wal_") || name.endsWith(".sst"))
                        && p.toFile().length() > 0;
            });
        }
    }

    /** Every key present, read back through the engine. */
    private List<String> survivors(LSMStoreEngine engine) {
        try (Stream<StorageEngine.Row> rows = engine.scan(null, null)) {
            return rows.map(StorageEngine.Row::key).toList();
        }
    }

    /**
     * Holds the store to the guarantee: a contiguous run from zero, every
     * value matching its key, and a scan that is sorted and free of repeats.
     */
    private int assertRecoveredCleanly(LSMStoreEngine engine) throws IOException {
        List<String> keys = survivors(engine);

        // The independent record of what the writer had already been told was
        // written. Everything below it has to be here; contiguity alone would
        // not notice the newest writes going missing.
        int acknowledged = CrashWriter.progressOf(dir);
        if (acknowledged >= 0) {
            assertTrue(keys.size() > acknowledged,
                    "the writer was told " + (acknowledged + 1) + " keys were written "
                            + "but only " + keys.size() + " came back; "
                            + (acknowledged + 1 - keys.size()) + " acknowledged "
                            + "writes were lost");
        }

        String previous = null;
        for (String key : keys) {
            if (previous != null) {
                assertTrue(key.compareTo(previous) > 0,
                        "scan returned " + key + " after " + previous);
            }
            previous = key;
        }

        for (int i = 0; i < keys.size(); i++) {
            String expected = CrashWriter.key(i);
            assertEquals(expected, keys.get(i),
                    "keys are not contiguous from zero: expected " + expected
                            + " at position " + i + " but found " + keys.get(i)
                            + ". A write that had returned was lost.");
            assertEquals(Optional.of(CrashWriter.value(i)), engine.get(expected),
                    "wrong value survived for " + expected);
        }
        return keys.size();
    }

    // --- killed while writing ---

    @ParameterizedTest(name = "killed after {0}ms")
    @ValueSource(longs = {150, 300, 600, 900, 1400, 2000})
    void everyAcknowledgedWriteSurvivesAKill(long millis) throws Exception {
        crashAfter(dir, millis, 64 * 1024, DurabilityMode.BUFFERED, false);

        LSMStoreEngine engine = new LSMStoreEngine(dir, 64 * 1024);
        try {
            int survived = assertRecoveredCleanly(engine);
            assertTrue(survived > 0, "the writer should have got somewhere in " + millis + "ms");
        } finally {
            engine.close();
        }
    }

    @ParameterizedTest(name = "tiny memtable, killed after {0}ms")
    @ValueSource(longs = {250, 700, 1500})
    void aKillDuringConstantFlushingLosesNothing(long millis) throws Exception {
        // A small bound keeps flushes and compactions running continuously, so
        // the kill is far more likely to land inside one.
        crashAfter(dir, millis, 2048, DurabilityMode.BUFFERED, false);

        LSMStoreEngine engine = new LSMStoreEngine(dir, 2048);
        try {
            assertTrue(assertRecoveredCleanly(engine) > 0);
        } finally {
            engine.close();
        }
    }

    @ParameterizedTest(name = "with deletes, killed after {0}ms")
    @ValueSource(longs = {400, 1100})
    void deletesAreNotUndoneByACrash(long millis) throws Exception {
        crashAfter(dir, millis, 4096, DurabilityMode.BUFFERED, true);

        LSMStoreEngine engine = new LSMStoreEngine(dir, 4096);
        try {
            List<String> keys = survivors(engine);
            assertFalse(keys.isEmpty(), "nothing survived");
            // The writer deletes key(i/2) every fourth write, so a surviving
            // store must not contain a key whose delete had already returned.
            for (String key : keys) {
                assertEquals(Optional.of(CrashWriter.value(indexOf(key))), engine.get(key));
            }
        } finally {
            engine.close();
        }
    }

    // --- the store is still usable afterwards ---

    @Test
    void theStoreKeepsWorkingAfterARecovery() throws Exception {
        crashAfter(dir, 800, 4096, DurabilityMode.BUFFERED, false);

        LSMStoreEngine engine = new LSMStoreEngine(dir, 4096);
        int recovered;
        try {
            recovered = assertRecoveredCleanly(engine);
            for (int i = 0; i < 500; i++) {
                engine.put("after_" + i, "value");
            }
            assertEquals(Optional.of("value"), engine.get("after_499"));
        } finally {
            engine.close();
        }

        // And the recovered state plus the new writes survive another open.
        LSMStoreEngine reopened = new LSMStoreEngine(dir, 4096);
        try {
            assertEquals(Optional.of(CrashWriter.value(recovered - 1)),
                    reopened.get(CrashWriter.key(recovered - 1)));
            assertEquals(Optional.of("value"), reopened.get("after_499"));
        } finally {
            reopened.close();
        }
    }

    @Test
    void repeatedCrashesStillLeaveAValidStore() throws Exception {
        int previous = 0;
        for (int round = 0; round < 4; round++) {
            crashAfter(dir, 400, 4096, DurabilityMode.BUFFERED, false);
            LSMStoreEngine engine = new LSMStoreEngine(dir, 4096);
            try {
                int survived = assertRecoveredCleanly(engine);
                // Each round restarts from zero and runs at least as far.
                assertTrue(survived >= previous / 2,
                        "round " + round + " recovered " + survived
                                + " after " + previous);
                previous = survived;
            } finally {
                engine.close();
            }
        }
    }

    @Test
    void aCrashLeavesNoHalfWrittenTableBehind() throws Exception {
        crashAfter(dir, 1000, 2048, DurabilityMode.BUFFERED, false);

        LSMStoreEngine engine = new LSMStoreEngine(dir, 2048);
        try {
            assertRecoveredCleanly(engine);
        } finally {
            engine.close();
        }
        // Opening sweeps them, so a leftover means the sweep missed.
        try (Stream<Path> files = Files.list(dir)) {
            assertTrue(files.noneMatch(p -> p.toString().endsWith(".tmp")),
                    "a partial table survived recovery");
        }
    }

    private static int indexOf(String key) {
        return Integer.parseInt(key.substring("key_".length()));
    }
}
