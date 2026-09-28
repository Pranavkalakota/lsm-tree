package lsm.property;

import lsm.LSMStoreEngine;
import lsm.wal.DurabilityMode;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The engine checked against a plain map over generated operation sequences.
 *
 * <p>A HashMap is the specification here: whatever sequence of puts and
 * deletes goes in, the engine has to answer every key the same way the map
 * does. Flushing, compaction and log replay all have to be invisible to that.
 */
class EnginePropertyTest {

    @TempDir
    Path dir;

    /** Seeds crossed with a durability mode and a MemTable bound. */
    static Stream<Arguments> runs() {
        long[] bounds = {64, 512, 4096};
        return Stream.of(DurabilityMode.BUFFERED, DurabilityMode.SYNC)
                .flatMap(mode -> Stream.of(bounds).flatMap(all ->
                        java.util.Arrays.stream(all).boxed().flatMap(bound ->
                                java.util.stream.IntStream.range(0, 8).mapToObj(seed ->
                                        Arguments.of(mode, bound, (long) seed)))));
    }

    /**
     * How many operations a mode can afford. SYNC pays an fsync per write, so
     * a run the size of the buffered one would dominate the whole suite for no
     * extra coverage: the point of the SYNC cases is that durability mode does
     * not change what the store answers, not to re-exercise volume.
     */
    private static int operationsFor(DurabilityMode mode) {
        return mode == DurabilityMode.SYNC ? 60 : 400;
    }

    /** Applies the same operations to the engine and to a reference map. */
    private Map<String, String> drive(LSMStoreEngine engine, long seed, int operations) {
        Random random = new Random(seed);
        Map<String, String> expected = new HashMap<>();
        // A small key space on purpose, so overwrites and deletes actually
        // collide rather than every operation touching a fresh key.
        int keySpace = 60;

        for (int i = 0; i < operations; i++) {
            String key = "key_" + random.nextInt(keySpace);
            if (random.nextDouble() < 0.3) {
                engine.delete(key);
                expected.remove(key);
            } else {
                String value = "v" + random.nextInt(100_000);
                engine.put(key, value);
                expected.put(key, value);
            }
        }
        return expected;
    }

    private void assertMatches(LSMStoreEngine engine, Map<String, String> expected) {
        for (int i = 0; i < 60; i++) {
            String key = "key_" + i;
            assertEquals(Optional.ofNullable(expected.get(key)), engine.get(key),
                    "engine disagreed with the reference on " + key);
        }
    }

    @ParameterizedTest(name = "{0} bound={1} seed={2}")
    @MethodSource("runs")
    void theEngineAgreesWithAReferenceMap(DurabilityMode mode, long bound, long seed) {
        LSMStoreEngine engine = new LSMStoreEngine(dir, bound, mode);
        try {
            assertMatches(engine, drive(engine, seed, operationsFor(mode)));
        } finally {
            engine.close();
        }
    }

    @ParameterizedTest(name = "{0} bound={1} seed={2}")
    @MethodSource("runs")
    void theEngineStillAgreesAfterAReopen(DurabilityMode mode, long bound, long seed) {
        Map<String, String> expected;
        LSMStoreEngine engine = new LSMStoreEngine(dir, bound, mode);
        try {
            expected = drive(engine, seed, operationsFor(mode));
        } finally {
            engine.close();
        }

        // Whatever is in tables and whatever is still only in the log have to
        // add back up to the same store.
        LSMStoreEngine reopened = new LSMStoreEngine(dir, bound, mode);
        try {
            assertMatches(reopened, expected);
        } finally {
            reopened.close();
        }
    }
}
