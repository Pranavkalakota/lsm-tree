package lsm.wal;

import lsm.LSMStoreEngine;
import lsm.memtable.Entry;
import lsm.memtable.MemTable;
import lsm.memtable.VersionedKey;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Losing the tail of the log, which is what a power cut does to it.
 *
 * <p>A crash kills the process and leaves the operating system holding the
 * bytes, so everything written still reaches the disk. A power cut does not:
 * whatever had not been forced out is simply gone, and the log is left ending
 * at an arbitrary byte, very possibly in the middle of a record.
 *
 * <p>These tests cut the log at every byte offset there is and require the
 * same thing each time: recovery returns a <em>prefix</em> of what was
 * written. Not an error, not a partly applied record, and never a value that
 * was never written.
 *
 * <p><b>What this does not prove.</b> It models the page cache vanishing,
 * which is the half of a power cut that software can reproduce. It says
 * nothing about whether the drive honours {@code fsync} rather than parking
 * the bytes in its own volatile buffer and reporting success. That claim
 * needs a machine someone can actually unplug, and no test here establishes
 * it.
 */
class PowerCutTest {

    @TempDir
    Path dir;

    /** One record as it was handed to the log. */
    private record Written(String key, long version, String value) {
        boolean isDelete() {
            return value == null;
        }
    }

    /** Writes a log of random puts and deletes, returning them in order. */
    private List<Written> writeLog(Path path, Random random, int count) throws IOException {
        List<Written> written = new ArrayList<>();
        try (WriteAheadLog wal = new WriteAheadLog(path)) {
            for (int i = 0; i < count; i++) {
                String key = "key" + random.nextInt(6);
                long version = i + 1;
                if (random.nextInt(4) == 0) {
                    wal.appendDelete(key, version);
                    written.add(new Written(key, version, null));
                } else {
                    String value = "v" + random.nextInt(100_000);
                    wal.appendPut(key, value, version);
                    written.add(new Written(key, version, value));
                }
            }
        }
        return written;
    }

    /** The map a replay should produce from the first {@code count} records. */
    private Map<VersionedKey, Entry> prefixState(List<Written> written, int count) {
        Map<VersionedKey, Entry> state = new LinkedHashMap<>();
        for (Written record : written.subList(0, count)) {
            state.put(new VersionedKey(record.key(), record.version()),
                    record.isDelete() ? Entry.tombstone() : Entry.put(record.value()));
        }
        return state;
    }

    /** Copies the log, keeping only its first {@code bytes} bytes. */
    private Path truncatedTo(Path source, long bytes) throws IOException {
        Path cut = dir.resolve("cut.log");
        Files.copy(source, cut, StandardCopyOption.REPLACE_EXISTING);
        try (FileChannel channel = FileChannel.open(cut, StandardOpenOption.WRITE)) {
            channel.truncate(bytes);
        }
        return cut;
    }

    /**
     * The property, checked at every possible cut: whatever survives is
     * exactly the first N records, for some N.
     *
     * <p>Every record carries its own version, so each one occupies its own
     * slot in the recovered map and the count of what came back identifies
     * the prefix precisely. That makes this an equality check rather than a
     * vaguer "nothing looks wrong".
     */
    @ParameterizedTest(name = "seed={0}")
    @MethodSource("seeds")
    void everyTruncationLeavesAPrefixOfTheLog(long seed) throws IOException {
        Random random = new Random(seed);
        Path log = dir.resolve("wal.log");
        List<Written> written = writeLog(log, random, 25);
        long size = Files.size(log);
        assertTrue(size > 0);

        for (long cut = 0; cut <= size; cut++) {
            Path truncated = truncatedTo(log, cut);
            MemTable recovered = new MemTable(Long.MAX_VALUE);

            // A torn log is an ordinary thing to find after a power cut, so
            // replay has to cope with it rather than refuse to start.
            assertDoesNotThrow(() -> WriteAheadLog.replay(truncated, recovered),
                    "replay threw at a cut of " + cut + " bytes");

            Map<VersionedKey, Entry> actual = recovered.versionedEntries();
            assertTrue(actual.size() <= written.size(),
                    "recovered more records than were written, cut at " + cut);
            assertEquals(prefixState(written, actual.size()), actual,
                    "cut at " + cut + " bytes did not leave a clean prefix");
        }
    }

    /** Cutting mid-record must drop that record whole, never half of it. */
    @Test
    void aTornRecordIsDiscardedRatherThanPartlyApplied() throws IOException {
        Path log = dir.resolve("torn.log");
        try (WriteAheadLog wal = new WriteAheadLog(log)) {
            wal.appendPut("first", "complete", 1);
            wal.appendPut("second", "this value is long enough to cut through", 2);
        }
        long size = Files.size(log);

        // Every cut that lands inside the second record leaves the first
        // intact and the second absent. There is no offset at which half a
        // value becomes a value.
        for (long cut = size - 30; cut < size; cut++) {
            MemTable recovered = new MemTable(Long.MAX_VALUE);
            WriteAheadLog.replay(truncatedTo(log, cut), recovered);

            assertEquals("complete", recovered.get("first").value().orElseThrow(),
                    "the completed record should survive a cut at " + cut);
            assertNull(recovered.get("second"),
                    "a half-written record was applied at a cut of " + cut);
        }
    }

    @Test
    void aLogCutToNothingRecoversAnEmptyStore() throws IOException {
        Path log = dir.resolve("empty.log");
        try (WriteAheadLog wal = new WriteAheadLog(log)) {
            wal.appendPut("k", "v", 1);
        }

        MemTable recovered = new MemTable(Long.MAX_VALUE);
        WriteAheadLog.replay(truncatedTo(log, 0), recovered);
        assertEquals(0, recovered.versionCount());
    }

    /**
     * The same cut, but through the engine: a store whose log lost its tail
     * has to open, serve reads, and accept writes again.
     *
     * <p>Sampled rather than exhaustive, because each case opens a store
     * rather than replaying a file.
     */
    @ParameterizedTest(name = "seed={0}")
    @MethodSource("seeds")
    void aStoreOpensOnATruncatedLogAndKeepsWorking(long seed) throws IOException {
        Random random = new Random(seed);
        Path store = dir.resolve("store-" + seed);

        LSMStoreEngine engine = new LSMStoreEngine(store, 1 << 20,
                DurabilityMode.BUFFERED, 1);
        List<String> keys = new ArrayList<>();
        try {
            for (int i = 0; i < 30; i++) {
                String key = "key" + i;
                keys.add(key);
                engine.put(key, "value" + i);
            }
        } finally {
            engine.close();
        }

        Path log = store.resolve("wal_0.log");
        assertTrue(Files.exists(log), "expected an unflushed log to cut");
        long size = Files.size(log);

        for (int attempt = 0; attempt < 5; attempt++) {
            long cut = random.nextInt((int) size + 1);
            Path backup = dir.resolve("backup-" + seed + "-" + attempt + ".log");
            Files.copy(log, backup, StandardCopyOption.REPLACE_EXISTING);
            try (FileChannel channel = FileChannel.open(log, StandardOpenOption.WRITE)) {
                channel.truncate(cut);
            }

            LSMStoreEngine reopened = new LSMStoreEngine(store, 1 << 20,
                    DurabilityMode.BUFFERED, 1);
            try {
                // Whatever survived, the store has to be usable: every key
                // either reads back what was written or is simply absent.
                for (int i = 0; i < keys.size(); i++) {
                    Optional<String> found = reopened.get(keys.get(i));
                    if (found.isPresent()) {
                        assertEquals("value" + i, found.orElseThrow(),
                                "a surviving key came back changed, cut at " + cut);
                    }
                }
                reopened.put("after", "the cut");
                assertEquals("the cut", reopened.get("after").orElseThrow());
            } finally {
                reopened.close();
            }

            // Recovery rewrites the log, so restore the original for the next cut.
            Files.copy(backup, log, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private static Stream<Long> seeds() {
        return IntStream.range(0, 12).mapToObj(i -> 5500L + i);
    }
}
