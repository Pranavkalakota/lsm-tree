package lsm;

import lsm.memtable.Entry;
import lsm.memtable.MemTable;
import lsm.memtable.VersionedKey;
import lsm.sstable.SSTableReader;
import lsm.wal.DurabilityMode;
import lsm.wal.WriteAheadLog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The engine stamping a version on every write, and keeping those versions
 * climbing across a flush and a reopen.
 */
class VersionAssignmentTest {

    @TempDir
    Path dir;

    private LSMStoreEngine open(long memTableBytes) {
        return new LSMStoreEngine(dir, memTableBytes, DurabilityMode.BUFFERED, 1);
    }

    private List<Path> tables() throws IOException {
        try (Stream<Path> files = Files.list(dir)) {
            return files.filter(p -> p.getFileName().toString().endsWith(".sst"))
                    .sorted()
                    .toList();
        }
    }

    private void dropLogs() throws IOException {
        try (Stream<Path> files = Files.list(dir)) {
            for (Path path : files.toList()) {
                if (path.getFileName().toString().startsWith("wal")) {
                    Files.delete(path);
                }
            }
        }
    }

    /** Highest version any table on disk carries. */
    private long highestVersionOnDisk() throws IOException {
        long highest = 0;
        for (Path path : tables()) {
            try (SSTableReader reader = new SSTableReader(path)) {
                highest = Math.max(highest, reader.maxVersion());
            }
        }
        return highest;
    }

    @Test
    void writesAreStampedWithClimbingVersions() throws IOException {
        LSMStoreEngine engine = open(60);
        try {
                for (int i = 0; i < 40; i++) {
                    engine.put("key" + i, "value" + i);
                }
        
        } finally {
            engine.close();
        }
        assertTrue(highestVersionOnDisk() > 1,
                "a store that wrote 40 times should hold versions above the first");
    }

    @Test
    void aFlushKeepsEveryVersionOfAKey() throws IOException {
        LSMStoreEngine engine = open(40);
        try {
                for (int i = 1; i <= 8; i++) {
                    engine.put("k", "value" + i);
                }
                assertEquals("value8", engine.get("k").orElseThrow());
        
        } finally {
            engine.close();
        }

        // The history has to leave memory intact, or a snapshot read could only
        // ever see as far back as the last flush.
        int versionsOfK = 0;
        for (Path path : tables()) {
            try (SSTableReader reader = new SSTableReader(path)) {
                SSTableReader.Cursor cursor = reader.cursor();
                while (cursor.hasNext()) {
                    if (cursor.next().key().equals("k")) {
                        versionsOfK++;
                    }
                }
            }
        }
        assertTrue(versionsOfK > 1,
                "expected several versions of \"k\" on disk, found " + versionsOfK);
    }

    @Test
    void versionsDoNotRestartAfterAReopen() throws IOException {
        LSMStoreEngine engine = open(40);
        try {
                for (int i = 0; i < 20; i++) {
                    engine.put("key" + i, "first");
                }
        
        } finally {
            engine.close();
        }
        long afterFirstRun = highestVersionOnDisk();
        assertTrue(afterFirstRun > 0, "the first run should have written versions");

        // Reopening must resume above everything already stored. A counter that
        // restarts hands a NEW write a version an OLD record already used, and
        // the new value then loses to the stale one on any version-ordered read.
        LSMStoreEngine reopened = open(40);
        try {
                for (int i = 0; i < 20; i++) {
                    reopened.put("later" + i, "second");
                }
        
        } finally {
            reopened.close();
        }
        assertTrue(highestVersionOnDisk() > afterFirstRun,
                "versions restarted: " + highestVersionOnDisk() + " is not above "
                        + afterFirstRun);
    }

    /**
     * The counter has to be recoverable from table metadata alone, because a
     * flush empties the log: "tables hold versions, the log holds none" is an
     * ordinary state to reopen from, not an exotic one. Reached here by
     * removing logs a flush has already made redundant.
     */
    @Test
    void versionsResumeAboveTablesWithNoLogToReadFrom() throws IOException {
        LSMStoreEngine engine = open(40);
        try {
            for (int i = 0; i < 200; i++) {
                engine.put("key" + i, "first");
            }
        } finally {
            engine.close();
        }

        long afterFirstRun = highestVersionOnDisk();
        assertTrue(afterFirstRun > 100, "expected a long first run, got " + afterFirstRun);
        assertFalse(tables().isEmpty(), "this case needs tables on disk");
        dropLogs();

        LSMStoreEngine reopened = open(40);
        try {
            for (int i = 0; i < 40; i++) {
                reopened.put("later" + i, "second");
            }
        } finally {
            reopened.close();
        }

        assertTrue(highestVersionOnDisk() > afterFirstRun,
                "versions restarted from the tables' metadata: "
                        + highestVersionOnDisk() + " is not above " + afterFirstRun);
    }

    @Test
    void versionsResumeAboveAnUnflushedLog() throws IOException {
        // Nothing flushes here, so the only record of these versions is the log.
        LSMStoreEngine engine = open(1 << 20);
        try {
                for (int i = 0; i < 30; i++) {
                    engine.put("key" + i, "first");
                }
        
        } finally {
            engine.close();
        }
        assertEquals(0, tables().size(), "this case is only interesting with no tables");

        LSMStoreEngine reopened = open(40);
        try {
                reopened.put("after", "recovery");
                assertEquals("recovery", reopened.get("after").orElseThrow());
                // Everything the log held is still readable, at its own version.
                assertEquals("first", reopened.get("key7").orElseThrow());
        
        } finally {
            reopened.close();
        }
        assertTrue(highestVersionOnDisk() > 30,
                "replayed versions should have pushed the counter past them, got "
                        + highestVersionOnDisk());
    }

    @Test
    void recoveryKeepsTheValueAKeyEndedOn() throws IOException {
        LSMStoreEngine engine = open(1 << 20);
        try {
                engine.put("k", "one");
                engine.put("k", "two");
                engine.put("k", "three");
                engine.delete("other");
        
        } finally {
            engine.close();
        }
        LSMStoreEngine reopened = open(1 << 20);
        try {
                assertEquals("three", reopened.get("k").orElseThrow());
                assertTrue(reopened.get("other").isEmpty());
        
        } finally {
            reopened.close();
        }
    }

    @Test
    void theLogRoundTripsAVersion() throws IOException {
        Path log = dir.resolve("versions.log");
        try (WriteAheadLog wal = new WriteAheadLog(log)) {
            wal.appendPut("k", "late", 42);
            wal.appendPut("k", "early", 7);
            wal.appendDelete("gone", 50);
        }

        MemTable replayed = new MemTable(1 << 20);
        WriteAheadLog.replay(log, replayed);

        assertEquals(50, replayed.maxVersion());
        assertEquals("late", replayed.get("k").value().orElseThrow());
        assertEquals("early", replayed.get("k", 7).value().orElseThrow());
        assertNull(replayed.get("k", 6));
        assertTrue(replayed.get("gone").isTombstone());
    }

    /**
     * A log written before versions existed still has to replay, because the
     * one time an old log is read is after a crash.
     */
    @Test
    void anUnversionedLogStillReplays() throws IOException {
        Path log = dir.resolve("legacy.log");
        try (OutputStream file = Files.newOutputStream(log);
             DataOutputStream out = new DataOutputStream(file)) {
            byte[] key = "legacy".getBytes(StandardCharsets.UTF_8);
            byte[] value = "kept".getBytes(StandardCharsets.UTF_8);
            out.writeByte(1);              // the old OP_PUT, with no version field
            out.writeInt(key.length);
            out.write(key);
            out.writeInt(value.length);
            out.write(value);

            byte[] gone = "dropped".getBytes(StandardCharsets.UTF_8);
            out.writeByte(2);              // the old OP_DELETE
            out.writeInt(gone.length);
            out.write(gone);
        }

        MemTable replayed = new MemTable(1 << 20);
        WriteAheadLog.replay(log, replayed);

        assertEquals("kept", replayed.get("legacy").value().orElseThrow());
        assertTrue(replayed.get("dropped").isTombstone());
        assertEquals(0, replayed.maxVersion(), "records with no version are version zero");
    }

    @Test
    void clearingTheMemtableDoesNotRewindTheVersionItReached() {
        MemTable table = new MemTable(1 << 20);
        table.put("k", "v", 99);
        table.clear();
        // Versions are global and only climb; the map is merely what has not
        // been flushed. Rewinding here would reissue a version already used.
        assertEquals(99, table.maxVersion());
    }

    @Test
    void everyWriteGetsItsOwnVersion() throws IOException {
        LSMStoreEngine engine = open(1 << 20);
        try {
                for (int i = 0; i < 25; i++) {
                    engine.put("k", "v" + i);
                }
        
        } finally {
            engine.close();
        }
        // Held in one MemTable, so recovery replays all 25 versions of the key.
        MemTable replayed = new MemTable(1 << 20);
        List<Path> logs = new ArrayList<>();
        try (Stream<Path> files = Files.list(dir)) {
            files.filter(p -> p.getFileName().toString().startsWith("wal"))
                    .forEach(logs::add);
        }
        for (Path log : logs) {
            WriteAheadLog.replay(log, replayed);
        }

        assertEquals(25, replayed.versionCount(),
                "25 writes to one key should be 25 distinct versions, not an overwrite");
        assertEquals(1, replayed.entryCount());
    }

    @Test
    void tablesRecordTheVersionTheyReached() throws IOException {
        LSMStoreEngine engine = open(40);
        try {
                for (int i = 0; i < 30; i++) {
                    engine.put("key" + i, "v");
                }
        
        } finally {
            engine.close();
        }
        for (Path path : tables()) {
            try (SSTableReader reader = new SSTableReader(path)) {
                SSTableReader.Cursor cursor = reader.cursor();
                long highest = 0;
                while (cursor.hasNext()) {
                    SSTableReader.Row row = cursor.next();
                    highest = Math.max(highest, row.version());
                }
                assertEquals(highest, reader.maxVersion(),
                        "metadata disagrees with the records in " + path.getFileName());
            }
        }
    }

    @Test
    void aStoredVersionCanBeReadBackFromTheTable() throws IOException {
        LSMStoreEngine engine = open(40);
        try {
                for (int i = 1; i <= 10; i++) {
                    engine.put("k", "value" + i);
                }
        
        } finally {
            engine.close();
        }

        // Whatever version each table holds for "k", reading at that version
        // must give back the value written then.
        for (Path path : tables()) {
            try (SSTableReader reader = new SSTableReader(path)) {
                SSTableReader.Cursor cursor = reader.cursor();
                while (cursor.hasNext()) {
                    SSTableReader.Row row = cursor.next();
                    if (!row.key().equals("k")) {
                        continue;
                    }
                    Entry at = reader.get("k", row.version());
                    assertNotNull(at);
                    assertEquals(row.value().value().orElseThrow(),
                            at.value().orElseThrow(),
                            "version " + row.version() + " in " + path.getFileName());
                }
            }
        }
    }

    @Test
    void versionedKeysSurviveAFlushInStoredOrder() throws IOException {
        LSMStoreEngine engine = open(40);
        try {
                for (int i = 0; i < 30; i++) {
                    engine.put("key" + (i % 5), "v" + i);
                }
        
        } finally {
            engine.close();
        }
        for (Path path : tables()) {
            try (SSTableReader reader = new SSTableReader(path)) {
                SSTableReader.Cursor cursor = reader.cursor();
                VersionedKey previous = null;
                while (cursor.hasNext()) {
                    VersionedKey current = cursor.next().versionedKey();
                    if (previous != null) {
                        assertTrue(previous.compareTo(current) < 0,
                                previous + " then " + current + " is out of order");
                    }
                    previous = current;
                }
            }
        }
    }
}
