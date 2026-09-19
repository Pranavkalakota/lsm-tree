package lsm;

import lsm.memtable.Entry;
import lsm.memtable.MemTable;
import lsm.wal.WriteAheadLog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class RobustnessTest {

    @TempDir
    Path tempDir;

    // --- Operations after close ---

    @Test
    void putAfterCloseThrows() {
        LSMStoreEngine engine = new LSMStoreEngine(tempDir);
        engine.put("k", "v");
        engine.close();
        assertThrows(IllegalStateException.class, () -> engine.put("k2", "v2"));
    }

    @Test
    void getAfterCloseThrows() {
        LSMStoreEngine engine = new LSMStoreEngine(tempDir);
        engine.put("k", "v");
        engine.close();
        assertThrows(IllegalStateException.class, () -> engine.get("k"));
    }

    @Test
    void deleteAfterCloseThrows() {
        LSMStoreEngine engine = new LSMStoreEngine(tempDir);
        engine.close();
        assertThrows(IllegalStateException.class, () -> engine.delete("k"));
    }

    @Test
    void doubleCloseDoesNotThrow() {
        LSMStoreEngine engine = new LSMStoreEngine(tempDir);
        engine.put("k", "v");
        engine.close();
        assertDoesNotThrow(engine::close);
    }

    // --- Encoding consistency ---

    @Test
    void unicodeSizeTrackingMatchesUtf8() {
        MemTable table = new MemTable(1024 * 1024);
        // Multi-byte UTF-8 characters
        String key = "éèê"; // e-acute, e-grave, e-circumflex: 2 bytes each in UTF-8
        String value = "☃";           // snowman: 3 bytes in UTF-8

        table.put(key, value);

        int expectedKeyBytes = key.getBytes(StandardCharsets.UTF_8).length; // 6
        int expectedValBytes = value.getBytes(StandardCharsets.UTF_8).length; // 3
        assertEquals(expectedKeyBytes + expectedValBytes, table.getSizeBytes());
    }

    @Test
    void fourByteUtf8SizeTracking() {
        MemTable table = new MemTable(1024 * 1024);
        // Emoji: 4 bytes in UTF-8, but only 2 chars in Java (surrogate pair)
        String key = "😀"; // grinning face
        String value = "🚀🚀"; // two rockets

        table.put(key, value);

        int expectedKeyBytes = key.getBytes(StandardCharsets.UTF_8).length; // 4
        int expectedValBytes = value.getBytes(StandardCharsets.UTF_8).length; // 8
        assertEquals(expectedKeyBytes + expectedValBytes, table.getSizeBytes());
    }

    // --- MemTable immutability ---

    @Test
    void entriesReturnsUnmodifiableView() {
        MemTable table = new MemTable(1024);
        table.put("a", "1");

        var entries = table.entries();
        assertThrows(UnsupportedOperationException.class, () ->
                entries.put("injected", Entry.put("hacked")));
    }

    // --- WAL corrupted mid-file (not just truncated tail) ---

    @Test
    void corruptedOpByteSkipsRestOfWal() throws IOException {
        Path walPath = tempDir.resolve("wal.log");

        // Write two valid records
        try (WriteAheadLog wal = new WriteAheadLog(walPath)) {
            wal.appendPut("first", "ok");
            wal.appendPut("second", "ok");
        }

        // Corrupt the op byte of the second record by replacing it
        // with an invalid op code (0xFF)
        byte[] walBytes = Files.readAllBytes(walPath);
        // First record: 1 byte op + 4 bytes keyLen + 5 bytes "first" + 4 bytes valLen + 2 bytes "ok" = 16
        int secondRecordOffset = 1 + 4 + 5 + 4 + 2;
        walBytes[secondRecordOffset] = (byte) 0xFF;
        Files.write(walPath, walBytes);

        MemTable table = new MemTable(1024 * 1024);
        WriteAheadLog.replay(walPath, table);

        // First record should survive, second is lost due to invalid op
        assertEquals("ok", table.get("first").value().orElseThrow());
        assertNull(table.get("second"));
    }

    @Test
    void corruptedKeyLenCausesGracefulStop() throws IOException {
        Path walPath = tempDir.resolve("wal.log");

        try (WriteAheadLog wal = new WriteAheadLog(walPath)) {
            wal.appendPut("good", "data");
        }

        // Append a record with an absurdly large key length (would OOM without bounds checking)
        try (DataOutputStream out = new DataOutputStream(
                new FileOutputStream(walPath.toFile(), true))) {
            out.writeByte(1); // OP_PUT
            out.writeInt(Integer.MAX_VALUE); // 2GB key — would OOM
            out.write("x".getBytes());
        }

        MemTable table = new MemTable(1024 * 1024);
        // Should NOT throw OutOfMemoryError — should stop at the corrupt record
        WriteAheadLog.replay(walPath, table);
        assertEquals("data", table.get("good").value().orElseThrow());
    }

    @Test
    void negativeKeyLenCausesGracefulStop() throws IOException {
        Path walPath = tempDir.resolve("wal.log");

        try (WriteAheadLog wal = new WriteAheadLog(walPath)) {
            wal.appendPut("survived", "yes");
        }

        // Append a record with a negative key length (bit-flip corruption)
        try (DataOutputStream out = new DataOutputStream(
                new FileOutputStream(walPath.toFile(), true))) {
            out.writeByte(1); // OP_PUT
            out.writeInt(-1); // negative length — corruption
        }

        MemTable table = new MemTable(1024 * 1024);
        WriteAheadLog.replay(walPath, table);
        assertEquals("yes", table.get("survived").value().orElseThrow());
    }

    @Test
    void corruptedValueLenCausesGracefulStop() throws IOException {
        Path walPath = tempDir.resolve("wal.log");

        try (WriteAheadLog wal = new WriteAheadLog(walPath)) {
            wal.appendPut("safe", "here");
        }

        // Valid key but absurdly large value length
        try (DataOutputStream out = new DataOutputStream(
                new FileOutputStream(walPath.toFile(), true))) {
            out.writeByte(1); // OP_PUT
            out.writeInt(3);
            out.write("key".getBytes());
            out.writeInt(Integer.MAX_VALUE); // 2GB value
            out.write("x".getBytes());
        }

        MemTable table = new MemTable(1024 * 1024);
        WriteAheadLog.replay(walPath, table);
        assertEquals("here", table.get("safe").value().orElseThrow());
        assertNull(table.get("key")); // corrupt record discarded
    }

    // --- WAL close/reopen ---

    @Test
    void walDoubleCloseDoesNotThrow() throws IOException {
        Path walPath = tempDir.resolve("wal.log");
        WriteAheadLog wal = new WriteAheadLog(walPath);
        wal.appendPut("k", "v");
        wal.close();
        assertDoesNotThrow(wal::close);
    }

    @Test
    void walWriteAfterCloseThrows() throws IOException {
        Path walPath = tempDir.resolve("wal.log");
        WriteAheadLog wal = new WriteAheadLog(walPath);
        wal.close();
        assertThrows(IOException.class, () -> wal.appendPut("k", "v"));
        assertThrows(IOException.class, () -> wal.appendDelete("k"));
    }

    // --- Data integrity: WAL content matches what engine returns ---

    @Test
    void walBytesMatchEngineState() throws IOException {
        Path dataDir = tempDir.resolve("db");
        LSMStoreEngine engine = new LSMStoreEngine(dataDir);
        engine.put("key1", "value1");
        engine.put("key2", "value2");
        engine.delete("key1");
        engine.close();

        // Replay the WAL independently and verify it matches engine behavior
        MemTable replayed = new MemTable(1024 * 1024);
        WriteAheadLog.replay(dataDir.resolve("wal.log"), replayed);

        assertTrue(replayed.get("key1").isTombstone());
        assertEquals("value2", replayed.get("key2").value().orElseThrow());
    }

    // --- Engine construction edge cases ---

    @Test
    void constructorCreatesDirectoryIfMissing() {
        Path nested = tempDir.resolve("deep/nested/dir");
        LSMStoreEngine engine = new LSMStoreEngine(nested);
        engine.put("k", "v");
        assertEquals(Optional.of("v"), engine.get("k"));
        engine.close();
        assertTrue(Files.isDirectory(nested));
    }

    @Test
    void constructorWithExistingEmptyDirectory() {
        LSMStoreEngine engine = new LSMStoreEngine(tempDir);
        assertEquals(Optional.empty(), engine.get("anything"));
        engine.close();
    }
}
