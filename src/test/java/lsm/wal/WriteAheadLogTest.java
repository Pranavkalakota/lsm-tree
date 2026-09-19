package lsm.wal;

import lsm.memtable.MemTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class WriteAheadLogTest {

    @TempDir
    Path tempDir;

    @Test
    void replayPuts() throws IOException {
        Path walPath = tempDir.resolve("wal.log");

        try (WriteAheadLog wal = new WriteAheadLog(walPath)) {
            wal.appendPut("a", "1");
            wal.appendPut("b", "2");
            wal.appendPut("c", "3");
        }

        MemTable table = new MemTable(1024 * 1024);
        WriteAheadLog.replay(walPath, table);

        assertEquals("1", table.get("a").value().orElseThrow());
        assertEquals("2", table.get("b").value().orElseThrow());
        assertEquals("3", table.get("c").value().orElseThrow());
    }

    @Test
    void replayDeletes() throws IOException {
        Path walPath = tempDir.resolve("wal.log");

        try (WriteAheadLog wal = new WriteAheadLog(walPath)) {
            wal.appendPut("a", "1");
            wal.appendDelete("a");
        }

        MemTable table = new MemTable(1024 * 1024);
        WriteAheadLog.replay(walPath, table);

        assertNotNull(table.get("a"));
        assertTrue(table.get("a").isTombstone());
    }

    @Test
    void replayOverwrites() throws IOException {
        Path walPath = tempDir.resolve("wal.log");

        try (WriteAheadLog wal = new WriteAheadLog(walPath)) {
            wal.appendPut("k", "old");
            wal.appendPut("k", "new");
        }

        MemTable table = new MemTable(1024 * 1024);
        WriteAheadLog.replay(walPath, table);

        assertEquals("new", table.get("k").value().orElseThrow());
    }

    @Test
    void replayEmptyFile() throws IOException {
        Path walPath = tempDir.resolve("wal.log");
        Files.createFile(walPath);

        MemTable table = new MemTable(1024 * 1024);
        WriteAheadLog.replay(walPath, table);
        assertTrue(table.isEmpty());
    }

    @Test
    void replayNonExistentFile() throws IOException {
        Path walPath = tempDir.resolve("doesnt-exist.log");

        MemTable table = new MemTable(1024 * 1024);
        WriteAheadLog.replay(walPath, table);
        assertTrue(table.isEmpty());
    }

    @Test
    void resetClearsWal() throws IOException {
        Path walPath = tempDir.resolve("wal.log");

        try (WriteAheadLog wal = new WriteAheadLog(walPath)) {
            wal.appendPut("a", "1");
            wal.appendPut("b", "2");
            wal.reset();
        }

        MemTable table = new MemTable(1024 * 1024);
        WriteAheadLog.replay(walPath, table);
        assertTrue(table.isEmpty());
    }

    @Test
    void replayUnicodeKeys() throws IOException {
        Path walPath = tempDir.resolve("wal.log");

        try (WriteAheadLog wal = new WriteAheadLog(walPath)) {
            wal.appendPut("café", "coffee");
            wal.appendPut("☃", "snowman");
        }

        MemTable table = new MemTable(1024 * 1024);
        WriteAheadLog.replay(walPath, table);

        assertEquals("coffee", table.get("café").value().orElseThrow());
        assertEquals("snowman", table.get("☃").value().orElseThrow());
    }
}
