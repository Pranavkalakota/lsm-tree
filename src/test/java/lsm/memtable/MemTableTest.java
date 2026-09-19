package lsm.memtable;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class MemTableTest {

    private MemTable table;

    @BeforeEach
    void setUp() {
        table = new MemTable(1024);
    }

    @Test
    void putAndGet() {
        table.put("name", "alice");
        Entry entry = table.get("name");
        assertNotNull(entry);
        assertFalse(entry.isTombstone());
        assertEquals("alice", entry.value().orElseThrow());
    }

    @Test
    void getMissing() {
        assertNull(table.get("nope"));
    }

    @Test
    void overwrite() {
        table.put("k", "v1");
        table.put("k", "v2");
        assertEquals("v2", table.get("k").value().orElseThrow());
        assertEquals(1, table.entryCount());
    }

    @Test
    void deleteExisting() {
        table.put("k", "v");
        table.delete("k");
        Entry entry = table.get("k");
        assertNotNull(entry);
        assertTrue(entry.isTombstone());
    }

    @Test
    void deleteNonExisting() {
        table.delete("k");
        Entry entry = table.get("k");
        assertNotNull(entry);
        assertTrue(entry.isTombstone());
    }

    @Test
    void putAfterDelete() {
        table.put("k", "v1");
        table.delete("k");
        table.put("k", "v2");
        Entry entry = table.get("k");
        assertFalse(entry.isTombstone());
        assertEquals("v2", entry.value().orElseThrow());
    }

    @Test
    void sizeTracking() {
        assertTrue(table.isEmpty());
        assertEquals(0, table.getSizeBytes());

        table.put("key", "value");
        assertTrue(table.getSizeBytes() > 0);
        assertEquals(1, table.entryCount());
    }

    @Test
    void shouldFlush() {
        MemTable small = new MemTable(10);
        assertFalse(small.shouldFlush());
        small.put("a-long-key", "a-long-value");
        assertTrue(small.shouldFlush());
    }

    @Test
    void clear() {
        table.put("a", "1");
        table.put("b", "2");
        table.clear();
        assertTrue(table.isEmpty());
        assertEquals(0, table.getSizeBytes());
        assertNull(table.get("a"));
    }

    @Test
    void entriesAreSorted() {
        table.put("cherry", "3");
        table.put("apple", "1");
        table.put("banana", "2");

        var keys = table.entries().keySet().stream().toList();
        assertEquals("apple", keys.get(0));
        assertEquals("banana", keys.get(1));
        assertEquals("cherry", keys.get(2));
    }

    @Test
    void sizeDecreasesOnOverwrite() {
        table.put("k", "a-very-long-value-that-takes-space");
        long sizeBefore = table.getSizeBytes();
        table.put("k", "short");
        assertTrue(table.getSizeBytes() < sizeBefore);
    }
}
