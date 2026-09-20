package lsm.sstable;

import lsm.memtable.Entry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.*;

class BlockCacheTest {

    @TempDir
    Path dir;

    private Path write(String name, int count) throws IOException {
        TreeMap<String, Entry> map = new TreeMap<>();
        for (int i = 0; i < count; i++) {
            map.put(String.format("key_%05d", i), Entry.put("value_" + i));
        }
        Path path = dir.resolve(name);
        SSTableWriter.write(path, map);
        return path;
    }

    @Test
    void secondReadIsServedFromMemory() throws IOException {
        Path path = write("table.sst", 5);
        BlockCache cache = new BlockCache(1024 * 1024);

        try (SSTableReader reader = new SSTableReader(path, cache)) {
            assertEquals("value_0", reader.get("key_00000").value().orElseThrow());

            // Wreck the file underneath the open reader. A read that still
            // answers correctly can only have come from the cache, and a read
            // that reached disk would fail its checksum.
            byte[] bytes = Files.readAllBytes(path);
            for (int i = 0; i < bytes.length - SSTableWriter.FOOTER_SIZE; i++) {
                bytes[i] ^= 0x7F;
            }
            Files.write(path, bytes);

            assertEquals("value_1", reader.get("key_00001").value().orElseThrow());
        }
    }

    @Test
    void oneBlockIsStoredOncePerTable() throws IOException {
        Path path = write("table.sst", 5);
        BlockCache cache = new BlockCache(1024 * 1024);

        try (SSTableReader reader = new SSTableReader(path, cache)) {
            for (int i = 0; i < 5; i++) {
                reader.get(String.format("key_%05d", i));
            }
            assertEquals(1, cache.blockCount(),
                    "five keys in one block should occupy one cache entry");
        }
    }

    @Test
    void separateReadersShareTheCache() throws IOException {
        Path path = write("table.sst", 5);
        BlockCache cache = new BlockCache(1024 * 1024);

        try (SSTableReader first = new SSTableReader(path, cache);
             SSTableReader second = new SSTableReader(path, cache)) {
            first.get("key_00000");
            second.get("key_00001");
            assertEquals(1, cache.blockCount(),
                    "both readers cover the same block, so it should be held once");
        }
    }

    @Test
    void blocksFromDifferentTablesDoNotCollide() throws IOException {
        Path one = write("L0_000000.sst", 5);
        Path two = write("L0_000001.sst", 5);
        BlockCache cache = new BlockCache(1024 * 1024);

        try (SSTableReader first = new SSTableReader(one, cache);
             SSTableReader second = new SSTableReader(two, cache)) {
            first.get("key_00000");
            second.get("key_00000");
            // Identical contents at identical offsets; only the path separates them.
            assertEquals(2, cache.blockCount());
        }
    }

    @Test
    void staysCorrectWhenTooSmallToHoldEverything() throws IOException {
        Path path = write("table.sst", 4_000);
        BlockCache tiny = new BlockCache(SSTableWriter.BLOCK_SIZE);

        try (SSTableReader reader = new SSTableReader(path, tiny)) {
            for (int i = 0; i < 4_000; i++) {
                assertEquals("value_" + i,
                        reader.get(String.format("key_%05d", i)).value().orElseThrow(),
                        "lost key at position " + i + " under eviction pressure");
            }
        }
    }

    @Test
    void aBlockThatFailedItsChecksumIsNotRemembered() throws IOException {
        Path path = write("table.sst", 5);
        byte[] bytes = Files.readAllBytes(path);
        bytes[8] ^= 0x01;
        Files.write(path, bytes);

        BlockCache cache = new BlockCache(1024 * 1024);
        try (SSTableReader reader = new SSTableReader(path, cache)) {
            assertThrows(IOException.class, () -> reader.get("key_00000"));
            assertEquals(0, cache.blockCount(), "a failed load must not be cached");
            // Still reports the fault rather than a stale or empty hit.
            assertThrows(IOException.class, () -> reader.get("key_00000"));
        }
    }
}
