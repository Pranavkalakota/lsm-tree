package lsm.sstable;

import lsm.memtable.Entry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.*;

class SSTableTest {

    @TempDir
    Path dir;

    private Path write(Map<String, Entry> entries) throws IOException {
        Path path = dir.resolve("table.sst");
        SSTableWriter.write(path, entries);
        return path;
    }

    private static TreeMap<String, Entry> entries(String... keyValuePairs) {
        TreeMap<String, Entry> map = new TreeMap<>();
        for (int i = 0; i < keyValuePairs.length; i += 2) {
            map.put(keyValuePairs[i], Entry.put(keyValuePairs[i + 1]));
        }
        return map;
    }

    // --- round trip ---

    @Test
    void readsBackASingleEntry() throws IOException {
        Path path = write(entries("hello", "world"));
        try (SSTableReader reader = new SSTableReader(path)) {
            Entry found = reader.get("hello");
            assertNotNull(found);
            assertFalse(found.isTombstone());
            assertEquals("world", found.value().orElseThrow());
        }
    }

    @Test
    void readsBackEveryEntry() throws IOException {
        Path path = write(entries("apple", "red", "banana", "yellow", "cherry", "crimson"));
        try (SSTableReader reader = new SSTableReader(path)) {
            assertEquals("red", reader.get("apple").value().orElseThrow());
            assertEquals("yellow", reader.get("banana").value().orElseThrow());
            assertEquals("crimson", reader.get("cherry").value().orElseThrow());
        }
    }

    @Test
    void absentKeyReturnsNullRatherThanTombstone() throws IOException {
        Path path = write(entries("present", "yes"));
        try (SSTableReader reader = new SSTableReader(path)) {
            assertNull(reader.get("absent"));
        }
    }

    @Test
    void tombstoneSurvivesTheRoundTrip() throws IOException {
        TreeMap<String, Entry> map = entries("alive", "value");
        map.put("deleted", Entry.tombstone());
        Path path = write(map);

        try (SSTableReader reader = new SSTableReader(path)) {
            assertFalse(reader.get("alive").isTombstone());
            assertTrue(reader.get("deleted").isTombstone());
        }
    }

    // --- sparse index navigation ---

    @Test
    void findsKeysInEveryIndexBlock() throws IOException {
        TreeMap<String, Entry> map = new TreeMap<>();
        for (int i = 0; i < 200; i++) {
            map.put(String.format("key_%04d", i), Entry.put("value_" + i));
        }
        Path path = write(map);

        try (SSTableReader reader = new SSTableReader(path)) {
            for (int i = 0; i < 200; i++) {
                assertEquals("value_" + i,
                        reader.get(String.format("key_%04d", i)).value().orElseThrow(),
                        "missing key at position " + i);
            }
        }
    }

    @Test
    void missesKeysThatFallBetweenStoredKeys() throws IOException {
        TreeMap<String, Entry> map = new TreeMap<>();
        for (int i = 0; i < 100; i += 2) {
            map.put(String.format("key_%03d", i), Entry.put("v" + i));
        }
        Path path = write(map);

        try (SSTableReader reader = new SSTableReader(path)) {
            assertNotNull(reader.get("key_000"));
            assertNotNull(reader.get("key_050"));
            assertNull(reader.get("key_001"));
            assertNull(reader.get("key_051"));
        }
    }

    @Test
    void missesKeysOutsideTheStoredRange() throws IOException {
        TreeMap<String, Entry> map = new TreeMap<>();
        for (int i = 0; i < 100; i++) {
            map.put(String.format("key_%03d", i), Entry.put("v" + i));
        }
        Path path = write(map);

        try (SSTableReader reader = new SSTableReader(path)) {
            assertNull(reader.get("aaa_before_everything"));
            assertNull(reader.get("zzz_after_everything"));
        }
    }

    // --- awkward payloads ---

    @Test
    void handlesAnEmptyTable() throws IOException {
        Path path = write(new TreeMap<>());
        try (SSTableReader reader = new SSTableReader(path)) {
            assertNull(reader.get("anything"));
        }
    }

    @Test
    void handlesEmptyKeyAndEmptyValue() throws IOException {
        Path path = write(entries("", ""));
        try (SSTableReader reader = new SSTableReader(path)) {
            assertEquals("", reader.get("").value().orElseThrow());
        }
    }

    @Test
    void handlesMultiByteAndSupplementaryCharacters() throws IOException {
        Path path = write(entries("日本語", "japanese", "snowman_☃", "frozen", "rocket_🚀", "liftoff"));
        try (SSTableReader reader = new SSTableReader(path)) {
            assertEquals("japanese", reader.get("日本語").value().orElseThrow());
            assertEquals("frozen", reader.get("snowman_☃").value().orElseThrow());
            assertEquals("liftoff", reader.get("rocket_🚀").value().orElseThrow());
        }
    }

    @Test
    void handlesValuesLargerThanTheScanBuffer() throws IOException {
        String big = "x".repeat(512 * 1024);
        TreeMap<String, Entry> map = entries("before", "small", "huge", big);
        map.put("zafter", Entry.put("small"));
        Path path = write(map);

        try (SSTableReader reader = new SSTableReader(path)) {
            assertEquals(big, reader.get("huge").value().orElseThrow());
            // A record wider than the scan buffer must not derail neighbouring reads.
            assertEquals("small", reader.get("zafter").value().orElseThrow());
        }
    }

    @Test
    void handlesKeysLargerThanTheScanBuffer() throws IOException {
        String bigKey = "k".repeat(64 * 1024);
        Path path = write(entries(bigKey, "value", "short", "other"));
        try (SSTableReader reader = new SSTableReader(path)) {
            assertEquals("value", reader.get(bigKey).value().orElseThrow());
            assertEquals("other", reader.get("short").value().orElseThrow());
        }
    }

    // --- format and durability ---

    @Test
    void footerCarriesTheMagicNumberAndVersion() throws IOException {
        Path path = write(entries("k", "v"));
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            ByteBuffer footer = ByteBuffer.allocate(SSTableWriter.FOOTER_SIZE);
            channel.read(footer, channel.size() - SSTableWriter.FOOTER_SIZE);
            footer.flip();
            footer.getLong();   // indexOffset
            footer.getInt();    // blockCount
            footer.getLong();   // metaOffset
            assertEquals(SSTableWriter.FORMAT_VERSION, footer.getInt());
            assertEquals(SSTableWriter.MAGIC, footer.getLong());
        }
    }

    @Test
    void rejectsAFileWithTheWrongMagicNumber() throws IOException {
        Path path = write(entries("k", "v"));
        byte[] bytes = Files.readAllBytes(path);
        bytes[bytes.length - 1] ^= 0xFF;
        Files.write(path, bytes);

        assertThrows(IOException.class, () -> new SSTableReader(path));
    }

    @Test
    void rejectsAFileShorterThanTheFooter() throws IOException {
        Path path = dir.resolve("stub.sst");
        Files.write(path, new byte[8]);
        assertThrows(IOException.class, () -> new SSTableReader(path));
    }

    @Test
    void rejectsAnUnknownFormatVersion() throws IOException {
        Path path = write(entries("k", "v"));
        byte[] bytes = Files.readAllBytes(path);
        // formatVersion sits in the 4 bytes before the trailing 8 byte magic.
        bytes[bytes.length - 9] = 99;
        Files.write(path, bytes);

        assertThrows(IOException.class, () -> new SSTableReader(path));
    }

    @Test
    void leavesNoTempFileBehind() throws IOException {
        write(entries("k", "v"));
        try (var files = Files.list(dir)) {
            assertTrue(files.noneMatch(p -> p.toString().endsWith(".tmp")));
        }
    }

    @Test
    void refusesToOverwriteAnExistingTable() throws IOException {
        Path path = write(entries("k", "v"));
        assertThrows(IOException.class, () -> SSTableWriter.write(path, entries("k", "other")));
        try (SSTableReader reader = new SSTableReader(path)) {
            assertEquals("v", reader.get("k").value().orElseThrow());
        }
    }

    // --- block checksums ---

    private static TreeMap<String, Entry> manyEntries(int count) {
        TreeMap<String, Entry> map = new TreeMap<>();
        for (int i = 0; i < count; i++) {
            map.put(String.format("key_%05d", i), Entry.put("value_" + i));
        }
        return map;
    }

    private int blockCount(Path path) throws IOException {
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            ByteBuffer footer = ByteBuffer.allocate(SSTableWriter.FOOTER_SIZE);
            channel.read(footer, channel.size() - SSTableWriter.FOOTER_SIZE);
            footer.flip();
            footer.getLong();
            return footer.getInt();
        }
    }

    @Test
    void splitsLargeTablesAcrossSeveralBlocks() throws IOException {
        Path path = write(manyEntries(2_000));
        assertTrue(blockCount(path) > 1,
                "2000 records should not fit in a single " + SSTableWriter.BLOCK_SIZE + " byte block");
    }

    @Test
    void findsEveryKeyAcrossBlockBoundaries() throws IOException {
        Path path = write(manyEntries(2_000));
        try (SSTableReader reader = new SSTableReader(path)) {
            for (int i = 0; i < 2_000; i++) {
                assertEquals("value_" + i,
                        reader.get(String.format("key_%05d", i)).value().orElseThrow(),
                        "lost key at position " + i);
            }
        }
    }

    @Test
    void detectsAFlippedBitInBlockData() throws IOException {
        Path path = write(manyEntries(500));
        byte[] bytes = Files.readAllBytes(path);
        // Land inside the first block's records, well clear of index and footer.
        bytes[64] ^= 0x01;
        Files.write(path, bytes);

        try (SSTableReader reader = new SSTableReader(path)) {
            IOException failure = assertThrows(IOException.class, () -> reader.get("key_00000"));
            assertTrue(failure.getMessage().contains("Checksum"),
                    "expected a checksum failure, got: " + failure.getMessage());
        }
    }

    @Test
    void detectsACorruptedChecksumItself() throws IOException {
        Path path = write(entries("only", "record"));
        byte[] bytes = Files.readAllBytes(path);

        // The sole block runs up to the index, so its checksum is the four
        // bytes immediately before indexOffset. Read that from the footer
        // rather than deriving it, so the test survives layout changes.
        ByteBuffer footer = ByteBuffer.wrap(bytes, bytes.length - SSTableWriter.FOOTER_SIZE,
                SSTableWriter.FOOTER_SIZE);
        int indexStart = (int) footer.getLong();
        bytes[indexStart - 1] ^= 0x01;
        Files.write(path, bytes);

        try (SSTableReader reader = new SSTableReader(path)) {
            assertThrows(IOException.class, () -> reader.get("only"));
        }
    }

    @Test
    void corruptionInOneBlockLeavesOthersReadable() throws IOException {
        Path path = write(manyEntries(2_000));
        assertTrue(blockCount(path) > 1);

        byte[] bytes = Files.readAllBytes(path);
        bytes[64] ^= 0x01;
        Files.write(path, bytes);

        try (SSTableReader reader = new SSTableReader(path)) {
            assertThrows(IOException.class, () -> reader.get("key_00000"));
            // A later block is untouched and must still answer.
            assertEquals("value_1999", reader.get("key_01999").value().orElseThrow());
        }
    }

    @Test
    void openFailureDoesNotLeakTheChannel() throws IOException {
        Path path = dir.resolve("broken.sst");
        Files.write(path, new byte[4]);
        assertThrows(IOException.class, () -> new SSTableReader(path));
        // The reader must have closed its channel, or Windows-style locks would
        // block this delete and the engine would leak a descriptor per bad file.
        assertTrue(Files.deleteIfExists(path));
    }
}
