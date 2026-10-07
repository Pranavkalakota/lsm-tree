package lsm.sstable;

import lsm.memtable.Entry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Random;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Block compression.
 *
 * <p>Compression is invisible to a caller by design, so most of this is about
 * proving it stays invisible: same keys, same values, same failure behaviour,
 * whichever way the bytes were stored.
 */
class CompressionTest {

    @TempDir
    Path dir;

    private static TreeMap<String, Entry> compressible(int count) {
        TreeMap<String, Entry> entries = new TreeMap<>();
        for (int i = 0; i < count; i++) {
            entries.put(String.format("user:%06d:profile", i),
                    Entry.put("status=active;tier=free;region=us-east-1"));
        }
        return entries;
    }

    /**
     * High entropy in the keys as well as the values.
     *
     * <p>Ordered keys compress even when the values do not, which is enough to
     * keep a block worth compressing. Randomising both is what actually
     * produces blocks LZ4 cannot shrink.
     */
    private static TreeMap<String, Entry> incompressible(int count, long seed) {
        TreeMap<String, Entry> entries = new TreeMap<>();
        Random random = new Random(seed);
        for (int i = 0; i < count; i++) {
            entries.put(randomAscii(random, 24), Entry.put(randomAscii(random, 64)));
        }
        return entries;
    }

    private static String randomAscii(Random random, int length) {
        StringBuilder out = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            out.append((char) (0x21 + random.nextInt(94)));
        }
        return out.toString();
    }

    private Path write(String name, TreeMap<String, Entry> entries, CompressionType type)
            throws IOException {
        Path path = dir.resolve(name + ".sst");
        try (SSTableWriter writer = SSTableWriter.create(path, entries.size(), type)) {
            for (var entry : entries.entrySet()) {
                writer.add(entry.getKey(), entry.getValue());
            }
            writer.finish();
        }
        return path;
    }

    // --- invisible to the caller ---

    @ParameterizedTest
    @EnumSource(CompressionType.class)
    void everyEntrySurvivesWhicheverWayItIsStored(CompressionType type) throws IOException {
        TreeMap<String, Entry> expected = compressible(3_000);
        expected.put("user:000500:profile", Entry.tombstone());
        Path path = write("table", expected, type);

        try (SSTableReader reader = new SSTableReader(path)) {
            for (var entry : expected.entrySet()) {
                Entry found = reader.get(entry.getKey());
                assertNotNull(found, "lost " + entry.getKey());
                assertEquals(entry.getValue().isTombstone(), found.isTombstone());
                assertEquals(entry.getValue().value(), found.value());
            }
        }
    }

    @ParameterizedTest
    @EnumSource(CompressionType.class)
    void scanningWorksWhicheverWayItIsStored(CompressionType type) throws IOException {
        TreeMap<String, Entry> expected = compressible(2_000);
        Path path = write("table", expected, type);

        try (SSTableReader reader = new SSTableReader(path)) {
            SSTableReader.Cursor cursor = reader.cursor();
            int seen = 0;
            String previous = null;
            while (cursor.hasNext()) {
                SSTableReader.Row row = cursor.next();
                if (previous != null) {
                    assertTrue(row.key().compareTo(previous) > 0);
                }
                previous = row.key();
                seen++;
            }
            assertEquals(expected.size(), seen);
        }
    }

    @ParameterizedTest
    @EnumSource(CompressionType.class)
    void multiBlockTablesRoundTrip(CompressionType type) throws IOException {
        TreeMap<String, Entry> expected = compressible(5_000);
        Path path = write("table", expected, type);

        try (SSTableReader reader = new SSTableReader(path)) {
            assertTrue(reader.blockCount() > 1, "test needs several blocks");
            assertEquals("status=active;tier=free;region=us-east-1",
                    reader.get("user:004999:profile").value().orElseThrow());
        }
    }

    // --- it actually saves something ---

    @Test
    void compressibleDataGetsSubstantiallySmaller() throws IOException {
        TreeMap<String, Entry> entries = compressible(5_000);
        long raw = Files.size(write("raw", entries, CompressionType.NONE));
        long packed = Files.size(write("packed", entries, CompressionType.LZ4));

        assertTrue(packed * 2 < raw,
                "expected at least a 2x saving on repetitive data, got "
                        + raw + " to " + packed);
    }

    @Test
    void blocksThatBarelyCompressAreStoredRaw() throws IOException {
        TreeMap<String, Entry> entries = incompressible(5_000, 42);
        long raw = Files.size(write("raw", entries, CompressionType.NONE));
        long packed = Files.size(write("packed", entries, CompressionType.LZ4));

        // LZ4 can still shave about 2% off this, and the writer deliberately
        // declines it: a block has to beat the threshold to be worth
        // decompressing on every single read for the rest of its life. The
        // files come out byte-for-byte the same size because every block here
        // failed that test and was written uncompressed.
        assertEquals(raw, packed,
                "a block that barely compresses should have been stored raw");
    }

    @Test
    void aFileCanHoldBothKindsOfBlock() throws IOException {
        // Compressible and incompressible runs in one table, so the per-block
        // decision has to differ within a single file.
        TreeMap<String, Entry> mixed = new TreeMap<>();
        mixed.putAll(compressible(2_000));
        mixed.putAll(incompressible(2_000, 7));
        Path path = write("mixed", mixed, CompressionType.LZ4);

        try (SSTableReader reader = new SSTableReader(path)) {
            for (String key : mixed.keySet()) {
                assertNotNull(reader.get(key), "lost " + key);
            }
        }
    }

    // --- failure behaviour is unchanged ---

    @Test
    void corruptionIsCaughtBeforeAnythingIsDecompressed() throws IOException {
        Path path = write("table", compressible(2_000), CompressionType.LZ4);
        byte[] bytes = Files.readAllBytes(path);
        bytes[64] ^= 0x01;
        Files.write(path, bytes);

        try (SSTableReader reader = new SSTableReader(path)) {
            // Must be a checksum failure, not whatever a decompressor does when
            // handed damaged input.
            IOException failure = assertThrows(IOException.class,
                    () -> reader.get("user:000000:profile"));
            assertTrue(failure.getMessage().contains("Checksum"),
                    "expected a checksum failure, got: " + failure.getMessage());
        }
    }

    @Test
    void anUnknownCompressionIdIsRejected() throws IOException {
        // Few enough entries to fit in one block, so the block being patched
        // is the whole data region and its checksum sits at a known offset.
        Path path = write("table", compressible(40), CompressionType.LZ4);
        byte[] bytes = Files.readAllBytes(path);

        // First byte of the first block is its compression id. Setting it to
        // something unrecognised must fail loudly rather than be guessed at.
        bytes[0] = 99;
        // Repair the checksum so the id is what fails, not the corruption check.
        int blockLength = firstBlockLength(path, bytes);
        int payloadEnd = blockLength - SSTableWriter.CHECKSUM_SIZE;
        java.util.zip.CRC32C crc = new java.util.zip.CRC32C();
        crc.update(bytes, 0, payloadEnd);
        ByteBuffer.wrap(bytes).putInt(payloadEnd, (int) crc.getValue());
        Files.write(path, bytes);

        try (SSTableReader reader = new SSTableReader(path)) {
            IOException failure = assertThrows(IOException.class,
                    () -> reader.get("user:000000:profile"));
            assertTrue(failure.getMessage().contains("compression"),
                    "expected the id to be rejected, got: " + failure.getMessage());
        }
    }

    /** With a single block, the data region and that block are the same thing. */
    private int firstBlockLength(Path path, byte[] bytes) throws IOException {
        try (SSTableReader reader = new SSTableReader(path)) {
            assertEquals(1, reader.blockCount(), "this test assumes one block");
        }
        ByteBuffer footer = ByteBuffer.wrap(bytes, bytes.length - SSTableWriter.FOOTER_SIZE,
                SSTableWriter.FOOTER_SIZE);
        return (int) footer.getLong();
    }
}
