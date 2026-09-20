package lsm.sstable;

import lsm.memtable.Entry;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32C;

/**
 * Serializes a sorted set of entries into an immutable SSTable file.
 *
 * <pre>
 * [block]*       records packed to ~BLOCK_SIZE, then [crc32c:4] over them
 *                record: [keyLen:4][key][tombstone:1][valLen:4][val]
 * [index block]  one entry per block: [keyLen:4][firstKey][offset:8][length:4]
 * [footer]       [indexOffset:8][indexCount:4][formatVersion:4][magic:8]
 * </pre>
 *
 * Records are grouped into blocks rather than written as one flat run so that
 * each block can carry a checksum over exactly the bytes a reader will load,
 * and so a reader has a natural unit to cache. The index holds one entry per
 * block, which keeps it small enough to stay resident for every open table.
 */
public final class SSTableWriter {

    /** "LSMSST" plus a two byte tag; trailing bytes of every well-formed file. */
    public static final long MAGIC = 0x4C534D5353543031L;

    /** Bumped from 1 when records were grouped into checksummed blocks. */
    public static final int FORMAT_VERSION = 2;
    public static final int FOOTER_SIZE = 24;
    public static final int CHECKSUM_SIZE = 4;

    /** A block is closed once it passes this; one oversized record may exceed it. */
    static final int BLOCK_SIZE = 4 * 1024;

    private static final int BUFFER_SIZE = 64 * 1024;
    private static final byte[] NO_BYTES = new byte[0];

    private SSTableWriter() {
    }

    /**
     * Writes {@code sortedEntries} to {@code path}. The caller must supply the
     * entries in ascending key order; the reader's binary search depends on it.
     *
     * <p>Content lands in a sibling temp file that is fsynced and then atomically
     * renamed, so a crash mid-write can never leave a partial file under a name
     * the engine would try to open.
     */
    public static void write(Path path, Map<String, Entry> sortedEntries) throws IOException {
        if (Files.exists(path)) {
            throw new IOException("Refusing to overwrite existing SSTable: " + path);
        }
        Path tmp = path.resolveSibling(path.getFileName() + ".tmp");
        try {
            writeTo(tmp, sortedEntries);
            Files.move(tmp, path, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            Files.deleteIfExists(tmp);
            throw e;
        }
    }

    private static void writeTo(Path tmp, Map<String, Entry> sortedEntries) throws IOException {
        try (FileChannel channel = FileChannel.open(tmp, StandardOpenOption.CREATE,
                StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {

            OutputStream out = new BufferedOutputStream(
                    Channels.newOutputStream(channel), BUFFER_SIZE);

            List<BlockRef> index = new ArrayList<>();
            ByteArrayOutputStream block = new ByteArrayOutputStream(BLOCK_SIZE * 2);
            DataOutputStream blockOut = new DataOutputStream(block);
            String firstKey = null;
            long offset = 0;

            for (Map.Entry<String, Entry> entry : sortedEntries.entrySet()) {
                if (firstKey == null) {
                    firstKey = entry.getKey();
                }
                byte[] key = entry.getKey().getBytes(StandardCharsets.UTF_8);
                Entry value = entry.getValue();
                byte[] val = value.isTombstone()
                        ? NO_BYTES
                        : value.value().orElseThrow().getBytes(StandardCharsets.UTF_8);

                blockOut.writeInt(key.length);
                blockOut.write(key);
                blockOut.writeByte(value.isTombstone() ? 1 : 0);
                blockOut.writeInt(val.length);
                blockOut.write(val);

                if (block.size() >= BLOCK_SIZE) {
                    offset += emitBlock(out, block, index, firstKey, offset);
                    firstKey = null;
                }
            }
            if (block.size() > 0) {
                offset += emitBlock(out, block, index, firstKey, offset);
            }

            long indexOffset = offset;
            DataOutputStream tail = new DataOutputStream(out);
            for (BlockRef ref : index) {
                byte[] key = ref.firstKey.getBytes(StandardCharsets.UTF_8);
                tail.writeInt(key.length);
                tail.write(key);
                tail.writeLong(ref.offset);
                tail.writeInt(ref.length);
            }

            tail.writeLong(indexOffset);
            tail.writeInt(index.size());
            tail.writeInt(FORMAT_VERSION);
            tail.writeLong(MAGIC);

            tail.flush();
            channel.force(true);
        }
    }

    /** Appends the pending block plus its checksum, and returns bytes written. */
    private static int emitBlock(OutputStream out, ByteArrayOutputStream block,
            List<BlockRef> index, String firstKey, long offset) throws IOException {
        byte[] bytes = block.toByteArray();
        block.reset();

        CRC32C crc = new CRC32C();
        crc.update(bytes);

        out.write(bytes);
        out.write(ByteBuffer.allocate(CHECKSUM_SIZE).putInt((int) crc.getValue()).array());

        int length = bytes.length + CHECKSUM_SIZE;
        index.add(new BlockRef(firstKey, offset, length));
        return length;
    }

    private record BlockRef(String firstKey, long offset, int length) {
    }
}
