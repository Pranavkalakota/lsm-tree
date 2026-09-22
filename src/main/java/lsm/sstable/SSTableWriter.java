package lsm.sstable;

import lsm.memtable.Entry;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
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
 * Serializes sorted entries into an immutable SSTable file.
 *
 * <pre>
 * [block]*       records packed to ~BLOCK_SIZE, then [crc32c:4] over them
 *                record: [keyLen:4][key][tombstone:1][valLen:4][val]
 * [index block]  one entry per block: [keyLen:4][firstKey][offset:8][length:4]
 * [metadata]     [maxKeyLen:4][maxKey]
 * [footer]       [indexOffset:8][blockCount:4][metaOffset:8][formatVersion:4][magic:8]
 * </pre>
 *
 * The largest key is recorded explicitly because compaction needs each table's
 * range to decide what overlaps what. The smallest key is already the first
 * index entry, but the largest lives in the final data block, and reading a
 * data block just to open a table would both slow startup and make a table with
 * one corrupt block impossible to open at all.
 *
 * Records are grouped into blocks rather than written as one flat run so each
 * block can carry a checksum over exactly the bytes a reader will load, and so
 * a reader has a natural unit to cache.
 *
 * <p>Entries are fed in one at a time rather than handed over as a map, because
 * compaction merges tables that can be larger than memory and has nowhere to
 * materialize the result. {@link #write} keeps the map form for callers that
 * already hold everything, which is what a MemTable flush does.
 *
 * <p>Content lands in a sibling temp file that is fsynced and atomically
 * renamed on {@link #finish}, so a crash mid-write cannot leave a partial file
 * under a name the engine would try to open. Closing without finishing throws
 * the temp file away.
 */
public final class SSTableWriter implements Closeable {

    /** "LSMSST" plus a two byte tag; trailing bytes of every well-formed file. */
    public static final long MAGIC = 0x4C534D5353543031L;

    public static final int FORMAT_VERSION = 3;
    public static final int FOOTER_SIZE = 32;
    public static final int CHECKSUM_SIZE = 4;

    /** A block is closed once it passes this; one oversized record may exceed it. */
    static final int BLOCK_SIZE = 4 * 1024;

    private static final int BUFFER_SIZE = 64 * 1024;
    private static final byte[] NO_BYTES = new byte[0];

    private final Path path;
    private final Path tmp;
    private final FileChannel channel;
    private final OutputStream out;

    private final List<BlockRef> index = new ArrayList<>();
    private final ByteArrayOutputStream block = new ByteArrayOutputStream(BLOCK_SIZE * 2);
    private final DataOutputStream blockOut = new DataOutputStream(block);

    private String firstKey;
    private String lastKey;
    private long offset;
    private boolean finished;

    /** Opens a writer for a table that does not exist yet. */
    public static SSTableWriter create(Path path) throws IOException {
        if (Files.exists(path)) {
            throw new IOException("Refusing to overwrite existing SSTable: " + path);
        }
        return new SSTableWriter(path);
    }

    /** Writes an entire sorted map as one table. */
    public static void write(Path path, Map<String, Entry> sortedEntries) throws IOException {
        try (SSTableWriter writer = create(path)) {
            for (Map.Entry<String, Entry> entry : sortedEntries.entrySet()) {
                writer.add(entry.getKey(), entry.getValue());
            }
            writer.finish();
        }
    }

    private SSTableWriter(Path path) throws IOException {
        this.path = path;
        this.tmp = path.resolveSibling(path.getFileName() + ".tmp");
        this.channel = FileChannel.open(tmp, StandardOpenOption.CREATE,
                StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
        this.out = new BufferedOutputStream(Channels.newOutputStream(channel), BUFFER_SIZE);
    }

    /**
     * Appends one entry. Keys must arrive in ascending order, since the reader's
     * binary search over block boundaries assumes it; a key that goes backwards
     * is rejected here rather than producing a table that silently loses lookups.
     */
    public void add(String key, Entry value) throws IOException {
        if (finished) {
            throw new IOException("SSTable already finished: " + path);
        }
        if (lastKey != null && key.compareTo(lastKey) <= 0) {
            throw new IOException("Keys must ascend, got " + key + " after " + lastKey);
        }
        lastKey = key;
        if (firstKey == null) {
            firstKey = key;
        }

        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        byte[] valBytes = value.isTombstone()
                ? NO_BYTES
                : value.value().orElseThrow().getBytes(StandardCharsets.UTF_8);

        blockOut.writeInt(keyBytes.length);
        blockOut.write(keyBytes);
        blockOut.writeByte(value.isTombstone() ? 1 : 0);
        blockOut.writeInt(valBytes.length);
        blockOut.write(valBytes);

        if (block.size() >= BLOCK_SIZE) {
            emitBlock();
        }
    }

    /** Flushes the tail block, writes the index and footer, and publishes the file. */
    public void finish() throws IOException {
        if (finished) {
            return;
        }
        if (block.size() > 0) {
            emitBlock();
        }

        long indexOffset = offset;
        long indexBytes = 0;
        DataOutputStream tail = new DataOutputStream(out);
        for (BlockRef ref : index) {
            byte[] key = ref.firstKey.getBytes(StandardCharsets.UTF_8);
            tail.writeInt(key.length);
            tail.write(key);
            tail.writeLong(ref.offset);
            tail.writeInt(ref.length);
            indexBytes += 4 + key.length + 8 + 4;
        }

        byte[] maxKey = lastKey == null
                ? NO_BYTES
                : lastKey.getBytes(StandardCharsets.UTF_8);
        tail.writeInt(maxKey.length);
        tail.write(maxKey);

        tail.writeLong(indexOffset);
        tail.writeInt(index.size());
        tail.writeLong(indexOffset + indexBytes);
        tail.writeInt(FORMAT_VERSION);
        tail.writeLong(MAGIC);

        tail.flush();
        channel.force(true);
        channel.close();

        Files.move(tmp, path, StandardCopyOption.ATOMIC_MOVE);
        finished = true;
    }

    /** Discards the temp file unless {@link #finish} already published it. */
    @Override
    public void close() throws IOException {
        if (finished) {
            return;
        }
        try {
            channel.close();
        } finally {
            Files.deleteIfExists(tmp);
        }
    }

    private void emitBlock() throws IOException {
        byte[] bytes = block.toByteArray();
        block.reset();

        CRC32C crc = new CRC32C();
        crc.update(bytes);

        out.write(bytes);
        out.write(ByteBuffer.allocate(CHECKSUM_SIZE).putInt((int) crc.getValue()).array());

        int length = bytes.length + CHECKSUM_SIZE;
        index.add(new BlockRef(firstKey, offset, length));
        offset += length;
        firstKey = null;
    }

    private record BlockRef(String firstKey, long offset, int length) {
    }
}
