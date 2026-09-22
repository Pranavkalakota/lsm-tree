package lsm.sstable;

import lsm.memtable.Entry;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32C;

/**
 * Point lookups against an immutable SSTable written by {@link SSTableWriter}.
 *
 * <p>The block index is read once at open time and kept in memory. A lookup
 * binary searches it for the one block that could hold the key, loads that
 * block in a single read, checks it against its stored checksum, and scans it.
 * Because the index names every block, exactly one block is ever loaded per
 * lookup no matter how large the table is.
 *
 * <p>All file access uses positional reads, so the reader holds no mutable
 * cursor of its own and concurrent lookups on one instance are safe.
 */
public final class SSTableReader implements AutoCloseable {

    private final Path path;
    private final FileChannel channel;
    private final BlockCache cache;
    private final String[] firstKeys;
    private final long[] blockOffsets;
    private final int[] blockLengths;

    /** Opens a reader with its own private cache; convenient for tests. */
    public SSTableReader(Path path) throws IOException {
        this(path, new BlockCache(SSTableWriter.BLOCK_SIZE * 8L));
    }

    public SSTableReader(Path path, BlockCache cache) throws IOException {
        this.path = path;
        this.cache = cache;
        this.channel = FileChannel.open(path, StandardOpenOption.READ);
        try {
            long size = channel.size();
            if (size < SSTableWriter.FOOTER_SIZE) {
                throw new IOException("Not an SSTable, file is too short: " + path);
            }

            ByteBuffer footer = read(size - SSTableWriter.FOOTER_SIZE, SSTableWriter.FOOTER_SIZE);
            long indexOffset = footer.getLong();
            int blockCount = footer.getInt();
            int version = footer.getInt();
            long magic = footer.getLong();

            if (magic != SSTableWriter.MAGIC) {
                throw new IOException("Bad SSTable magic number: " + path);
            }
            if (version != SSTableWriter.FORMAT_VERSION) {
                throw new IOException("Unsupported SSTable format version " + version
                        + " in " + path);
            }
            long indexEnd = size - SSTableWriter.FOOTER_SIZE;
            if (indexOffset < 0 || indexOffset > indexEnd || blockCount < 0) {
                throw new IOException("Corrupt SSTable footer: " + path);
            }

            this.firstKeys = new String[blockCount];
            this.blockOffsets = new long[blockCount];
            this.blockLengths = new int[blockCount];
            loadIndex(indexOffset, (int) (indexEnd - indexOffset), blockCount, indexOffset);
        } catch (IOException e) {
            channel.close();
            throw e;
        }
    }

    /**
     * Returns the stored entry for {@code key}, which may be a tombstone, or
     * {@code null} when this table says nothing about the key at all. The two
     * cases are distinct: a tombstone must stop the engine from consulting
     * older tables, whereas an absent key must not.
     *
     * @throws IOException if the block holding the key fails its checksum
     */
    public Entry get(String key) throws IOException {
        int slot = indexSlot(key);
        if (slot < 0) {
            return null;
        }
        ByteBuffer block = readBlock(slot);

        while (block.remaining() > 0) {
            int keyLen = block.getInt();
            byte[] keyBytes = new byte[keyLen];
            block.get(keyBytes);
            boolean tombstone = block.get() == 1;
            int valLen = block.getInt();

            int cmp = new String(keyBytes, StandardCharsets.UTF_8).compareTo(key);
            if (cmp == 0) {
                if (tombstone) {
                    return Entry.tombstone();
                }
                byte[] val = new byte[valLen];
                block.get(val);
                return Entry.put(new String(val, StandardCharsets.UTF_8));
            }
            if (cmp > 0) {
                // Keys ascend, so anything further in this block is past the
                // target, and the index guarantees later blocks start later still.
                return null;
            }
            block.position(block.position() + valLen);
        }
        return null;
    }

    public Path path() {
        return path;
    }

    /** Number of blocks in this table. */
    public int blockCount() {
        return blockOffsets.length;
    }

    /**
     * Walks every entry in key order, tombstones included.
     *
     * <p>Compaction reads whole tables this way rather than through {@link #get},
     * so it streams block by block and never holds more than one block of the
     * table in memory. Tombstones are surfaced because only the caller knows
     * whether an older table below still needs shadowing.
     */
    public Cursor cursor() throws IOException {
        return new Cursor();
    }

    /** One entry of a table: the key, and the value or tombstone stored for it. */
    public record Row(String key, Entry value) {
    }

    /** Forward-only walk over a table's entries. Not thread safe; make one per scan. */
    public final class Cursor {

        private int block = -1;
        private ByteBuffer records = ByteBuffer.allocate(0);
        private Row current;

        private Cursor() throws IOException {
            advance();
        }

        /** The entry the cursor sits on, or null once the table is exhausted. */
        public Row current() {
            return current;
        }

        public boolean hasNext() {
            return current != null;
        }

        /** Moves to the next entry, returning the one just passed. */
        public Row next() throws IOException {
            Row row = current;
            advance();
            return row;
        }

        private void advance() throws IOException {
            while (!records.hasRemaining()) {
                if (++block >= blockOffsets.length) {
                    current = null;
                    return;
                }
                records = readBlock(block);
            }

            int keyLen = records.getInt();
            byte[] keyBytes = new byte[keyLen];
            records.get(keyBytes);
            boolean tombstone = records.get() == 1;
            int valLen = records.getInt();

            String key = new String(keyBytes, StandardCharsets.UTF_8);
            if (tombstone) {
                current = new Row(key, Entry.tombstone());
                return;
            }
            byte[] val = new byte[valLen];
            records.get(val);
            current = new Row(key, Entry.put(new String(val, StandardCharsets.UTF_8)));
        }
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    /**
     * Returns the records of one block, from cache when resident.
     *
     * <p>The checksum is verified where the bytes are read, so a cache hit
     * skips it: the block was already proven intact on the way in, and tables
     * are immutable so it cannot have changed since. The buffer is a fresh
     * view over shared bytes, giving each caller its own scan position.
     */
    private ByteBuffer readBlock(int slot) throws IOException {
        long offset = blockOffsets[slot];
        byte[] records = cache.get(path, offset, () -> loadAndVerify(slot, offset));
        return ByteBuffer.wrap(records);
    }

    private byte[] loadAndVerify(int slot, long offset) throws IOException {
        int length = blockLengths[slot];
        ByteBuffer raw = read(offset, length);
        int dataLength = length - SSTableWriter.CHECKSUM_SIZE;

        CRC32C crc = new CRC32C();
        crc.update(raw.array(), 0, dataLength);
        if ((int) crc.getValue() != raw.getInt(dataLength)) {
            throw new IOException("Checksum mismatch in block " + slot + " of " + path
                    + "; the file has been corrupted");
        }
        byte[] records = new byte[dataLength];
        System.arraycopy(raw.array(), 0, records, 0, dataLength);
        return records;
    }

    private void loadIndex(long from, int length, int count, long dataEnd) throws IOException {
        ByteBuffer buf = read(from, length);
        for (int i = 0; i < count; i++) {
            if (buf.remaining() < 4) {
                throw new IOException("Truncated SSTable index: " + path);
            }
            int keyLen = buf.getInt();
            if (keyLen < 0 || buf.remaining() < keyLen + 12) {
                throw new IOException("Truncated SSTable index: " + path);
            }
            byte[] keyBytes = new byte[keyLen];
            buf.get(keyBytes);
            firstKeys[i] = new String(keyBytes, StandardCharsets.UTF_8);
            blockOffsets[i] = buf.getLong();
            blockLengths[i] = buf.getInt();

            if (blockLengths[i] < SSTableWriter.CHECKSUM_SIZE
                    || blockOffsets[i] < 0
                    || blockOffsets[i] + blockLengths[i] > dataEnd) {
                throw new IOException("Corrupt SSTable index entry " + i + " in " + path);
            }
        }
    }

    /**
     * Index of the last block whose first key is less than or equal to
     * {@code key}, or -1 when {@code key} sorts before every key in the table.
     * Ordering uses {@link String#compareTo}, matching the TreeMap order the
     * writer relies on.
     */
    private int indexSlot(String key) {
        int lo = 0;
        int hi = firstKeys.length - 1;
        int slot = -1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            if (firstKeys[mid].compareTo(key) <= 0) {
                slot = mid;
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        return slot;
    }

    private ByteBuffer read(long position, int length) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(length);
        while (buf.hasRemaining()) {
            if (channel.read(buf, position + buf.position()) < 0) {
                throw new EOFException("Unexpected end of SSTable: " + path);
            }
        }
        return buf.flip();
    }
}
