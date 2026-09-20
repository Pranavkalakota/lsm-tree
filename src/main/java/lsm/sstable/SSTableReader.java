package lsm.sstable;

import lsm.memtable.Entry;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Point lookups against an immutable SSTable written by {@link SSTableWriter}.
 *
 * <p>The sparse index is read once at open time and kept in memory. A lookup
 * binary searches it for the block that could hold the key, then scans that
 * block; because the index covers every {@link SSTableWriter#INDEX_INTERVAL}-th
 * key, the scan touches at most that many records no matter how large the
 * table is.
 *
 * <p>All file access uses positional reads, so the reader holds no mutable
 * cursor of its own and concurrent lookups on one instance are safe.
 */
public final class SSTableReader implements AutoCloseable {

    private static final int SCAN_BUFFER = 16 * 1024;

    private final Path path;
    private final FileChannel channel;
    private final String[] indexKeys;
    private final long[] indexOffsets;
    private final long dataEnd;

    public SSTableReader(Path path) throws IOException {
        this.path = path;
        this.channel = FileChannel.open(path, StandardOpenOption.READ);
        try {
            long size = channel.size();
            if (size < SSTableWriter.FOOTER_SIZE) {
                throw new IOException("Not an SSTable, file is too short: " + path);
            }

            ByteBuffer footer = read(size - SSTableWriter.FOOTER_SIZE, SSTableWriter.FOOTER_SIZE);
            long indexOffset = footer.getLong();
            int indexCount = footer.getInt();
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
            if (indexOffset < 0 || indexOffset > indexEnd || indexCount < 0) {
                throw new IOException("Corrupt SSTable footer: " + path);
            }

            this.dataEnd = indexOffset;
            this.indexKeys = new String[indexCount];
            this.indexOffsets = new long[indexCount];
            loadIndex(indexOffset, indexEnd, indexCount);
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
     */
    public Entry get(String key) throws IOException {
        int slot = indexSlot(key);
        if (slot < 0) {
            return null;
        }
        long blockEnd = (slot + 1 < indexOffsets.length) ? indexOffsets[slot + 1] : dataEnd;
        Cursor cursor = new Cursor(indexOffsets[slot], blockEnd);

        while (cursor.ensure(4)) {
            int keyLen = cursor.buffer().getInt();
            if (!cursor.ensure(keyLen + 5)) {
                break;
            }
            ByteBuffer buf = cursor.buffer();
            byte[] keyBytes = new byte[keyLen];
            buf.get(keyBytes);
            boolean tombstone = buf.get() == 1;
            int valLen = buf.getInt();

            int cmp = new String(keyBytes, StandardCharsets.UTF_8).compareTo(key);
            if (cmp == 0) {
                if (tombstone) {
                    return Entry.tombstone();
                }
                return Entry.put(new String(read(cursor.offset(), valLen).array(),
                        StandardCharsets.UTF_8));
            }
            if (cmp > 0) {
                return null;
            }
            cursor.skip(valLen);
        }
        return null;
    }

    public Path path() {
        return path;
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    private void loadIndex(long from, long to, int count) throws IOException {
        Cursor cursor = new Cursor(from, to);
        for (int i = 0; i < count; i++) {
            if (!cursor.ensure(4)) {
                throw new IOException("Truncated SSTable index: " + path);
            }
            int keyLen = cursor.buffer().getInt();
            if (keyLen < 0 || !cursor.ensure(keyLen + 8)) {
                throw new IOException("Truncated SSTable index: " + path);
            }
            ByteBuffer buf = cursor.buffer();
            byte[] keyBytes = new byte[keyLen];
            buf.get(keyBytes);
            indexKeys[i] = new String(keyBytes, StandardCharsets.UTF_8);
            indexOffsets[i] = buf.getLong();
        }
    }

    /**
     * Index of the last indexed key that is less than or equal to {@code key},
     * or -1 when {@code key} sorts before every key in the table. Ordering uses
     * {@link String#compareTo}, matching the TreeMap order the writer relies on.
     */
    private int indexSlot(String key) {
        int lo = 0;
        int hi = indexKeys.length - 1;
        int slot = -1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            if (indexKeys[mid].compareTo(key) <= 0) {
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

    /**
     * A forward-only view over a byte range of the file, filled in chunks so a
     * block scan costs one read rather than one per field. Each lookup makes its
     * own cursor; the shared channel is only touched through positional reads.
     */
    private final class Cursor {

        private final long end;
        private ByteBuffer buf;
        private long next;

        Cursor(long start, long end) {
            this.end = end;
            this.next = start;
            this.buf = ByteBuffer.allocate(SCAN_BUFFER);
            this.buf.limit(0);
        }

        ByteBuffer buffer() {
            return buf;
        }

        /** File offset of the next byte this cursor will hand out. */
        long offset() {
            return next - buf.remaining();
        }

        /** Makes at least {@code n} bytes readable, growing for outsized records. */
        boolean ensure(int n) throws IOException {
            if (buf.remaining() >= n) {
                return true;
            }
            if (n > buf.capacity()) {
                buf = ByteBuffer.allocate(n).put(buf);
            } else {
                buf.compact();
            }
            while (buf.position() < n && next < end) {
                int room = (int) Math.min(buf.capacity() - buf.position(), end - next);
                buf.limit(buf.position() + room);
                int read = channel.read(buf, next);
                buf.limit(buf.capacity());
                if (read < 0) {
                    break;
                }
                next += read;
            }
            boolean enough = buf.position() >= n;
            buf.flip();
            return enough;
        }

        void skip(long n) {
            if (n <= buf.remaining()) {
                buf.position(buf.position() + (int) n);
            } else {
                next = offset() + n;
                buf.position(0);
                buf.limit(0);
            }
        }
    }
}
