package lsm.wal;

import lsm.memtable.MemTable;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The durability half of a write: an append-only record of everything that has
 * happened, replayed on startup to rebuild whatever the MemTable held.
 *
 * <p>Appending and committing are separate operations. Appending has to be
 * ordered, so callers serialize it, but it is only a memcpy into the channel.
 * Committing is an fsync costing milliseconds, and crucially one fsync commits
 * every record written before it. {@link #syncTo} exploits that: while one
 * thread is inside the fsync, other writers queue behind it, and a single
 * follow-up fsync commits the whole batch. Without that, concurrent writers buy
 * nothing at all, because each one waits out its own disk round trip.
 */
public class WriteAheadLog implements Closeable {

    private static final byte OP_PUT = 1;
    private static final byte OP_DELETE = 2;
    private static final int MAX_KEY_SIZE = 10 * 1024 * 1024;
    private static final int MAX_VALUE_SIZE = 100 * 1024 * 1024;

    private final Path path;
    private final DurabilityMode mode;

    /** Guards the channel reference, the fsync itself, and {@link #syncedSeq}. */
    private final Object syncLock = new Object();

    private final AtomicLong writeSeq = new AtomicLong();
    private volatile long syncedSeq = 0;
    private boolean syncing = false;

    private FileChannel channel;
    private volatile boolean closed = false;

    public WriteAheadLog(Path path) throws IOException {
        this(path, DurabilityMode.BUFFERED);
    }

    public WriteAheadLog(Path path, DurabilityMode mode) throws IOException {
        this.path = path;
        this.mode = mode;
        this.channel = open(path);
    }

    private static FileChannel open(Path path) throws IOException {
        return FileChannel.open(path, StandardOpenOption.CREATE,
                StandardOpenOption.WRITE, StandardOpenOption.APPEND);
    }

    public DurabilityMode mode() {
        return mode;
    }

    /**
     * Appends a put and returns its sequence number, which {@link #syncTo} can
     * later be asked to commit. Callers must serialize appends; the log's order
     * has to match the order the MemTable saw.
     */
    public long appendPut(String key, String value) throws IOException {
        checkNotClosed();
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        byte[] valBytes = value.getBytes(StandardCharsets.UTF_8);

        // One buffer for the whole record: [op:1][keyLen:4][key][valLen:4][val]
        ByteBuffer buf = ByteBuffer.allocate(1 + 4 + keyBytes.length + 4 + valBytes.length);
        buf.put(OP_PUT);
        buf.putInt(keyBytes.length);
        buf.put(keyBytes);
        buf.putInt(valBytes.length);
        buf.put(valBytes);
        return writeRecord(buf);
    }

    /** Appends a delete. See {@link #appendPut} for the ordering contract. */
    public long appendDelete(String key) throws IOException {
        checkNotClosed();
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);

        ByteBuffer buf = ByteBuffer.allocate(1 + 4 + keyBytes.length);
        buf.put(OP_DELETE);
        buf.putInt(keyBytes.length);
        buf.put(keyBytes);
        return writeRecord(buf);
    }

    private long writeRecord(ByteBuffer buf) throws IOException {
        buf.flip();
        while (buf.hasRemaining()) {
            channel.write(buf);
        }
        return writeSeq.incrementAndGet();
    }

    /**
     * Blocks until the record numbered {@code seq} is committed to disk.
     *
     * <p>Does nothing in {@link DurabilityMode#BUFFERED}, where handing the
     * bytes to the operating system is the whole guarantee.
     *
     * <p>In {@link DurabilityMode#SYNC} the caller either performs the fsync or
     * waits for one already running. Whoever performs it commits everything
     * appended up to that moment, so a writer that arrives while an fsync is in
     * flight is very often already durable by the time it wakes up.
     */
    public void syncTo(long seq) throws IOException {
        if (mode == DurabilityMode.BUFFERED || syncedSeq >= seq) {
            return;
        }
        synchronized (syncLock) {
            while (syncedSeq < seq && !closed) {
                if (syncing) {
                    awaitSync();
                    continue;
                }
                syncing = true;
                // Read the watermark before the fsync, never after: records
                // appended during the fsync are not necessarily covered by it.
                long covered = writeSeq.get();
                try {
                    channel.force(false);
                    syncedSeq = Math.max(syncedSeq, covered);
                } finally {
                    syncing = false;
                    syncLock.notifyAll();
                }
            }
        }
    }

    private void awaitSync() throws InterruptedIOException {
        try {
            syncLock.wait();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedIOException("Interrupted waiting for a log commit");
        }
    }

    public static void replay(Path path, MemTable memTable) throws IOException {
        if (!Files.exists(path) || Files.size(path) == 0) {
            return;
        }

        try (DataInputStream in = new DataInputStream(
                new BufferedInputStream(new FileInputStream(path.toFile())))) {
            while (in.available() > 0) {
                try {
                    byte op = in.readByte();

                    int keyLen = in.readInt();
                    if (keyLen < 0 || keyLen > MAX_KEY_SIZE) {
                        break;
                    }
                    byte[] keyBytes = new byte[keyLen];
                    in.readFully(keyBytes);
                    String key = new String(keyBytes, StandardCharsets.UTF_8);

                    if (op == OP_PUT) {
                        int valLen = in.readInt();
                        if (valLen < 0 || valLen > MAX_VALUE_SIZE) {
                            break;
                        }
                        byte[] valBytes = new byte[valLen];
                        in.readFully(valBytes);
                        memTable.put(key, new String(valBytes, StandardCharsets.UTF_8));
                    } else if (op == OP_DELETE) {
                        memTable.delete(key);
                    } else {
                        break;
                    }
                } catch (EOFException e) {
                    break;
                }
            }
        }
    }

    /**
     * Discards the log and starts an empty one, called once a flush has put the
     * same records somewhere durable.
     *
     * <p>Everything appended so far therefore counts as committed, and the sync
     * watermark jumps forward to say so. A writer still waiting on an older
     * sequence is released rather than left blocking on a channel that no
     * longer exists.
     */
    public void reset() throws IOException {
        synchronized (syncLock) {
            checkNotClosed();
            channel.close();
            Files.deleteIfExists(path);
            channel = open(path);
            syncedSeq = writeSeq.get();
            syncLock.notifyAll();
        }
    }

    @Override
    public void close() throws IOException {
        synchronized (syncLock) {
            if (closed) {
                return;
            }
            closed = true;
            // Committed even in BUFFERED mode: a clean shutdown should not be
            // the thing that loses writes.
            channel.force(false);
            syncedSeq = writeSeq.get();
            channel.close();
            syncLock.notifyAll();
        }
    }

    private void checkNotClosed() throws IOException {
        if (closed) {
            throw new IOException("WAL is closed");
        }
    }
}
