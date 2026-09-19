package lsm.wal;

import lsm.memtable.MemTable;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class WriteAheadLog implements Closeable {

    private static final byte OP_PUT = 1;
    private static final byte OP_DELETE = 2;
    private static final int MAX_KEY_SIZE = 10 * 1024 * 1024;
    private static final int MAX_VALUE_SIZE = 100 * 1024 * 1024;

    private final Path path;
    private FileChannel channel;
    private boolean closed = false;

    public WriteAheadLog(Path path) throws IOException {
        this.path = path;
        this.channel = FileChannel.open(path,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
    }

    public void appendPut(String key, String value) throws IOException {
        checkNotClosed();
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        byte[] valBytes = value.getBytes(StandardCharsets.UTF_8);

        // Pack the entire record into one buffer so it's written atomically
        // to the channel: [op:1][keyLen:4][key:N][valLen:4][val:M]
        ByteBuffer buf = ByteBuffer.allocate(1 + 4 + keyBytes.length + 4 + valBytes.length);
        buf.put(OP_PUT);
        buf.putInt(keyBytes.length);
        buf.put(keyBytes);
        buf.putInt(valBytes.length);
        buf.put(valBytes);
        buf.flip();

        while (buf.hasRemaining()) {
            channel.write(buf);
        }
        channel.force(false);
    }

    public void appendDelete(String key) throws IOException {
        checkNotClosed();
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);

        ByteBuffer buf = ByteBuffer.allocate(1 + 4 + keyBytes.length);
        buf.put(OP_DELETE);
        buf.putInt(keyBytes.length);
        buf.put(keyBytes);
        buf.flip();

        while (buf.hasRemaining()) {
            channel.write(buf);
        }
        channel.force(false);
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
                        String value = new String(valBytes, StandardCharsets.UTF_8);
                        memTable.put(key, value);
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

    public void reset() throws IOException {
        checkNotClosed();
        channel.close();
        Files.deleteIfExists(path);
        this.channel = FileChannel.open(path,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            closed = true;
            channel.close();
        }
    }

    private void checkNotClosed() throws IOException {
        if (closed) {
            throw new IOException("WAL is closed");
        }
    }
}
