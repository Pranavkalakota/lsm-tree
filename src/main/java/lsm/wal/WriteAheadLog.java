package lsm.wal;

import lsm.memtable.MemTable;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class WriteAheadLog implements Closeable {

    private static final byte OP_PUT = 1;
    private static final byte OP_DELETE = 2;
    private static final int MAX_KEY_SIZE = 10 * 1024 * 1024;   // 10 MB
    private static final int MAX_VALUE_SIZE = 100 * 1024 * 1024; // 100 MB

    private final Path path;
    private DataOutputStream out;
    private FileChannel channel;
    private boolean closed = false;

    public WriteAheadLog(Path path) throws IOException {
        this.path = path;
        openStream();
    }

    public void appendPut(String key, String value) throws IOException {
        checkNotClosed();
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        byte[] valBytes = value.getBytes(StandardCharsets.UTF_8);

        out.writeByte(OP_PUT);
        out.writeInt(keyBytes.length);
        out.write(keyBytes);
        out.writeInt(valBytes.length);
        out.write(valBytes);
        out.flush();
        channel.force(false);
    }

    public void appendDelete(String key) throws IOException {
        checkNotClosed();
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);

        out.writeByte(OP_DELETE);
        out.writeInt(keyBytes.length);
        out.write(keyBytes);
        out.flush();
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
        out.close();
        channel.close();
        Files.deleteIfExists(path);
        openStream();
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            closed = true;
            out.close();
            channel.close();
        }
    }

    private void openStream() throws IOException {
        this.channel = FileChannel.open(path,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND, StandardOpenOption.WRITE);
        this.out = new DataOutputStream(
                new BufferedOutputStream(new FileOutputStream(path.toFile(), true)));
    }

    private void checkNotClosed() throws IOException {
        if (closed) {
            throw new IOException("WAL is closed");
        }
    }
}
