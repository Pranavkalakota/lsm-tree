package lsm.wal;

import lsm.memtable.Entry;
import lsm.memtable.MemTable;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public class WriteAheadLog implements Closeable {

    private static final byte OP_PUT = 1;
    private static final byte OP_DELETE = 2;

    private final Path path;
    private DataOutputStream out;

    public WriteAheadLog(Path path) throws IOException {
        this.path = path;
        this.out = new DataOutputStream(
                new BufferedOutputStream(new FileOutputStream(path.toFile(), true)));
    }

    public void appendPut(String key, String value) throws IOException {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        byte[] valBytes = value.getBytes(StandardCharsets.UTF_8);

        out.writeByte(OP_PUT);
        out.writeInt(keyBytes.length);
        out.write(keyBytes);
        out.writeInt(valBytes.length);
        out.write(valBytes);
        out.flush();
    }

    public void appendDelete(String key) throws IOException {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);

        out.writeByte(OP_DELETE);
        out.writeInt(keyBytes.length);
        out.write(keyBytes);
        out.flush();
    }

    /**
     * Replays all records in the WAL file into the given MemTable.
     * Used on startup to recover state after a crash.
     */
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
                    byte[] keyBytes = new byte[keyLen];
                    in.readFully(keyBytes);
                    String key = new String(keyBytes, StandardCharsets.UTF_8);

                    if (op == OP_PUT) {
                        int valLen = in.readInt();
                        byte[] valBytes = new byte[valLen];
                        in.readFully(valBytes);
                        String value = new String(valBytes, StandardCharsets.UTF_8);
                        memTable.put(key, value);
                    } else if (op == OP_DELETE) {
                        memTable.delete(key);
                    }
                } catch (EOFException e) {
                    // Truncated record at the end of the WAL — the process crashed
                    // mid-write. Everything before this point was fully written, so
                    // we discard the partial tail and recover what we can.
                    break;
                }
            }
        }
    }

    public void reset() throws IOException {
        out.close();
        Files.deleteIfExists(path);
        this.out = new DataOutputStream(
                new BufferedOutputStream(new FileOutputStream(path.toFile(), true)));
    }

    @Override
    public void close() throws IOException {
        out.close();
    }
}
