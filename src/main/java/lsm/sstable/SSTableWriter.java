package lsm.sstable;

import lsm.memtable.Entry;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
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

/**
 * Serializes a sorted set of entries into an immutable SSTable file.
 *
 * <pre>
 * [data block]   record*, key-sorted: [keyLen:4][key][tombstone:1][valLen:4][val]
 * [index block]  every INDEX_INTERVAL-th key: [keyLen:4][key][dataOffset:8]
 * [footer]       [indexOffset:8][indexCount:4][formatVersion:4][magic:8]
 * </pre>
 *
 * The index is sparse so it stays small enough to hold in memory for every open
 * table: one entry per 16 keys costs ~1/16th the RAM of a dense index, and the
 * cost of the linear scan it forces is bounded by those 16 records.
 */
public final class SSTableWriter {

    /** "LSMSST" plus a two byte tag; trailing bytes of every well-formed file. */
    public static final long MAGIC = 0x4C534D5353543031L;

    public static final int FORMAT_VERSION = 1;
    public static final int FOOTER_SIZE = 24;
    public static final int INDEX_INTERVAL = 16;

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

            DataOutputStream out = new DataOutputStream(
                    new BufferedOutputStream(Channels.newOutputStream(channel), BUFFER_SIZE));

            List<String> indexKeys = new ArrayList<>();
            List<Long> indexOffsets = new ArrayList<>();
            long offset = 0;
            int count = 0;

            for (Map.Entry<String, Entry> entry : sortedEntries.entrySet()) {
                byte[] key = entry.getKey().getBytes(StandardCharsets.UTF_8);
                Entry value = entry.getValue();
                byte[] val = value.isTombstone()
                        ? NO_BYTES
                        : value.value().orElseThrow().getBytes(StandardCharsets.UTF_8);

                if (count % INDEX_INTERVAL == 0) {
                    indexKeys.add(entry.getKey());
                    indexOffsets.add(offset);
                }

                out.writeInt(key.length);
                out.write(key);
                out.writeByte(value.isTombstone() ? 1 : 0);
                out.writeInt(val.length);
                out.write(val);

                offset += 4 + key.length + 1 + 4 + val.length;
                count++;
            }

            long indexOffset = offset;
            for (int i = 0; i < indexKeys.size(); i++) {
                byte[] key = indexKeys.get(i).getBytes(StandardCharsets.UTF_8);
                out.writeInt(key.length);
                out.write(key);
                out.writeLong(indexOffsets.get(i));
            }

            out.writeLong(indexOffset);
            out.writeInt(indexKeys.size());
            out.writeInt(FORMAT_VERSION);
            out.writeLong(MAGIC);

            out.flush();
            channel.force(true);
        }
    }
}
