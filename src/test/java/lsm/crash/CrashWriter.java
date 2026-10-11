package lsm.crash;

import lsm.LSMStoreEngine;
import lsm.wal.DurabilityMode;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * A child process that writes until something kills it.
 *
 * <p>Run in its own JVM so the test can send a real SIGKILL. Simulating a
 * crash by throwing inside the engine would exercise the recovery code but
 * not the thing recovery is for: a process that stops between any two
 * instructions, with no chance to close a file, flush a buffer or run a
 * shutdown hook.
 *
 * <p>Keys are written in strict order, and progress is recorded in a separate
 * fsynced file outside the engine. Contiguity alone is not enough to catch
 * anything: losing the most recent writes still leaves a contiguous run, so a
 * store that drops its newest data looks identical to one that was simply
 * killed earlier. The progress file is the independent record of how far the
 * writer actually got, and every key below it must come back.
 */
public final class CrashWriter {

    public static void main(String[] args) throws Exception {
        Path dir = Path.of(args[0]);
        long memTableBytes = Long.parseLong(args[1]);
        DurabilityMode mode = DurabilityMode.valueOf(args[2]);
        boolean deleting = args.length > 3 && Boolean.parseBoolean(args[3]);

        LSMStoreEngine engine = new LSMStoreEngine(dir, memTableBytes, mode);
        try (FileChannel progress = FileChannel.open(dir.resolve(PROGRESS_FILE),
                StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            // Never closed on purpose: the process is expected to die mid-write.
            for (int i = 0; ; i++) {
                engine.put(key(i), value(i));
                if (deleting && i % 4 == 0) {
                    engine.delete(key(i / 2));
                }
                if (i % BATCH == 0) {
                    record(progress, i);
                }
            }
        }
    }

    /** Name of the file holding the highest key index known to be written. */
    public static final String PROGRESS_FILE = "progress.marker";

    /** How often progress is committed; each one costs an fsync. */
    public static final int BATCH = 50;

    /**
     * Records that every key up to {@code upTo} has been written, and waits
     * for the disk to confirm it. Fsynced so the reader can trust it even
     * though the engine it is checking may be running without fsync at all.
     */
    private static void record(FileChannel progress, int upTo) throws IOException {
        byte[] bytes = String.valueOf(upTo).getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(8 + bytes.length);
        buffer.putInt(bytes.length);
        buffer.put(bytes);
        buffer.putInt(bytes.length);
        buffer.flip();
        progress.write(buffer, 0);
        progress.force(true);
    }

    /**
     * Reads back the recorded progress, or -1 if none was committed.
     *
     * <p>The length is written on both sides of the number so a half-written
     * record is recognisable rather than being read as a smaller one.
     */
    public static int progressOf(Path dir) throws IOException {
        Path file = dir.resolve(PROGRESS_FILE);
        if (!java.nio.file.Files.exists(file)) {
            return -1;
        }
        byte[] all = java.nio.file.Files.readAllBytes(file);
        if (all.length < 8) {
            return -1;
        }
        ByteBuffer buffer = ByteBuffer.wrap(all);
        int length = buffer.getInt();
        if (length <= 0 || all.length < 8 + length) {
            return -1;
        }
        byte[] digits = new byte[length];
        buffer.get(digits);
        if (buffer.getInt() != length) {
            return -1;
        }
        return Integer.parseInt(new String(digits, StandardCharsets.UTF_8));
    }

    public static String key(int i) {
        return String.format("key_%08d", i);
    }

    public static String value(int i) {
        return "value_" + i + "_" + "x".repeat(i % 50);
    }

    private CrashWriter() {
    }
}
