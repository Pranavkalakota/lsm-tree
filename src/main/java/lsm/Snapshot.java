package lsm;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A fixed point in the store's history that a reader can keep returning to.
 *
 * <p>A snapshot is a version number and nothing more. Reads taken against it
 * see the newest version of each key written at or before it, so they are
 * unaffected by everything that happens afterwards: a key overwritten a
 * thousand times still reads as it did when the snapshot was taken, and a key
 * deleted since still has its value.
 *
 * <p>Holding one costs space rather than time. Reads are no slower, but
 * compaction may not discard any version a snapshot could still ask for, so
 * the history under it stays on disk until it is closed. A snapshot left open
 * forever stops the store from ever reclaiming anything beneath it, which is
 * why this is {@link AutoCloseable} and why try-with-resources is the intended
 * shape.
 */
public final class Snapshot implements AutoCloseable {

    private final long version;
    private final Runnable release;
    private final AtomicBoolean open = new AtomicBoolean(true);

    Snapshot(long version, Runnable release) {
        this.version = version;
        this.release = release;
    }

    /** The version this snapshot reads at. */
    public long version() {
        return version;
    }

    /** Whether this snapshot is still usable. */
    public boolean isOpen() {
        return open.get();
    }

    /**
     * Releases the history this snapshot was holding back. Idempotent, so
     * closing twice is harmless; reading through it afterwards is not, and the
     * engine rejects it rather than quietly answering from a version it may
     * already have let go.
     */
    @Override
    public void close() {
        if (open.compareAndSet(true, false)) {
            release.run();
        }
    }

    @Override
    public String toString() {
        return "Snapshot@" + version + (isOpen() ? "" : " (closed)");
    }
}
