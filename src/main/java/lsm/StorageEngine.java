package lsm;

import java.util.Optional;
import java.util.stream.Stream;

public interface StorageEngine {

    void put(String key, String value);

    Optional<String> get(String key);

    void delete(String key);

    /**
     * Entries in a key range, ascending.
     *
     * <p>The stream is lazy and holds resources open while it runs, so callers
     * must close it; try-with-resources is the intended shape. Deleted keys are
     * filtered out, and each key appears once with its newest value.
     *
     * @param fromInclusive lowest key to return, or null to start at the first
     * @param toExclusive   first key to stop before, or null to run to the end
     */
    Stream<Row> scan(String fromInclusive, String toExclusive);

    /**
     * Opens a snapshot at the store's current version.
     *
     * <p>Callers must close it. Until they do, compaction cannot discard any
     * version the snapshot could still read, so an abandoned snapshot stops
     * the store reclaiming space underneath it.
     */
    Snapshot snapshot();

    /** The value of a key as of a snapshot, rather than as of now. */
    Optional<String> get(String key, Snapshot at);

    /**
     * A range scan as of a snapshot. The stream must be closed, as with
     * {@link #scan(String, String)}.
     */
    Stream<Row> scan(String fromInclusive, String toExclusive, Snapshot at);

    void close();

    /** One entry of a scan. */
    record Row(String key, String value) {
    }
}
