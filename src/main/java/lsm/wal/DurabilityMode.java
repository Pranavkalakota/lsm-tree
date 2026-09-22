package lsm.wal;

/**
 * How hard the write-ahead log works to make a write survive.
 *
 * <p>The choice is a throughput/durability trade, not a correctness one: both
 * modes replay identically, they differ only in what class of failure can lose
 * the tail of the log.
 */
public enum DurabilityMode {

    /**
     * A write returns once the bytes reach the operating system.
     *
     * <p>Survives the process dying, because the page cache outlives it, and is
     * roughly three orders of magnitude faster than {@link #SYNC}. Does not
     * survive the machine losing power with the tail still uncommitted. This is
     * the default for the same reason RocksDB and LevelDB default to it: almost
     * every real failure is a process failure.
     */
    BUFFERED,

    /**
     * A write returns only once the disk has confirmed the bytes.
     *
     * <p>Survives power loss. Costs one fsync per commit, measured at ~3.8ms,
     * so a single writer is capped near 270 writes/sec. Concurrent writers
     * share fsyncs via group commit, which is what makes this mode scale at all.
     */
    SYNC
}
