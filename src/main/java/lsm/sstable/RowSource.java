package lsm.sstable;

import lsm.memtable.Entry;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * An ascending stream of entries that a merge can consume.
 *
 * <p>Compaction merges tables, but a range scan has to merge tables *and* the
 * MemTable, which is an in-memory map rather than a file. This is the shape
 * they have in common, so {@link MergingCursor} can drive either without
 * caring which it is holding.
 */
public interface RowSource {

    /** Whether {@link #current} has an entry to give. */
    boolean hasNext();

    /** The entry the source sits on, or null once it is exhausted. */
    SSTableReader.Row current();

    /** Returns the current entry and advances past it. */
    SSTableReader.Row next() throws IOException;

    /**
     * Wraps an already-sorted iterator, such as a MemTable range view.
     *
     * <p>The caller owns the ordering guarantee: a merge assumes every source
     * ascends, and a source that does not will silently produce wrong results
     * rather than fail.
     */
    static RowSource of(Iterator<Map.Entry<String, Entry>> entries) {
        return new RowSource() {
            private SSTableReader.Row current = pull();

            private SSTableReader.Row pull() {
                if (!entries.hasNext()) {
                    return null;
                }
                Map.Entry<String, Entry> entry = entries.next();
                return new SSTableReader.Row(entry.getKey(), entry.getValue());
            }

            @Override
            public boolean hasNext() {
                return current != null;
            }

            @Override
            public SSTableReader.Row current() {
                return current;
            }

            @Override
            public SSTableReader.Row next() {
                SSTableReader.Row row = current;
                current = pull();
                return row;
            }
        };
    }
}
