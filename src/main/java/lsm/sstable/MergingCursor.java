package lsm.sstable;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Merges several sorted tables into one ascending stream of entries.
 *
 * <p>Sources are given newest first. When more than one holds the same key the
 * newest wins and the rest are discarded, which is how a compaction collapses
 * the history of a key down to its current value. That ordering is the whole
 * correctness argument: get it backwards and compaction resurrects overwritten
 * data.
 *
 * <p>Tombstones are emitted like any other entry. Only the caller knows whether
 * a level below still holds a value the tombstone has to keep hiding, so the
 * decision to drop one is not made here.
 */
public final class MergingCursor implements Closeable {

    private final List<SSTableReader.Cursor> sources;
    private final PriorityQueue<Head> heap;

    /** @param sources cursors over the inputs, newest first */
    public MergingCursor(List<SSTableReader.Cursor> sources) {
        this.sources = new ArrayList<>(sources);
        this.heap = new PriorityQueue<>(
                Comparator.comparing(Head::key).thenComparingInt(Head::source));
        for (int i = 0; i < this.sources.size(); i++) {
            SSTableReader.Cursor cursor = this.sources.get(i);
            if (cursor.hasNext()) {
                heap.add(new Head(i, cursor.current().key()));
            }
        }
    }

    public boolean hasNext() {
        return !heap.isEmpty();
    }

    /**
     * Returns the winning entry for the next key, consuming every source that
     * also holds it.
     */
    public SSTableReader.Row next() throws IOException {
        Head winner = heap.poll();
        if (winner == null) {
            return null;
        }
        String key = winner.key();
        SSTableReader.Row row = advance(winner.source());

        // Same key in an older source is a superseded version; drop it.
        while (!heap.isEmpty() && heap.peek().key().equals(key)) {
            advance(heap.poll().source());
        }
        return row;
    }

    /** Steps one source forward and re-queues it if it has more to give. */
    private SSTableReader.Row advance(int source) throws IOException {
        SSTableReader.Cursor cursor = sources.get(source);
        SSTableReader.Row row = cursor.next();
        if (cursor.hasNext()) {
            heap.add(new Head(source, cursor.current().key()));
        }
        return row;
    }

    @Override
    public void close() {
        heap.clear();
        sources.clear();
    }

    private record Head(int source, String key) {
    }
}
