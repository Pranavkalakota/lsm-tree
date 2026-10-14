package lsm.sstable;

import lsm.memtable.VersionedKey;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Merges several sorted tables into one ascending stream of records.
 *
 * <p>Every version of every key comes through, in {@link VersionedKey} order:
 * keys ascending, and within a key newest version first. Nothing is collapsed
 * here. Deciding which versions of a key are still worth keeping needs to know
 * what the oldest live snapshot is, and this class has no business knowing
 * that, so the policy belongs to the caller: a scan keeps the newest version a
 * reader can see, a compaction keeps whatever a snapshot might still ask for.
 *
 * <p>Sources are given newest first, which only matters when two of them hold
 * the very same version of the same key. That is the same write seen twice, so
 * one copy is dropped and the newer source's wins. Any genuine difference in
 * version is a different record and both survive.
 *
 * <p>Tombstones are emitted like any other record. Only the caller knows
 * whether a level below still holds a value the tombstone has to keep hiding,
 * so the decision to drop one is not made here either.
 */
public final class MergingCursor implements Closeable {

    private final List<RowSource> sources;
    private final PriorityQueue<Head> heap;

    /** @param sources the inputs, newest first */
    public MergingCursor(List<? extends RowSource> sources) {
        this.sources = new ArrayList<>(sources);
        this.heap = new PriorityQueue<>(
                Comparator.comparing(Head::key).thenComparingInt(Head::source));
        for (int i = 0; i < this.sources.size(); i++) {
            RowSource cursor = this.sources.get(i);
            if (cursor.hasNext()) {
                heap.add(new Head(i, cursor.current().versionedKey()));
            }
        }
    }

    public boolean hasNext() {
        return !heap.isEmpty();
    }

    /**
     * Returns the next record in order, discarding any duplicate of it held by
     * an older source.
     */
    public SSTableReader.Row next() throws IOException {
        Head winner = heap.poll();
        if (winner == null) {
            return null;
        }
        VersionedKey key = winner.key();
        SSTableReader.Row row = advance(winner.source());

        // The same version of the same key in another source is the same write,
        // so only one copy goes through. An older VERSION is a different record
        // and is left in the heap to come out on its own.
        while (!heap.isEmpty() && heap.peek().key().equals(key)) {
            advance(heap.poll().source());
        }
        return row;
    }

    /** Steps one source forward and re-queues it if it has more to give. */
    private SSTableReader.Row advance(int source) throws IOException {
        RowSource cursor = sources.get(source);
        SSTableReader.Row row = cursor.next();
        if (cursor.hasNext()) {
            heap.add(new Head(source, cursor.current().versionedKey()));
        }
        return row;
    }

    @Override
    public void close() {
        heap.clear();
        sources.clear();
    }

    private record Head(int source, VersionedKey key) {
    }
}
