package lsm.compaction;

import lsm.sstable.BlockCache;
import lsm.sstable.MergingCursor;
import lsm.sstable.SSTableReader;
import lsm.sstable.SSTableWriter;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.LongSupplier;

/**
 * Chooses and performs leveled compactions.
 *
 * <p>Level 0 holds whole MemTable snapshots, so its tables overlap each other
 * freely and a lookup has to consult all of them. Every level below holds
 * tables with disjoint key ranges, so at most one table per level can contain a
 * given key. That is the point of leveling: read cost grows with the number of
 * levels, which is logarithmic in the data size, rather than with the number of
 * files ever written.
 *
 * <p>A compaction takes some tables from level N, every table in level N+1
 * whose range overlaps them, and merges the lot into fresh level N+1 tables.
 * Because the inputs cover a contiguous key range and the output is written in
 * order, the result stays disjoint from whatever else level N+1 still holds.
 */
public final class Compactor {

    /** Level 0 compacts on file count, since its tables overlap and all get read. */
    public static final int L0_TRIGGER = 4;

    /** Level 1's byte budget; each level below is ten times the one above. */
    public static final long LEVEL1_BYTES = 8L * 1024 * 1024;

    /** Output tables are cut at roughly this size to keep levels made of many. */
    public static final long TARGET_FILE_BYTES = 2L * 1024 * 1024;

    private final Path dataDir;
    private final BlockCache blockCache;
    private final LongSupplier nextSequence;

    public Compactor(Path dataDir, BlockCache blockCache, LongSupplier nextSequence) {
        this.dataDir = dataDir;
        this.blockCache = blockCache;
        this.nextSequence = nextSequence;
    }

    /** The inputs of one compaction, and where its output belongs. */
    public record Job(int outputLevel, List<SSTableReader> inputs, boolean mayDropTombstones) {
    }

    /**
     * Picks the most urgent compaction, or null when every level is within
     * budget. Level 0 is considered first because it is the one level whose
     * overgrowth slows down every single read.
     */
    public Job choose(List<SSTableReader> tables) {
        int deepest = tables.stream().mapToInt(SSTableReader::level).max().orElse(0);

        for (int level = 0; level <= deepest; level++) {
            List<SSTableReader> atLevel = atLevel(tables, level);
            if (!isOverBudget(atLevel, level)) {
                continue;
            }

            List<SSTableReader> seed = level == 0
                    ? atLevel                      // L0 tables overlap, so take them all
                    : List.of(oldest(atLevel));    // deeper levels are disjoint, one suffices

            String lo = seed.stream().map(SSTableReader::minKey)
                    .filter(k -> k != null).min(Comparator.naturalOrder()).orElse(null);
            String hi = seed.stream().map(SSTableReader::maxKey)
                    .filter(k -> k != null).max(Comparator.naturalOrder()).orElse(null);
            if (lo == null) {
                continue;   // nothing but empty tables; merging them buys nothing
            }

            List<SSTableReader> inputs = new ArrayList<>(seed);
            for (SSTableReader table : atLevel(tables, level + 1)) {
                if (table.overlaps(lo, hi)) {
                    inputs.add(table);
                }
            }

            // Dropping a tombstone is only safe when nothing below could still
            // hold a value for that key; otherwise the delete silently undoes
            // itself and the old value comes back.
            int outputLevel = level + 1;
            boolean bottom = tables.stream().noneMatch(t -> t.level() > outputLevel);
            return new Job(outputLevel, inputs, bottom);
        }
        return null;
    }

    /**
     * Merges a job's inputs into new tables and returns them. The caller
     * installs the result and retires the inputs; nothing here touches the
     * engine's view, so this can run off the write path.
     */
    public List<Path> run(Job job) throws IOException {
        List<SSTableReader.Cursor> cursors = new ArrayList<>();
        // Newest first: lower level wins, and within a level higher sequence wins.
        List<SSTableReader> ordered = new ArrayList<>(job.inputs());
        ordered.sort(Comparator.comparingInt(SSTableReader::level)
                .thenComparing(Comparator.comparingLong(SSTableReader::sequence).reversed()));
        for (SSTableReader table : ordered) {
            cursors.add(table.cursor());
        }

        List<Path> written = new ArrayList<>();
        SSTableWriter writer = null;
        long writtenBytes = 0;

        try (MergingCursor merged = new MergingCursor(cursors)) {
            while (merged.hasNext()) {
                SSTableReader.Row row = merged.next();
                if (row.value().isTombstone() && job.mayDropTombstones()) {
                    continue;
                }

                if (writer == null) {
                    Path path = dataDir.resolve(String.format(
                            "L%d_%06d.sst", job.outputLevel(), nextSequence.getAsLong()));
                    writer = SSTableWriter.create(path);
                    written.add(path);
                    writtenBytes = 0;
                }

                writer.add(row.key(), row.value());
                writtenBytes += row.key().length()
                        + row.value().value().map(String::length).orElse(0);

                if (writtenBytes >= TARGET_FILE_BYTES) {
                    writer.finish();
                    writer.close();
                    writer = null;
                }
            }
            if (writer != null) {
                writer.finish();
            }
        } catch (IOException e) {
            if (writer != null) {
                writer.close();
            }
            throw e;
        } finally {
            if (writer != null) {
                writer.close();
            }
        }
        return written;
    }

    /** Opens readers over freshly written tables so they can join the live set. */
    public List<SSTableReader> open(List<Path> paths) throws IOException {
        List<SSTableReader> readers = new ArrayList<>();
        try {
            for (Path path : paths) {
                readers.add(new SSTableReader(path, blockCache));
            }
        } catch (IOException e) {
            readers.forEach(SSTableReader::close);
            throw e;
        }
        return readers;
    }

    private boolean isOverBudget(List<SSTableReader> atLevel, int level) {
        if (atLevel.isEmpty()) {
            return false;
        }
        if (level == 0) {
            return atLevel.size() >= L0_TRIGGER;
        }
        long budget = LEVEL1_BYTES;
        for (int i = 1; i < level; i++) {
            budget *= 10;
        }
        return totalBytes(atLevel) > budget;
    }

    private static long totalBytes(List<SSTableReader> tables) {
        long total = 0;
        for (SSTableReader table : tables) {
            try {
                total += table.sizeBytes();
            } catch (IOException e) {
                // A table we cannot stat is one we should not be sizing a
                // compaction around; treat it as contributing nothing.
            }
        }
        return total;
    }

    private static List<SSTableReader> atLevel(List<SSTableReader> tables, int level) {
        return tables.stream().filter(t -> t.level() == level).toList();
    }

    private static SSTableReader oldest(List<SSTableReader> tables) {
        return tables.stream()
                .min(Comparator.comparingLong(SSTableReader::sequence))
                .orElseThrow();
    }
}
