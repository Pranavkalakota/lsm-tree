package lsm.net;

import lsm.LSMStoreEngine;
import lsm.sstable.SSTableReader;
import lsm.wal.DurabilityMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/** One client's commands, and the snapshot a session holds between them. */
class SessionTest {

    @TempDir
    Path dir;

    private LSMStoreEngine open() {
        return open(4 * 1024 * 1024);
    }

    private LSMStoreEngine open(long memTableBytes) {
        return new LSMStoreEngine(dir, memTableBytes, DurabilityMode.BUFFERED, 1);
    }

    /** The single reply line, for the commands that produce exactly one. */
    private String reply(Session session, String line) {
        Session.Response response = session.execute(line);
        assertEquals(1, response.lines().size(),
                "expected one line for \"" + line + "\", got " + response.lines());
        return response.lines().get(0);
    }

    @Test
    void putThenGet() {
        LSMStoreEngine engine = open();
        try (Session session = new Session(engine)) {
            assertEquals("OK", reply(session, "PUT name ada"));
            assertEquals("VALUE ada", reply(session, "GET name"));
        } finally {
            engine.close();
        }
    }

    @Test
    void getMissingKeyIsNil() {
        LSMStoreEngine engine = open();
        try (Session session = new Session(engine)) {
            assertEquals("NIL", reply(session, "GET absent"));
        } finally {
            engine.close();
        }
    }

    @Test
    void deleteThenGetIsNil() {
        LSMStoreEngine engine = open();
        try (Session session = new Session(engine)) {
            reply(session, "PUT k v");
            assertEquals("OK", reply(session, "DEL k"));
            assertEquals("NIL", reply(session, "GET k"));
        } finally {
            engine.close();
        }
    }

    @Test
    void aValueMayContainSpaces() {
        LSMStoreEngine engine = open();
        try (Session session = new Session(engine)) {
            reply(session, "PUT greeting hello there world");
            assertEquals("VALUE hello there world", reply(session, "GET greeting"));
        } finally {
            engine.close();
        }
    }

    @Test
    void commandsAreCaseInsensitive() {
        LSMStoreEngine engine = open();
        try (Session session = new Session(engine)) {
            reply(session, "put k v");
            assertEquals("VALUE v", reply(session, "Get k"));
            assertEquals("PONG", reply(session, "ping"));
        } finally {
            engine.close();
        }
    }

    @Test
    void keysKeepTheirCase() {
        LSMStoreEngine engine = open();
        try (Session session = new Session(engine)) {
            reply(session, "put Key Value");
            assertEquals("VALUE Value", reply(session, "get Key"));
            assertEquals("NIL", reply(session, "get key"));
        } finally {
            engine.close();
        }
    }

    @Test
    void anUnknownCommandLeavesTheSessionUsable() {
        LSMStoreEngine engine = open();
        try (Session session = new Session(engine)) {
            assertTrue(reply(session, "FROBNICATE k").startsWith("ERR unknown command"));
            assertEquals("OK", reply(session, "PUT k v"), "the session still works");
        } finally {
            engine.close();
        }
    }

    @Test
    void malformedCommandsAnswerWithUsage() {
        LSMStoreEngine engine = open();
        try (Session session = new Session(engine)) {
            assertTrue(reply(session, "PUT k").startsWith("ERR usage: PUT"));
            assertTrue(reply(session, "GET").startsWith("ERR usage: GET"));
            assertTrue(reply(session, "GET a b").startsWith("ERR usage: GET"));
            assertTrue(reply(session, "DEL").startsWith("ERR usage: DEL"));
        } finally {
            engine.close();
        }
    }

    @Test
    void blankLinesProduceNoReply() {
        LSMStoreEngine engine = open();
        try (Session session = new Session(engine)) {
            assertTrue(session.execute("").lines().isEmpty());
            assertTrue(session.execute("   ").lines().isEmpty());
            assertTrue(session.execute(null).lines().isEmpty());
        } finally {
            engine.close();
        }
    }

    @Test
    void quitRepliesBeforeAskingToEnd() {
        LSMStoreEngine engine = open();
        try (Session session = new Session(engine)) {
            Session.Response response = session.execute("QUIT");
            assertEquals(List.of("OK"), response.lines(),
                    "a client should hear that it was understood");
            assertTrue(response.endConnection());
        } finally {
            engine.close();
        }
    }

    @Test
    void onlyQuitEndsTheConnection() {
        LSMStoreEngine engine = open();
        try (Session session = new Session(engine)) {
            assertFalse(session.execute("PING").endConnection());
            assertFalse(session.execute("NONSENSE").endConnection());
            assertFalse(session.execute("GET").endConnection());
        } finally {
            engine.close();
        }
    }

    // --- scans ---

    @Test
    void scanReturnsRowsThenEnd() {
        LSMStoreEngine engine = open();
        try (Session session = new Session(engine)) {
            reply(session, "PUT a 1");
            reply(session, "PUT b 2");

            Session.Response response = session.execute("SCAN");
            assertEquals(List.of("ROW a 1", "ROW b 2", "END"), response.lines());
        } finally {
            engine.close();
        }
    }

    @Test
    void scanTakesBounds() {
        LSMStoreEngine engine = open();
        try (Session session = new Session(engine)) {
            reply(session, "PUT a 1");
            reply(session, "PUT b 2");
            reply(session, "PUT c 3");

            assertEquals(List.of("ROW b 2", "END"), session.execute("SCAN b c").lines());
            // A dash stands in for an open bound, since the bounds are
            // positional and an absent lower bound still needs a placeholder.
            assertEquals(List.of("ROW a 1", "ROW b 2", "END"),
                    session.execute("SCAN - c").lines());
            assertEquals(List.of("ROW b 2", "ROW c 3", "END"),
                    session.execute("SCAN b -").lines());
        } finally {
            engine.close();
        }
    }

    @Test
    void anEmptyScanIsJustEnd() {
        LSMStoreEngine engine = open();
        try (Session session = new Session(engine)) {
            assertEquals(List.of("END"), session.execute("SCAN").lines());
        } finally {
            engine.close();
        }
    }

    // --- snapshots, which are the reason a session exists ---

    @Test
    void aSnapshotReportsItsVersion() {
        LSMStoreEngine engine = open();
        try (Session session = new Session(engine)) {
            reply(session, "PUT k v");
            assertTrue(reply(session, "SNAPSHOT").startsWith("OK "));
            assertTrue(session.snapshotVersion().isPresent());
        } finally {
            engine.close();
        }
    }

    @Test
    void readsThroughASnapshotIgnoreLaterWrites() {
        LSMStoreEngine engine = open();
        try (Session session = new Session(engine)) {
            reply(session, "PUT k before");
            reply(session, "SNAPSHOT");
            reply(session, "PUT k after");

            assertEquals("VALUE before", reply(session, "GET k"));
            assertEquals(List.of("ROW k before", "END"), session.execute("SCAN").lines());
        } finally {
            engine.close();
        }
    }

    @Test
    void aSnapshotHidesLaterDeletesAndLaterKeys() {
        LSMStoreEngine engine = open();
        try (Session session = new Session(engine)) {
            reply(session, "PUT k v");
            reply(session, "SNAPSHOT");
            reply(session, "DEL k");
            reply(session, "PUT fresh 1");

            assertEquals("VALUE v", reply(session, "GET k"));
            assertEquals("NIL", reply(session, "GET fresh"));
        } finally {
            engine.close();
        }
    }

    @Test
    void writesFromASessionHoldingASnapshotAreVisibleToOthers() {
        LSMStoreEngine engine = open();
        Session reader = new Session(engine);
        try (reader; Session other = new Session(engine)) {
            reply(reader, "PUT k before");
            reply(reader, "SNAPSHOT");
            reply(reader, "PUT k after");

            // A snapshot is a consistent view to read, not a transaction: the
            // write lands for everyone at once, including a session that
            // cannot see it through its own snapshot.
            assertEquals("VALUE before", reply(reader, "GET k"));
            assertEquals("VALUE after", reply(other, "GET k"));
        } finally {
            engine.close();
        }
    }

    @Test
    void releasingASnapshotGoesBackToReadingTheLatest() {
        LSMStoreEngine engine = open();
        try (Session session = new Session(engine)) {
            reply(session, "PUT k before");
            reply(session, "SNAPSHOT");
            reply(session, "PUT k after");
            assertEquals("VALUE before", reply(session, "GET k"));

            assertEquals("OK", reply(session, "RELEASE"));
            assertTrue(session.snapshotVersion().isEmpty());
            assertEquals("VALUE after", reply(session, "GET k"));
        } finally {
            engine.close();
        }
    }

    @Test
    void openingASecondSnapshotIsRefused() {
        LSMStoreEngine engine = open();
        try (Session session = new Session(engine)) {
            reply(session, "SNAPSHOT");
            long version = session.snapshotVersion().orElseThrow();

            // Silently replacing it would strand the history the first one
            // held and leave the client unsure which view it is reading.
            assertTrue(reply(session, "SNAPSHOT").startsWith("ERR snapshot already open"));
            assertEquals(version, session.snapshotVersion().orElseThrow());
        } finally {
            engine.close();
        }
    }

    @Test
    void releasingWithoutASnapshotIsRefused() {
        LSMStoreEngine engine = open();
        try (Session session = new Session(engine)) {
            assertEquals("ERR no snapshot open", reply(session, "RELEASE"));
        } finally {
            engine.close();
        }
    }

    @Test
    void twoSessionsHoldTheirOwnSnapshots() {
        LSMStoreEngine engine = open();
        try (Session first = new Session(engine); Session second = new Session(engine)) {
            reply(first, "PUT k one");
            reply(first, "SNAPSHOT");
            reply(first, "PUT k two");
            reply(second, "SNAPSHOT");
            reply(first, "PUT k three");

            assertEquals("VALUE one", reply(first, "GET k"));
            assertEquals("VALUE two", reply(second, "GET k"));
        } finally {
            engine.close();
        }
    }

    // --- the lifetime guarantee ---

    @Test
    void closingASessionReleasesItsSnapshot() {
        LSMStoreEngine engine = open();
        try {
            Session session = new Session(engine);
            reply(session, "PUT k v");
            reply(session, "SNAPSHOT");
            assertTrue(session.snapshotVersion().isPresent());

            session.close();
            assertTrue(session.snapshotVersion().isEmpty());
            session.close();   // idempotent, so a finally block is safe
        } finally {
            engine.close();
        }
    }

    @Test
    void aClosedSessionRefusesCommands() {
        LSMStoreEngine engine = open();
        try {
            Session session = new Session(engine);
            session.close();
            assertThrows(IllegalStateException.class, () -> session.execute("PING"));
        } finally {
            engine.close();
        }
    }

    /**
     * The failure this whole design exists to prevent: a session that goes
     * away without releasing its snapshot would pin every version underneath
     * it for the life of the process, and the store could never reclaim that
     * space again.
     *
     * <p>Checked by watching the disk rather than the API, because the leak is
     * not visible in any answer the store gives — reads stay correct either
     * way. Only the space does not come back.
     */
    @Test
    void anAbandonedSessionDoesNotPinHistoryForever() throws IOException {
        LSMStoreEngine engine = open(64);
        try {
            Session session = new Session(engine);
            session.execute("PUT k before");
            session.execute("SNAPSHOT");

            for (int i = 0; i < 400; i++) {
                session.execute("PUT k churn" + i);
            }
            long whileHeld = versionsOnDisk("k");
            assertTrue(whileHeld > 50,
                    "a live snapshot should be holding its history, saw " + whileHeld);

            // However the connection ended, this is the call the server owes
            // the store.
            session.close();

            // Compaction is driven by flushes, so more writing is what gives
            // it the chance to notice the watermark has moved.
            for (int i = 0; i < 400; i++) {
                session2(engine).execute("PUT k more" + i);
            }

            long afterRelease = awaitVersionsBelow("k", 10);
            assertTrue(afterRelease < 10,
                    "history stayed pinned after the session closed: " + afterRelease
                            + " versions still on disk");
        } finally {
            if (writer != null) {
                writer.close();
            }
            engine.close();
        }
    }

    /**
     * Waits for the on-disk history of a key to fall below {@code target},
     * since compaction runs on its own thread. Returns what it settled on, so
     * a timeout fails the caller's assertion rather than this method.
     */
    private long awaitVersionsBelow(String key, long target) throws IOException {
        Instant deadline = Instant.now().plus(Duration.ofSeconds(20));
        long seen = versionsOnDisk(key);
        while (seen >= target && Instant.now().isBefore(deadline)) {
            try {
                Thread.sleep(25);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            seen = versionsOnDisk(key);
        }
        return seen;
    }

    private Session writer;

    /** A session to write through once the one under test has been closed. */
    private Session session2(LSMStoreEngine engine) {
        if (writer == null) {
            writer = new Session(engine);
        }
        return writer;
    }

    /** How many versions of a key are stored across every table on disk. */
    private long versionsOnDisk(String key) throws IOException {
        List<Path> tables;
        try (Stream<Path> files = Files.list(dir)) {
            tables = files.filter(p -> p.getFileName().toString().endsWith(".sst")).toList();
        }

        long found = 0;
        for (Path path : tables) {
            try (SSTableReader reader = new SSTableReader(path)) {
                SSTableReader.Cursor cursor = reader.cursor();
                while (cursor.hasNext()) {
                    if (cursor.next().key().equals(key)) {
                        found++;
                    }
                }
            } catch (IOException e) {
                // Compaction retired the file between listing and opening it,
                // which simply means its contents live somewhere else now.
            }
        }
        return found;
    }
}
