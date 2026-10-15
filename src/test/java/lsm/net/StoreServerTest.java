package lsm.net;

import lsm.LSMStoreEngine;
import lsm.sstable.SSTableReader;
import lsm.wal.DurabilityMode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The server over a real socket, and above all the promise that a session
 * dies with its connection however that connection ends.
 */
class StoreServerTest {

    @TempDir
    Path dir;

    private LSMStoreEngine engine;
    private StoreServer server;

    @BeforeEach
    void setUp() throws IOException {
        // A small MemTable so flushes and compaction actually happen during a
        // test, which is what the leak checks below need.
        engine = new LSMStoreEngine(dir, 64, DurabilityMode.BUFFERED, 1);
        server = new StoreServer(engine, 0);
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.close();
        }
        if (engine != null) {
            engine.close();
        }
    }

    /** A line-protocol client, enough to drive the server by hand would. */
    private final class Client implements Closeable {
        private final Socket socket;
        private final BufferedReader in;
        private final Writer out;

        Client() throws IOException {
            this(15_000);
        }

        Client(int timeoutMillis) throws IOException {
            socket = new Socket("127.0.0.1", server.port());
            socket.setSoTimeout(timeoutMillis);
            in = new BufferedReader(new InputStreamReader(
                    socket.getInputStream(), StandardCharsets.UTF_8));
            out = new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8);
        }

        /** Sends a command and reads its single reply line. */
        String send(String command) throws IOException {
            sendRaw(command + "\n");
            return in.readLine();
        }

        /** Sends a command and reads reply lines up to and including END. */
        List<String> sendUntilEnd(String command) throws IOException {
            sendRaw(command + "\n");
            List<String> lines = new ArrayList<>();
            String line;
            while ((line = in.readLine()) != null) {
                lines.add(line);
                if (line.equals("END")) {
                    break;
                }
            }
            return lines;
        }

        void sendRaw(String text) throws IOException {
            out.write(text);
            out.flush();
        }

        String readLine() throws IOException {
            return in.readLine();
        }

        @Override
        public void close() throws IOException {
            socket.close();
        }
    }

    // --- the protocol over a socket ---

    @Test
    void respondsToPing() throws IOException {
        try (Client client = new Client()) {
            assertEquals("PONG", client.send("PING"));
        }
    }

    @Test
    void storesAndReadsBack() throws IOException {
        try (Client client = new Client()) {
            assertEquals("OK", client.send("PUT name ada"));
            assertEquals("VALUE ada", client.send("GET name"));
            assertEquals("OK", client.send("DEL name"));
            assertEquals("NIL", client.send("GET name"));
        }
    }

    @Test
    void scanComesBackAsRowsEndingInEnd() throws IOException {
        try (Client client = new Client()) {
            client.send("PUT a 1");
            client.send("PUT b 2");
            assertEquals(List.of("ROW a 1", "ROW b 2", "END"), client.sendUntilEnd("SCAN"));
        }
    }

    @Test
    void acceptsTelnetStyleLineEndings() throws IOException {
        try (Client client = new Client()) {
            client.sendRaw("PUT k v\r\n");
            assertEquals("OK", client.readLine());
            client.sendRaw("GET k\r\n");
            assertEquals("VALUE k".replace("k", "v"), client.readLine());
        }
    }

    @Test
    void carriesUnicodeValuesIntact() throws IOException {
        try (Client client = new Client()) {
            assertEquals("OK", client.send("PUT greeting 日本語 🚀 café"));
            assertEquals("VALUE 日本語 🚀 café", client.send("GET greeting"));
        }
    }

    @Test
    void aBadCommandDoesNotEndTheConnection() throws IOException {
        try (Client client = new Client()) {
            assertTrue(client.send("WAT").startsWith("ERR"));
            assertEquals("PONG", client.send("PING"), "the connection is still usable");
        }
    }

    @Test
    void twoClientsSeeEachOthersWrites() throws IOException {
        try (Client first = new Client(); Client second = new Client()) {
            first.send("PUT shared yes");
            assertEquals("VALUE yes", second.send("GET shared"));
        }
    }

    @Test
    void anOverlongLineIsRefused() throws IOException {
        try (Client client = new Client()) {
            // No newline is ever sent, so an unbounded reader would buffer
            // this until the server ran out of memory.
            client.sendRaw("PUT k " + "x".repeat(StoreServer.MAX_LINE_LENGTH + 100));
            assertEquals("ERR line too long", client.readLine());
        }
    }

    @Test
    void connectionsBeyondTheCapAreTurnedAwayRatherThanLeftHanging() throws IOException {
        server.close();
        server = new StoreServer(engine, 0, 1);

        try (Client first = new Client()) {
            assertEquals("PONG", first.send("PING"));
            await(() -> server.activeConnections() == 1, "the first connection to register");

            // A short timeout, because the failure being guarded against is
            // silence: a server with no cap serves this connection and simply
            // never says anything about being full.
            try (Client second = new Client(3_000)) {
                String reply;
                try {
                    reply = second.readLine();
                } catch (SocketTimeoutException e) {
                    reply = "<nothing, the connection was left hanging>";
                }
                // Told, not queued: a bounded thread pool would accept this
                // and leave it silently unserved, which reads as a hang.
                assertEquals("ERR server at capacity", reply);
            }
        }
    }

    @Test
    void thePortIsDiscoverableWhenTheOsPicksIt() {
        assertTrue(server.port() > 0);
    }

    /**
     * Every thread the server owns is a daemon, so an embedding application
     * is never held open by one it has finished with. A process whose only
     * job is serving therefore has to wait somewhere, or the JVM exits the
     * instant it finishes starting up and takes the server with it.
     */
    @Test
    void awaitShutdownBlocksUntilTheServerIsClosed() throws Exception {
        AtomicBoolean returned = new AtomicBoolean();
        Thread waiter = new Thread(() -> {
            try {
                server.awaitShutdown();
                returned.set(true);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        waiter.setDaemon(true);
        waiter.start();

        Thread.sleep(100);
        assertFalse(returned.get(), "it returned while the server was still serving");

        server.close();
        waiter.join(10_000);
        assertTrue(returned.get(), "it did not return once the server closed");
    }

    // --- snapshots across commands, which is why connections are stateful ---

    @Test
    void aConnectionKeepsItsSnapshotBetweenCommands() throws IOException {
        try (Client client = new Client()) {
            client.send("PUT k before");
            assertTrue(client.send("SNAPSHOT").startsWith("OK "));
            client.send("PUT k after");

            assertEquals("VALUE before", client.send("GET k"));
            assertEquals(List.of("ROW k before", "END"), client.sendUntilEnd("SCAN"));
        }
    }

    @Test
    void anotherConnectionIsUnaffectedByASnapshot() throws IOException {
        try (Client reader = new Client(); Client writer = new Client()) {
            writer.send("PUT k before");
            reader.send("SNAPSHOT");
            writer.send("PUT k after");

            assertEquals("VALUE before", reader.send("GET k"));
            assertEquals("VALUE after", writer.send("GET k"));
        }
    }

    // --- the lifetime guarantee, one test per way out of a connection ---

    @Test
    void quitReleasesTheSnapshot() throws IOException {
        Client client = new Client();
        try (client) {
            client.send("PUT k before");
            client.send("SNAPSHOT");
            churn(client, 200);
            long held = versionsOnDisk("k");
            assertTrue(held > 50, "the snapshot should be holding history, saw " + held);

            assertEquals("OK", client.send("QUIT"));
            assertHistoryReclaimed(held, "quitting left the snapshot open");
        }
    }

    @Test
    void aClientThatVanishesReleasesItsSnapshot() throws IOException {
        Client client = new Client();
        client.send("PUT k before");
        client.send("SNAPSHOT");
        churn(client, 200);
        long held = versionsOnDisk("k");
        assertTrue(held > 50, "the snapshot should be holding history, saw " + held);

        // No QUIT, no RELEASE: the socket simply goes away, which is what a
        // crashed or unplugged client looks like.
        client.close();

        assertHistoryReclaimed(held, "a vanished client left its snapshot open");
    }

    @Test
    void shuttingDownTheServerReleasesOpenSnapshots() throws IOException {
        Client client = new Client();
        client.send("PUT k before");
        client.send("SNAPSHOT");
        churn(client, 200);
        long held = versionsOnDisk("k");
        assertTrue(held > 50, "the snapshot should be holding history, saw " + held);

        // The connection is still open and its thread is blocked reading.
        server.close();
        server = new StoreServer(engine, 0);

        assertHistoryReclaimed(held, "server shutdown left a snapshot open");
        client.close();
    }

    @Test
    void aConnectionStopsBeingCountedOnceItEnds() throws IOException {
        try (Client client = new Client()) {
            client.send("PING");
            await(() -> server.activeConnections() == 1, "the connection to register");
        }
        await(() -> server.activeConnections() == 0, "the connection to be released");
    }

    /** Writes through a fresh connection until compaction has had its chance. */
    private void churn(Client client, int writes) throws IOException {
        for (int i = 0; i < writes; i++) {
            client.send("PUT k churn" + i);
        }
    }

    /**
     * Drives more writes through a new connection and waits for the history
     * of "k" to collapse, which can only happen once nothing holds a snapshot
     * over it.
     *
     * <p>Compared against what the snapshot was holding rather than against a
     * fixed number, because the floor is not one version. Level 0 keeps up to
     * three files below its compaction trigger, and with a MemTable this small
     * each of those holds several versions of the same key, so a couple of
     * dozen survive even when everything is working. What distinguishes a leak
     * is the order of magnitude: released history collapses by well over half,
     * a leak retains all of it and keeps growing.
     *
     * <p>Checked on disk rather than through the API on purpose: a leaked
     * snapshot changes no answer the store gives, it only stops the space ever
     * coming back.
     */
    private void assertHistoryReclaimed(long whileHeld, String whatWentWrong)
            throws IOException {
        try (Client fresh = new Client()) {
            // Compaction is driven by flushes, so it needs writes to notice
            // that the watermark has moved.
            churn(fresh, 200);
        }

        long target = whileHeld / 4;
        Instant deadline = Instant.now().plus(Duration.ofSeconds(20));
        long seen = versionsOnDisk("k");
        while (seen >= target && Instant.now().isBefore(deadline)) {
            sleep();
            seen = versionsOnDisk("k");
        }
        assertTrue(seen < target, whatWentWrong + ": " + seen
                + " versions still on disk, against " + whileHeld + " while it was held");
    }

    private void await(BooleanSupplier condition, String what) {
        Instant deadline = Instant.now().plus(Duration.ofSeconds(10));
        while (!condition.getAsBoolean() && Instant.now().isBefore(deadline)) {
            sleep();
        }
        assertTrue(condition.getAsBoolean(), "timed out waiting for " + what);
    }

    private static void sleep() {
        try {
            Thread.sleep(25);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
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
                // so its contents live somewhere else now.
            }
        }
        return found;
    }
}
