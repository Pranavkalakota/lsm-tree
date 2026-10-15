package lsm.net;

import lsm.Snapshot;
import lsm.StorageEngine;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * One client's conversation with the store: a command in, a response out.
 *
 * <p>Deliberately knows nothing about sockets. Everything interesting about a
 * session is in what it remembers between commands, and that is testable
 * without a network, so the parsing, the command semantics and the snapshot
 * lifetime are all exercised directly rather than through a connection.
 *
 * <p>A session may hold a snapshot, which is the whole reason it exists rather
 * than each request being independent. While one is open, reads through this
 * session see the store as it was when {@code SNAPSHOT} ran, however much
 * other clients have written since. Writes are not held back and are visible
 * to everyone immediately: this gives a client a consistent view to read, not
 * a transaction to commit.
 *
 * <p>Closing is not optional. An open snapshot stops the store reclaiming any
 * version underneath it, so a session abandoned without closing would pin
 * history for as long as the process lives. {@link #close} releases it, and
 * the server calls it on every path out of a connection.
 */
public final class Session implements AutoCloseable {

    /** The protocol's reply to a command that said nothing. */
    private static final Response NOTHING = new Response(List.of(), false);

    private final StorageEngine engine;
    private Snapshot snapshot;
    private boolean closed;

    public Session(StorageEngine engine) {
        this.engine = engine;
    }

    /**
     * What one command produced, and whether the connection should now end.
     *
     * <p>{@code QUIT} is the only command that asks for the latter, and it
     * still gets a reply first, so a client knows it was heard rather than
     * seeing the socket vanish.
     */
    public record Response(List<String> lines, boolean endConnection) {

        static Response of(String line) {
            return new Response(List.of(line), false);
        }

        static Response error(String message) {
            return of("ERR " + message);
        }
    }

    /** The version this session reads at, or empty when it has no snapshot. */
    public Optional<Long> snapshotVersion() {
        return snapshot == null ? Optional.empty() : Optional.of(snapshot.version());
    }

    /**
     * Runs one command line.
     *
     * <p>A failing command answers with an error and leaves the session usable.
     * A client that asks for a missing key, or sends nonsense, has not done
     * anything that should cost it its connection or its snapshot.
     */
    public Response execute(String line) {
        if (closed) {
            throw new IllegalStateException("Session is closed");
        }
        if (line == null) {
            return NOTHING;
        }
        String trimmed = line.strip();
        if (trimmed.isEmpty()) {
            return NOTHING;
        }

        // Three parts at most: the verb, the key, and a value that is allowed
        // to contain spaces because it is whatever is left of the line.
        String[] parts = trimmed.split(" ", 3);
        String command = parts[0].toUpperCase(Locale.ROOT);

        try {
            return switch (command) {
                case "PUT" -> put(parts);
                case "GET" -> get(parts);
                case "DEL", "DELETE" -> delete(parts);
                case "SCAN" -> scan(parts);
                case "SNAPSHOT" -> openSnapshot();
                case "RELEASE" -> releaseSnapshot();
                case "PING" -> Response.of("PONG");
                case "QUIT" -> new Response(List.of("OK"), true);
                default -> Response.error("unknown command: " + parts[0]);
            };
        } catch (IllegalStateException e) {
            // The store was closed under this session, or its snapshot was.
            return Response.error(message(e));
        } catch (IllegalArgumentException e) {
            return Response.error(message(e));
        } catch (RuntimeException e) {
            // An I/O failure in the engine surfaces as an unchecked wrapper.
            // One client's bad luck should not take the server down with it.
            return Response.error(message(e));
        }
    }

    private Response put(String[] parts) {
        if (parts.length < 3) {
            return Response.error("usage: PUT <key> <value>");
        }
        engine.put(parts[1], parts[2]);
        return Response.of("OK");
    }

    private Response get(String[] parts) {
        if (parts.length != 2) {
            return Response.error("usage: GET <key>");
        }
        Optional<String> value = snapshot == null
                ? engine.get(parts[1])
                : engine.get(parts[1], snapshot);
        return value.map(v -> Response.of("VALUE " + v)).orElseGet(() -> Response.of("NIL"));
    }

    private Response delete(String[] parts) {
        if (parts.length != 2) {
            return Response.error("usage: DEL <key>");
        }
        engine.delete(parts[1]);
        return Response.of("OK");
    }

    /**
     * {@code SCAN}, optionally bounded. The reply is several lines ending in
     * {@code END}, so a client knows where the rows stop without being told
     * how many to expect up front.
     */
    private Response scan(String[] parts) {
        String from = parts.length > 1 && !parts[1].equals("-") ? parts[1] : null;
        String to = parts.length > 2 && !parts[2].equals("-") ? parts[2] : null;

        List<String> lines = new ArrayList<>();
        try (Stream<StorageEngine.Row> rows = snapshot == null
                ? engine.scan(from, to)
                : engine.scan(from, to, snapshot)) {
            rows.forEach(row -> lines.add("ROW " + row.key() + " " + row.value()));
        }
        lines.add("END");
        return new Response(lines, false);
    }

    private Response openSnapshot() {
        if (snapshot != null) {
            // Replacing it silently would strand the history the old one was
            // holding, and leave the client unsure which view it is reading.
            return Response.error("snapshot already open at version " + snapshot.version());
        }
        snapshot = engine.snapshot();
        return Response.of("OK " + snapshot.version());
    }

    private Response releaseSnapshot() {
        if (snapshot == null) {
            return Response.error("no snapshot open");
        }
        snapshot.close();
        snapshot = null;
        return Response.of("OK");
    }

    private static String message(RuntimeException e) {
        return e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
    }

    /** Releases any snapshot this session held. Idempotent. */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        if (snapshot != null) {
            snapshot.close();
            snapshot = null;
        }
    }
}
