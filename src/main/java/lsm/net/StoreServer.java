package lsm.net;

import lsm.LSMStoreEngine;
import lsm.StorageEngine;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Serves the store over TCP, one {@link Session} per connection.
 *
 * <p>Connections are stateful on purpose: a client can open a snapshot and
 * keep reading the same point in history across many commands, which is the
 * thing a stateless request-per-connection protocol cannot offer.
 *
 * <p>That state is also the hazard. An open snapshot stops the store
 * reclaiming any version beneath it, so a connection that goes away without
 * releasing one would pin that history for as long as the process lives. The
 * guarantee this class exists to make is that a session is closed on every
 * path out of a connection: an orderly {@code QUIT}, a client that simply
 * vanishes, a command that throws, and the server itself shutting down with
 * connections still open. Nothing about a leak is visible in the answers the
 * store gives — reads stay correct either way — so it has to be structural
 * rather than something a client is trusted to do.
 *
 * <p>The engine is borrowed, not owned: closing the server leaves it open for
 * whoever created it.
 */
public final class StoreServer implements Closeable {

    /** Connections served at once before new ones are turned away. */
    public static final int DEFAULT_MAX_CONNECTIONS = 128;

    /**
     * Longest command line accepted, in characters. A line protocol with no
     * bound lets one client make the server allocate without limit simply by
     * never sending a newline.
     */
    public static final int MAX_LINE_LENGTH = 64 * 1024;

    private final StorageEngine engine;
    private final int maxConnections;
    private final ServerSocket socket;
    private final ExecutorService connections;
    private final Thread acceptor;

    /** Live sockets, so shutdown can reach into connections that are blocked on a read. */
    private final Set<Socket> open = ConcurrentHashMap.newKeySet();

    private final AtomicInteger active = new AtomicInteger();
    private volatile boolean closed;

    /** Binds and starts serving. Port 0 asks the OS for a free one. */
    public StoreServer(StorageEngine engine, int port) throws IOException {
        this(engine, port, DEFAULT_MAX_CONNECTIONS);
    }

    public StoreServer(StorageEngine engine, int port, int maxConnections) throws IOException {
        this.engine = engine;
        this.maxConnections = maxConnections;
        this.socket = new ServerSocket();
        this.socket.setReuseAddress(true);
        this.socket.bind(new InetSocketAddress("127.0.0.1", port));

        this.connections = Executors.newCachedThreadPool(runnable -> {
            Thread thread = new Thread(runnable, "lsm-connection");
            thread.setDaemon(true);
            return thread;
        });
        this.acceptor = new Thread(this::acceptLoop, "lsm-accept");
        this.acceptor.setDaemon(true);
        this.acceptor.start();
    }

    /** The port actually bound, which matters when port 0 was requested. */
    public int port() {
        return socket.getLocalPort();
    }

    /** Connections currently being served. */
    public int activeConnections() {
        return active.get();
    }

    /**
     * Blocks until the server stops accepting, which is what a process whose
     * only job is to serve needs.
     *
     * <p>Every thread here is a daemon, so that an embedding application is
     * never held open by a server it has finished with. The cost is that a
     * {@code main} which merely starts one and returns would exit immediately,
     * taking the server with it, so a standalone process waits here.
     */
    public void awaitShutdown() throws InterruptedException {
        acceptor.join();
    }

    private void acceptLoop() {
        while (!closed) {
            Socket client;
            try {
                client = socket.accept();
            } catch (IOException e) {
                return;   // the listening socket was closed; nothing left to accept
            }

            // Turned away explicitly rather than queued. A bounded thread pool
            // would accept the connection and then leave it sitting unserved,
            // which looks to the client exactly like a hung server.
            if (active.incrementAndGet() > maxConnections) {
                active.decrementAndGet();
                refuse(client);
                continue;
            }

            open.add(client);
            try {
                connections.execute(() -> serve(client));
            } catch (RuntimeException e) {
                // The pool is shutting down, so nobody will serve this one.
                open.remove(client);
                active.decrementAndGet();
                closeQuietly(client);
            }
        }
    }

    private void refuse(Socket client) {
        try (client) {
            Writer out = new OutputStreamWriter(
                    client.getOutputStream(), StandardCharsets.UTF_8);
            out.write("ERR server at capacity\n");
            out.flush();
        } catch (IOException e) {
            // The client gave up first, which is a fine way for this to end.
        }
    }

    /**
     * Serves one connection to its end.
     *
     * <p>The session is created and closed in the same try-with-resources, so
     * every way out of this method — a clean quit, end of stream, a broken
     * pipe, an unexpected runtime failure, the socket being closed underneath
     * it by shutdown — releases the snapshot on the way past.
     */
    private void serve(Socket client) {
        try (Socket held = client;
             Session session = new Session(engine);
             BufferedReader in = new BufferedReader(new InputStreamReader(
                     held.getInputStream(), StandardCharsets.UTF_8));
             BufferedWriter out = new BufferedWriter(new OutputStreamWriter(
                     held.getOutputStream(), StandardCharsets.UTF_8))) {

            while (!closed) {
                String line;
                try {
                    line = readLine(in);
                } catch (LineTooLong e) {
                    write(out, "ERR line too long");
                    return;
                }
                if (line == null) {
                    return;   // the client went away without saying so
                }

                Session.Response response = session.execute(line);
                for (String reply : response.lines()) {
                    out.write(reply);
                    out.write('\n');
                }
                out.flush();

                if (response.endConnection()) {
                    return;
                }
            }
        } catch (IOException e) {
            // A connection that broke mid-conversation. Normal, and the only
            // thing that matters is that the session closes, which it has.
        } finally {
            open.remove(client);
            active.decrementAndGet();
        }
    }

    private static void write(Writer out, String line) throws IOException {
        out.write(line);
        out.write('\n');
        out.flush();
    }

    /**
     * Reads one line, refusing one that grows past {@link #MAX_LINE_LENGTH}.
     *
     * <p>Hand-rolled because {@code BufferedReader.readLine} has no bound, so
     * a client that never sends a newline would have the server buffer until
     * it ran out of memory.
     */
    private static String readLine(BufferedReader in) throws IOException, LineTooLong {
        StringBuilder line = new StringBuilder();
        int c;
        while ((c = in.read()) != -1) {
            if (c == '\n') {
                // Tolerate CRLF, which is what a telnet client sends.
                if (!line.isEmpty() && line.charAt(line.length() - 1) == '\r') {
                    line.setLength(line.length() - 1);
                }
                return line.toString();
            }
            if (line.length() >= MAX_LINE_LENGTH) {
                throw new LineTooLong();
            }
            line.append((char) c);
        }
        // End of stream. A trailing fragment with no newline is not a command.
        return null;
    }

    /** Thrown internally when a client's line outgrows the limit. */
    private static final class LineTooLong extends Exception {
    }

    /**
     * Stops serving and closes every connection still open, which releases
     * whatever snapshots they were holding.
     *
     * <p>Sockets are closed directly because the threads serving them are
     * blocked reading and will not notice a flag. Closing the socket is what
     * wakes them, and each then runs its own cleanup on the way out.
     */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        closeQuietly(socket);

        for (Socket client : open) {
            closeQuietly(client);
        }

        connections.shutdown();
        try {
            if (!connections.awaitTermination(5, TimeUnit.SECONDS)) {
                connections.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            connections.shutdownNow();
        }
    }

    private static void closeQuietly(Closeable target) {
        try {
            target.close();
        } catch (IOException e) {
            // Already broken, which is the state we were aiming for.
        }
    }

    /**
     * Runs a store on a port: {@code StoreServer <data-dir> [port]}.
     *
     * <p>Speaks a line protocol, so {@code telnet} or {@code nc} is enough to
     * drive it by hand.
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length < 1) {
            System.err.println("usage: StoreServer <data-dir> [port]");
            System.exit(2);
        }
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 7070;

        LSMStoreEngine engine = new LSMStoreEngine(Path.of(args[0]));
        StoreServer server = new StoreServer(engine, port);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.close();
            engine.close();
        }));
        System.out.println("listening on 127.0.0.1:" + server.port());
        server.awaitShutdown();
    }
}
