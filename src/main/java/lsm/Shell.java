package lsm;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.Scanner;
import java.util.stream.Stream;

/**
 * An interactive prompt for poking at a store by hand.
 *
 * <pre>
 *   mvn compile
 *   java -cp target/classes lsm.Shell ./demo-data
 * </pre>
 *
 * Defaults to a deliberately tiny MemTable so that a handful of writes is
 * enough to trigger a flush and make tables appear on disk while you watch.
 */
public final class Shell {

    public static void main(String[] args) throws IOException {
        Path dir = Path.of(args.length > 0 ? args[0] : "./demo-data");
        long bound = args.length > 1 ? Long.parseLong(args[1]) : 32;

        System.out.println("store    : " + dir.toAbsolutePath());
        System.out.println("memtable : flushes past " + bound + " bytes");
        System.out.println("type 'help' for commands\n");

        LSMStoreEngine engine = new LSMStoreEngine(dir, bound);
        Scanner in = new Scanner(System.in);

        try {
            while (true) {
                System.out.print("> ");
                if (!in.hasNextLine()) {
                    break;
                }
                String[] parts = in.nextLine().trim().split("\\s+", 3);
                String command = parts[0].toLowerCase();

                if (command.isEmpty()) {
                    continue;
                }
                if (command.equals("exit") || command.equals("quit")) {
                    break;
                }
                run(engine, dir, command, parts);
            }
        } finally {
            engine.close();
            System.out.println("closed. data is still on disk; run again to reopen it.");
        }
    }

    private static void run(LSMStoreEngine engine, Path dir, String command, String[] parts)
            throws IOException {
        switch (command) {
            case "put" -> {
                if (parts.length < 3) {
                    System.out.println("usage: put <key> <value>");
                    return;
                }
                long before = tables(dir).size();
                engine.put(parts[1], parts[2]);
                long after = tables(dir).size();
                System.out.println("ok" + (after > before ? "  (memtable filled up, flushed to disk)" : ""));
            }
            case "get" -> {
                if (parts.length < 2) {
                    System.out.println("usage: get <key>");
                    return;
                }
                Optional<String> found = engine.get(parts[1]);
                System.out.println(found.map(v -> "= " + v).orElse("(not found)"));
            }
            case "del", "delete" -> {
                if (parts.length < 2) {
                    System.out.println("usage: del <key>");
                    return;
                }
                engine.delete(parts[1]);
                System.out.println("ok  (wrote a tombstone; the old value is still on disk until compaction)");
            }
            case "files" -> {
                List<Path> tables = tables(dir);
                Path wal = dir.resolve("wal.log");
                System.out.printf("%-22s %6d bytes   <- writes not yet flushed%n",
                        "wal.log", Files.exists(wal) ? Files.size(wal) : 0);
                if (tables.isEmpty()) {
                    System.out.println("(no tables yet -- write more to trigger a flush)");
                }
                for (Path table : tables) {
                    System.out.printf("%-22s %6d bytes%n",
                            table.getFileName(), Files.size(table));
                }
            }
            case "help" -> System.out.println("""
                    put <key> <value>   store a value
                    get <key>           look one up
                    del <key>           delete one
                    files               show what is on disk right now
                    exit                close cleanly""");
            default -> System.out.println("unknown command: " + command + " (try 'help')");
        }
    }

    private static List<Path> tables(Path dir) throws IOException {
        if (!Files.isDirectory(dir)) {
            return List.of();
        }
        try (Stream<Path> files = Files.list(dir)) {
            return files.filter(p -> p.toString().endsWith(".sst")).sorted().toList();
        }
    }
}
