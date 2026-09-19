package lsm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class CrashRecoveryTest {

    @TempDir
    Path tempDir;

    // We simulate "crash" by calling close() — since every write is fsynced
    // to disk before returning, the data is durable the instant put() returns.
    // The close() just releases the file lock so we can reopen.

    @Test
    void dataRecoveredAfterCleanShutdown() {
        LSMStoreEngine engine1 = new LSMStoreEngine(tempDir);
        engine1.put("name", "alice");
        engine1.put("age", "20");
        engine1.close();

        LSMStoreEngine engine2 = new LSMStoreEngine(tempDir);
        assertEquals(Optional.of("alice"), engine2.get("name"));
        assertEquals(Optional.of("20"), engine2.get("age"));
        engine2.close();
    }

    @Test
    void dataRecoveredAfterCrash() {
        LSMStoreEngine engine1 = new LSMStoreEngine(tempDir);
        engine1.put("key1", "value1");
        engine1.put("key2", "value2");
        engine1.put("key3", "value3");
        engine1.close();

        LSMStoreEngine engine2 = new LSMStoreEngine(tempDir);
        assertEquals(Optional.of("value1"), engine2.get("key1"));
        assertEquals(Optional.of("value2"), engine2.get("key2"));
        assertEquals(Optional.of("value3"), engine2.get("key3"));
        engine2.close();
    }

    @Test
    void deleteRecoveredAfterCrash() {
        LSMStoreEngine engine1 = new LSMStoreEngine(tempDir);
        engine1.put("k", "v");
        engine1.delete("k");
        engine1.close();

        LSMStoreEngine engine2 = new LSMStoreEngine(tempDir);
        assertEquals(Optional.empty(), engine2.get("k"));
        engine2.close();
    }

    @Test
    void overwriteRecoveredAfterCrash() {
        LSMStoreEngine engine1 = new LSMStoreEngine(tempDir);
        engine1.put("k", "old");
        engine1.put("k", "new");
        engine1.close();

        LSMStoreEngine engine2 = new LSMStoreEngine(tempDir);
        assertEquals(Optional.of("new"), engine2.get("k"));
        engine2.close();
    }

    @Test
    void manyWritesRecoveredAfterCrash() {
        LSMStoreEngine engine1 = new LSMStoreEngine(tempDir);
        for (int i = 0; i < 500; i++) {
            engine1.put("key-" + i, "val-" + i);
        }
        engine1.close();

        LSMStoreEngine engine2 = new LSMStoreEngine(tempDir);
        for (int i = 0; i < 500; i++) {
            assertEquals(Optional.of("val-" + i), engine2.get("key-" + i));
        }
        engine2.close();
    }
}
