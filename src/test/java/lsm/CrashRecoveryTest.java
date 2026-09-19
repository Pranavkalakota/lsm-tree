package lsm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class CrashRecoveryTest {

    @TempDir
    Path tempDir;

    @Test
    void dataRecoveredAfterCleanShutdown() {
        LSMStoreEngine engine1 = new LSMStoreEngine(tempDir);
        engine1.put("name", "alice");
        engine1.put("age", "20");
        engine1.close();

        // Reopen from the same directory — WAL should be replayed
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
        // No close() — simulating a crash

        // Reopen — WAL replay should recover all three keys
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
        // Crash — no close()

        LSMStoreEngine engine2 = new LSMStoreEngine(tempDir);
        assertEquals(Optional.empty(), engine2.get("k"));
        engine2.close();
    }

    @Test
    void overwriteRecoveredAfterCrash() {
        LSMStoreEngine engine1 = new LSMStoreEngine(tempDir);
        engine1.put("k", "old");
        engine1.put("k", "new");
        // Crash

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
        // Crash

        LSMStoreEngine engine2 = new LSMStoreEngine(tempDir);
        for (int i = 0; i < 500; i++) {
            assertEquals(Optional.of("val-" + i), engine2.get("key-" + i));
        }
        engine2.close();
    }
}
