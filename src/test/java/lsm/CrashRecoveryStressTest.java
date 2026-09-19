package lsm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class CrashRecoveryStressTest {

    @TempDir
    Path tempDir;

    @Test
    void multipleCrashRecoverCycles() {
        // Cycle 1: write and "crash"
        LSMStoreEngine engine1 = new LSMStoreEngine(tempDir);
        engine1.put("a", "1");
        engine1.put("b", "2");
        // crash — no close()

        // Cycle 2: recover, write more, "crash" again
        LSMStoreEngine engine2 = new LSMStoreEngine(tempDir);
        assertEquals(Optional.of("1"), engine2.get("a"));
        assertEquals(Optional.of("2"), engine2.get("b"));
        engine2.put("c", "3");
        engine2.put("a", "updated");
        // crash again

        // Cycle 3: verify everything survived both crashes
        LSMStoreEngine engine3 = new LSMStoreEngine(tempDir);
        assertEquals(Optional.of("updated"), engine3.get("a"));
        assertEquals(Optional.of("2"), engine3.get("b"));
        assertEquals(Optional.of("3"), engine3.get("c"));
        engine3.close();
    }

    @Test
    void deletePersistedAcrossCrash() {
        LSMStoreEngine engine1 = new LSMStoreEngine(tempDir);
        engine1.put("keep", "yes");
        engine1.put("remove", "no");
        engine1.delete("remove");
        // crash

        LSMStoreEngine engine2 = new LSMStoreEngine(tempDir);
        assertEquals(Optional.of("yes"), engine2.get("keep"));
        assertEquals(Optional.empty(), engine2.get("remove"));

        // The key should STAY deleted even after another crash
        // crash

        LSMStoreEngine engine3 = new LSMStoreEngine(tempDir);
        assertEquals(Optional.empty(), engine3.get("remove"));
        engine3.close();
    }

    @Test
    void putAfterDeleteSurvivesCrash() {
        LSMStoreEngine engine1 = new LSMStoreEngine(tempDir);
        engine1.put("k", "original");
        engine1.delete("k");
        engine1.put("k", "resurrected");
        // crash

        LSMStoreEngine engine2 = new LSMStoreEngine(tempDir);
        assertEquals(Optional.of("resurrected"), engine2.get("k"));
        engine2.close();
    }

    @Test
    void emptyStringsSurviveCrash() {
        LSMStoreEngine engine1 = new LSMStoreEngine(tempDir);
        engine1.put("", "empty-key");
        engine1.put("empty-val", "");
        // crash

        LSMStoreEngine engine2 = new LSMStoreEngine(tempDir);
        assertEquals(Optional.of("empty-key"), engine2.get(""));
        assertEquals(Optional.of(""), engine2.get("empty-val"));
        engine2.close();
    }

    @Test
    void largeDataSurvivesCrash() {
        LSMStoreEngine engine1 = new LSMStoreEngine(tempDir);
        String bigVal = "x".repeat(50_000);
        engine1.put("big", bigVal);
        // crash

        LSMStoreEngine engine2 = new LSMStoreEngine(tempDir);
        assertEquals(Optional.of(bigVal), engine2.get("big"));
        engine2.close();
    }

    @Test
    void randomizedCrashRecovery() {
        Map<String, String> reference = new HashMap<>();
        Random rng = new Random(99);

        // Write a bunch of random operations
        LSMStoreEngine engine1 = new LSMStoreEngine(tempDir);
        for (int i = 0; i < 2000; i++) {
            String key = "key-" + rng.nextInt(300);
            if (rng.nextInt(4) == 0) {
                engine1.delete(key);
                reference.remove(key);
            } else {
                String val = "val-" + rng.nextInt(50000);
                engine1.put(key, val);
                reference.put(key, val);
            }
        }
        // crash

        // Verify every single key matches the reference
        LSMStoreEngine engine2 = new LSMStoreEngine(tempDir);
        for (Map.Entry<String, String> e : reference.entrySet()) {
            assertEquals(Optional.of(e.getValue()), engine2.get(e.getKey()),
                    "Mismatch for key: " + e.getKey());
        }

        // Verify deleted keys are gone
        for (int i = 0; i < 300; i++) {
            String key = "key-" + i;
            if (!reference.containsKey(key)) {
                assertEquals(Optional.empty(), engine2.get(key),
                        "Key should be deleted: " + key);
            }
        }
        engine2.close();
    }

    @Test
    void cleanShutdownThenReopenWorks() {
        LSMStoreEngine engine1 = new LSMStoreEngine(tempDir);
        engine1.put("a", "1");
        engine1.close(); // clean shutdown

        LSMStoreEngine engine2 = new LSMStoreEngine(tempDir);
        assertEquals(Optional.of("1"), engine2.get("a"));
        engine2.close();
    }

    @Test
    void reopenEmptyDatabase() {
        LSMStoreEngine engine1 = new LSMStoreEngine(tempDir);
        engine1.close();

        LSMStoreEngine engine2 = new LSMStoreEngine(tempDir);
        assertEquals(Optional.empty(), engine2.get("anything"));
        engine2.close();
    }

    @Test
    void unicodeSurvivesCrash() {
        LSMStoreEngine engine1 = new LSMStoreEngine(tempDir);
        engine1.put("☃", "snowman");
        engine1.put("café", "coffee");
        engine1.put("🚀", "rocket");
        // crash

        LSMStoreEngine engine2 = new LSMStoreEngine(tempDir);
        assertEquals(Optional.of("snowman"), engine2.get("☃"));
        assertEquals(Optional.of("coffee"), engine2.get("café"));
        assertEquals(Optional.of("rocket"), engine2.get("🚀"));
        engine2.close();
    }
}
