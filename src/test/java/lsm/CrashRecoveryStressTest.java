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
        LSMStoreEngine engine1 = new LSMStoreEngine(tempDir);
        engine1.put("a", "1");
        engine1.put("b", "2");
        engine1.close();

        LSMStoreEngine engine2 = new LSMStoreEngine(tempDir);
        assertEquals(Optional.of("1"), engine2.get("a"));
        assertEquals(Optional.of("2"), engine2.get("b"));
        engine2.put("c", "3");
        engine2.put("a", "updated");
        engine2.close();

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
        engine1.close();

        LSMStoreEngine engine2 = new LSMStoreEngine(tempDir);
        assertEquals(Optional.of("yes"), engine2.get("keep"));
        assertEquals(Optional.empty(), engine2.get("remove"));
        engine2.close();

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
        engine1.close();

        LSMStoreEngine engine2 = new LSMStoreEngine(tempDir);
        assertEquals(Optional.of("resurrected"), engine2.get("k"));
        engine2.close();
    }

    @Test
    void emptyStringsSurviveCrash() {
        LSMStoreEngine engine1 = new LSMStoreEngine(tempDir);
        engine1.put("", "empty-key");
        engine1.put("empty-val", "");
        engine1.close();

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
        engine1.close();

        LSMStoreEngine engine2 = new LSMStoreEngine(tempDir);
        assertEquals(Optional.of(bigVal), engine2.get("big"));
        engine2.close();
    }

    @Test
    void randomizedCrashRecovery() {
        Map<String, String> reference = new HashMap<>();
        Random rng = new Random(99);

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
        engine1.close();

        LSMStoreEngine engine2 = new LSMStoreEngine(tempDir);
        for (Map.Entry<String, String> e : reference.entrySet()) {
            assertEquals(Optional.of(e.getValue()), engine2.get(e.getKey()),
                    "Mismatch for key: " + e.getKey());
        }
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
        engine1.close();

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
        engine1.close();

        LSMStoreEngine engine2 = new LSMStoreEngine(tempDir);
        assertEquals(Optional.of("snowman"), engine2.get("☃"));
        assertEquals(Optional.of("coffee"), engine2.get("café"));
        assertEquals(Optional.of("rocket"), engine2.get("🚀"));
        engine2.close();
    }

    @Test
    void fiveCrashRecoverCyclesWithInterleavedWrites() {
        Map<String, String> reference = new HashMap<>();

        for (int cycle = 0; cycle < 5; cycle++) {
            LSMStoreEngine engine = new LSMStoreEngine(tempDir);

            for (Map.Entry<String, String> e : reference.entrySet()) {
                assertEquals(Optional.of(e.getValue()), engine.get(e.getKey()),
                        "Cycle " + cycle + " lost key: " + e.getKey());
            }

            for (int i = 0; i < 50; i++) {
                String key = "cycle" + cycle + "-key" + i;
                String val = "cycle" + cycle + "-val" + i;
                engine.put(key, val);
                reference.put(key, val);
            }
            engine.close();
        }

        LSMStoreEngine finalEngine = new LSMStoreEngine(tempDir);
        for (Map.Entry<String, String> e : reference.entrySet()) {
            assertEquals(Optional.of(e.getValue()), finalEngine.get(e.getKey()),
                    "Final check lost key: " + e.getKey());
        }
        assertEquals(250, reference.size());
        finalEngine.close();
    }

    @Test
    void walWriteOnDiskBeforeGetSees() {
        LSMStoreEngine engine = new LSMStoreEngine(tempDir);
        java.io.File walFile = tempDir.resolve("wal.log").toFile();

        long sizeBefore = walFile.length();
        engine.put("k1", "v1");
        long sizeAfterPut = walFile.length();
        assertTrue(sizeAfterPut > sizeBefore,
                "WAL must grow on disk after put");

        assertEquals(Optional.of("v1"), engine.get("k1"));
        engine.close();
    }

    @Test
    void overwriteAcrossMultipleCrashCycles() {
        LSMStoreEngine engine1 = new LSMStoreEngine(tempDir);
        engine1.put("shared", "v1");
        engine1.close();

        LSMStoreEngine engine2 = new LSMStoreEngine(tempDir);
        assertEquals(Optional.of("v1"), engine2.get("shared"));
        engine2.put("shared", "v2");
        engine2.close();

        LSMStoreEngine engine3 = new LSMStoreEngine(tempDir);
        assertEquals(Optional.of("v2"), engine3.get("shared"));
        engine3.put("shared", "v3");
        engine3.close();

        LSMStoreEngine engine4 = new LSMStoreEngine(tempDir);
        assertEquals(Optional.of("v3"), engine4.get("shared"));
        engine4.close();
    }

    @Test
    void deleteAcrossMultipleCrashCycles() {
        LSMStoreEngine engine1 = new LSMStoreEngine(tempDir);
        engine1.put("victim", "alive");
        engine1.close();

        LSMStoreEngine engine2 = new LSMStoreEngine(tempDir);
        assertEquals(Optional.of("alive"), engine2.get("victim"));
        engine2.delete("victim");
        engine2.close();

        LSMStoreEngine engine3 = new LSMStoreEngine(tempDir);
        assertEquals(Optional.empty(), engine3.get("victim"));
        engine3.put("victim", "resurrected");
        engine3.close();

        LSMStoreEngine engine4 = new LSMStoreEngine(tempDir);
        assertEquals(Optional.of("resurrected"), engine4.get("victim"));
        engine4.close();
    }
}
