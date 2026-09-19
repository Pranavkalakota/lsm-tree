package lsm;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class LSMStoreEngineTest {

    private StorageEngine engine;

    @BeforeEach
    void setUp() {
        engine = new LSMStoreEngine();
    }

    @Test
    void putAndGet() {
        engine.put("greeting", "hello");
        assertEquals(Optional.of("hello"), engine.get("greeting"));
    }

    @Test
    void getMissing() {
        assertEquals(Optional.empty(), engine.get("nothing"));
    }

    @Test
    void overwrite() {
        engine.put("k", "v1");
        engine.put("k", "v2");
        assertEquals(Optional.of("v2"), engine.get("k"));
    }

    @Test
    void deleteExisting() {
        engine.put("k", "v");
        engine.delete("k");
        assertEquals(Optional.empty(), engine.get("k"));
    }

    @Test
    void deleteNonExisting() {
        engine.delete("ghost");
        assertEquals(Optional.empty(), engine.get("ghost"));
    }

    @Test
    void putAfterDelete() {
        engine.put("k", "v1");
        engine.delete("k");
        engine.put("k", "v2");
        assertEquals(Optional.of("v2"), engine.get("k"));
    }

    @Test
    void manyKeys() {
        for (int i = 0; i < 1000; i++) {
            engine.put("key-" + i, "val-" + i);
        }
        for (int i = 0; i < 1000; i++) {
            assertEquals(Optional.of("val-" + i), engine.get("key-" + i));
        }
    }

    @Test
    void nullKeyThrows() {
        assertThrows(IllegalArgumentException.class, () -> engine.put(null, "v"));
        assertThrows(IllegalArgumentException.class, () -> engine.get(null));
        assertThrows(IllegalArgumentException.class, () -> engine.delete(null));
    }

    @Test
    void nullValueThrows() {
        assertThrows(IllegalArgumentException.class, () -> engine.put("k", null));
    }
}
