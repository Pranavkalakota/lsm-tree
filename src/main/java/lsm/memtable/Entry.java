package lsm.memtable;

import java.util.Optional;

/**
 * Wraps a value in the MemTable. A present value means a PUT; an empty value means a DELETE
 * (tombstone). We use this instead of null so we can distinguish "key was deleted" from
 * "key was never written" — both would be null in a plain TreeMap.
 */
public record Entry(Optional<String> value) {

    public static Entry put(String value) {
        return new Entry(Optional.of(value));
    }

    public static Entry tombstone() {
        return new Entry(Optional.empty());
    }

    public boolean isTombstone() {
        return value.isEmpty();
    }

    public int sizeBytes() {
        return value.map(v -> v.getBytes().length).orElse(0);
    }
}
