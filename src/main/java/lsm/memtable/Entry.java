package lsm.memtable;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

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
        return value.map(v -> v.getBytes(StandardCharsets.UTF_8).length).orElse(0);
    }
}
