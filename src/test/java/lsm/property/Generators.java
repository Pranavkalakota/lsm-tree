package lsm.property;

import lsm.memtable.Entry;

import java.util.Random;
import java.util.TreeMap;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Seeded input generation for the property tests.
 *
 * <p>Every case is driven by a seed that appears in its display name, so a
 * failure names the exact dataset that produced it and can be replayed by
 * running that one case.
 */
public final class Generators {

    /** Character repertoires, so cases cover more than tidy ASCII identifiers. */
    public enum Alphabet {
        ASCII("abcdefghijklmnopqrstuvwxyz0123456789"),
        PUNCTUATED("ab _-./:;'\"\\|@#$%^&*()[]{}+=<>?!~`"),
        ACCENTED("aeiouàéîõüñçßæøå"),
        CJK("日本語中文한국어"),
        EMOJI("🚀🔥☃🎉🧪🦀"),
        MIXED("ab_日🚀é.9");

        /**
         * Held as code points, not as a string. Indexing a string by UTF-16
         * unit can land on half of a surrogate pair, and an unpaired surrogate
         * is not encodable as UTF-8 -- it becomes a replacement character on
         * the way to disk and the key stops round-tripping. That is a property
         * of the encoding rather than of the store, so the generator avoids
         * producing them; {@code EncodingLimitsTest} covers the behaviour
         * explicitly instead.
         */
        private final int[] codePoints;

        Alphabet(String glyphs) {
            this.codePoints = glyphs.codePoints().toArray();
        }

        String pick(Random random, int length) {
            StringBuilder out = new StringBuilder();
            for (int i = 0; i < length; i++) {
                out.appendCodePoint(codePoints[random.nextInt(codePoints.length)]);
            }
            return out.toString();
        }
    }

    /** One generated dataset: how big, what it is made of, how much is deleted. */
    public record Shape(long seed, Alphabet alphabet, int entries, int maxKeyLength,
                        int maxValueLength, double tombstoneRatio) {

        @Override
        public String toString() {
            return String.format("seed=%d %s n=%d key<=%d val<=%d tomb=%.0f%%",
                    seed, alphabet, entries, maxKeyLength, maxValueLength,
                    tombstoneRatio * 100);
        }

        /** Builds the dataset. Sorted, so it can go straight into a table. */
        public TreeMap<String, Entry> build() {
            Random random = new Random(seed);
            TreeMap<String, Entry> entries = new TreeMap<>();
            // Keys can collide, so this is an upper bound rather than a count.
            for (int i = 0; i < this.entries; i++) {
                String key = alphabet.pick(random, 1 + random.nextInt(maxKeyLength));
                entries.put(key, random.nextDouble() < tombstoneRatio
                        ? Entry.tombstone()
                        : Entry.put(alphabet.pick(random, random.nextInt(maxValueLength + 1))));
            }
            return entries;
        }

        /** Keys that are absent but drawn from the same alphabet. */
        public Stream<String> absentKeys(TreeMap<String, Entry> present, int count) {
            Random random = new Random(seed * 31 + 7);
            return IntStream.range(0, count)
                    .mapToObj(i -> alphabet.pick(random, 1 + random.nextInt(maxKeyLength)))
                    .filter(key -> !present.containsKey(key));
        }
    }

    /** A spread of shapes: every alphabet, several sizes, several delete ratios. */
    public static Stream<Shape> shapes(int perAlphabet) {
        return Stream.of(Alphabet.values()).flatMap(alphabet ->
                IntStream.range(0, perAlphabet).mapToObj(i -> {
                    Random random = new Random(alphabet.ordinal() * 1000L + i);
                    return new Shape(
                            alphabet.ordinal() * 1000L + i,
                            alphabet,
                            1 + random.nextInt(400),
                            1 + random.nextInt(40),
                            random.nextInt(120),
                            random.nextDouble() * 0.5);
                }));
    }

    private Generators() {
    }
}
