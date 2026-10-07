package lsm.sstable;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import java.io.IOException;
import java.util.Arrays;

/**
 * How a block's bytes are stored.
 *
 * <p>Recorded per block rather than per file, so a block that does not
 * compress usefully is written raw and a reader handles either without being
 * told which to expect. That also means changing the default costs nothing:
 * existing blocks keep describing themselves.
 */
public enum CompressionType {

    NONE(0) {
        @Override
        byte[] compress(byte[] data) {
            return data;
        }

        @Override
        byte[] decompress(byte[] source, int offset, int length, int originalLength) {
            return Arrays.copyOfRange(source, offset, offset + length);
        }
    },

    LZ4(1) {
        @Override
        byte[] compress(byte[] data) {
            LZ4Compressor compressor = LZ4Factory.fastestInstance().fastCompressor();
            byte[] out = new byte[compressor.maxCompressedLength(data.length)];
            int written = compressor.compress(data, 0, data.length, out, 0, out.length);
            return Arrays.copyOf(out, written);
        }

        @Override
        byte[] decompress(byte[] source, int offset, int length, int originalLength) {
            LZ4FastDecompressor decompressor = LZ4Factory.fastestInstance().fastDecompressor();
            byte[] out = new byte[originalLength];
            decompressor.decompress(source, offset, out, 0, originalLength);
            return out;
        }
    };

    private final byte id;

    CompressionType(int id) {
        this.id = (byte) id;
    }

    public byte id() {
        return id;
    }

    abstract byte[] compress(byte[] data);

    /** @param originalLength size before compression, which LZ4 needs up front */
    abstract byte[] decompress(byte[] source, int offset, int length, int originalLength);

    static CompressionType byId(byte id) throws IOException {
        for (CompressionType type : values()) {
            if (type.id == id) {
                return type;
            }
        }
        throw new IOException("Unknown block compression id: " + id);
    }
}
