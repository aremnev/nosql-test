package net.thumbtack.research.nosql.utils;

import java.nio.ByteBuffer;

/**
 * User: vkornev
 * Date: 13.08.13
 * Time: 17:12
 */

public final class LongSerializer {

    private static final LongSerializer instance = new LongSerializer();

    private LongSerializer() {}

    public static LongSerializer get() {
        return instance;
    }

    public ByteBuffer toByteBuffer(Long obj) {
        if (obj == null) {
            return null;
        }
        return ByteBuffer.allocate(8).putLong(0, obj);
    }

    public Long fromByteBuffer(ByteBuffer byteBuffer) {
        if ((byteBuffer == null) || (byteBuffer.remaining() < 8)) {
            return null;
        }
        long l = byteBuffer.getLong();
        return l;
    }
}