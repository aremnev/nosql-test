package net.thumbtack.research.nosql.utils;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * User: vkornev
 * Date: 13.08.13
 * Time: 17:10
 */
public final class StringSerializer {

    private static final String UTF_8 = "UTF-8";
    private static final StringSerializer instance = new StringSerializer();
    private static final Charset charset = Charset.forName(UTF_8);

    private StringSerializer() {

    }

    public static StringSerializer get() {
        return instance;
    }

    public ByteBuffer toByteBuffer(String obj) {
        if (obj == null) {
            return null;
        }
        return ByteBuffer.wrap(obj.getBytes(charset));
    }

    public String fromByteBuffer(ByteBuffer byteBuffer) {
        if (byteBuffer == null) {
            return null;
        }
        return charset.decode(byteBuffer).toString();
    }
}
