package net.thumbtack.research.nosql;

import java.io.Closeable;

/**
 * User: vkornev
 * Date: 12.08.13
 * Time: 18:06
 *
 * General interface for base method of NoSQL data bases
 */
public interface Database<T> extends AutoCloseable {
    void init(Configurator configurator);
    void write(String key, T value);
    T read(String key);
}
