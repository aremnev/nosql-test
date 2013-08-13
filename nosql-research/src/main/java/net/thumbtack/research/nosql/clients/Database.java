package net.thumbtack.research.nosql.clients;

import net.thumbtack.research.nosql.Configurator;

/**
 * User: vkornev
 * Date: 12.08.13
 * Time: 18:06
 *
 * General interface for base method of NoSQL data bases
 */
public interface Database<T> {
    void init(Configurator configurator);
    void Write(String key, T value);
    T read(String key);
}
