package net.thumbtack.research.nosql.clients;

import net.thumbtack.research.nosql.Configurator;

import java.nio.ByteBuffer;

/**
 * User: vkornev
 * Date: 12.08.13
 * Time: 18:06
 *
 * General interface for base method of NoSQL data bases
 */
public interface Database {
    void init(Configurator configurator);
    void Write(String key, ByteBuffer value);
    ByteBuffer read(String key);
}
