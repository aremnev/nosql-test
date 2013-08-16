package net.thumbtack.research.nosql.clients;

import net.thumbtack.research.nosql.Configurator;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;

/**
 * User: vkornev
 * Date: 12.08.13
 * Time: 18:06
 *
 * General interface for base method of NoSQL data bases
 */
public interface Client {
    void init(Configurator configurator) throws ClientException;
    void write(String key, Map<String, ByteBuffer> value) throws ClientException;
    Map<String, ByteBuffer> read(String key, Set<String> columnNames) throws ClientException;
    void close() throws Exception;
}
