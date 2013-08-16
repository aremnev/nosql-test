package net.thumbtack.research.nosql.clients;

import java.util.HashMap;
import java.util.Map;

/**
 * User: vkornev
 * Date: 12.08.13
 * Time: 19:23
 */
public final class ClientPool {
    public static final String DB_CASSANDRA = "cassandra";
    public static final String DB_AEROSPIKE = "aerospike";

    private static final ClientPool instance = new ClientPool();
    private final Map<String, Class<? extends Client>> clientPool;

    private ClientPool() {
        clientPool = new HashMap<>();
        clientPool.put(DB_CASSANDRA, CassandraClient.class);
        //clientPool.put(DB_AEROSPIKE, AerospikeClientDB.class);
    }

    public static Client get(String databaseName) throws IllegalAccessException, InstantiationException {
        return instance.clientPool.get(databaseName).newInstance();
    }
}
