package net.thumbtack.research.nosql.clients;

import java.util.HashMap;
import java.util.Map;

/**
 * User: vkornev
 * Date: 12.08.13
 * Time: 19:23
 */
public final class DatabasePool {
    public static final String DB_CASSANDRA = "cassandra";
    public static final String DB_AEROSPIKE = "aerospike";

    private static final DatabasePool instance = new DatabasePool();
    private final Map<String, Class<? extends Database>> databasePool;

    private DatabasePool() {
        databasePool = new HashMap<>();
        databasePool.put(DB_CASSANDRA, CassandraClient.class);
        databasePool.put(DB_AEROSPIKE, AerospikeClientDB.class);
    }

    public static Database get(String databaseName) throws IllegalAccessException, InstantiationException {
        return instance.databasePool.get(databaseName).newInstance();
    }
}
