package net.thumbtack.research.nosql.clients;

import java.util.HashMap;
import java.util.Map;

/**
 * User: vkornev
 * Date: 12.08.13
 * Time: 19:23
 */
public class DatabasePool {
    private static final String DB_CASSANDRA = "cassandra";
    private static final String[] databases = {DB_CASSANDRA};

    private static final DatabasePool instance = new DatabasePool();
    private final Map<String, Class<? extends Database>> databasePool;

    @SuppressWarnings("unchecked")
    private DatabasePool() {
        databasePool = new HashMap<String, Class<? extends Database>>();
        databasePool.put(DB_CASSANDRA, CassandraClient.class);
    }

    public static Database get(String databaseName) throws IllegalAccessException, InstantiationException {
        return instance.databasePool.get(databaseName).newInstance();
    }
}
