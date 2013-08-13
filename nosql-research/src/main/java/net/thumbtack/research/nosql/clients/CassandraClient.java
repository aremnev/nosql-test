package net.thumbtack.research.nosql.clients;

import net.thumbtack.research.nosql.Configurator;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.thrift.*;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CassandraClient implements Database {
    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_PORT = 9160;
    private static final String KEY_SPACE_PROPERTY = "keySpace";
    private static final String DEFAULT_KEY_SPACE = "key_space";
    private static final String COLUMN_FAMILY_PROPERTY = "columnFamily";
    private static final String DEFAULT_COLUMN_FAMILY = "column_family";
    private static final String COLUMN_NAME_PROPERTY = "columnName";
    private static final String DEFAULT_COLUMN_NAME = "column_name";

    private static final String READ_CONSISTENCY_LEVEL_PROPERTY = "readConsistencyLevel";
    private static final String WRITE_CONSISTENCY_LEVEL_PROPERTY = "writeConsistencyLevel";

    private static final String REPLICATION_FACTOR_PROPERTY = "replicationFactor";
    private static final String DEFAULT_REPLICATION_FACTOR = "1";

    private static final Logger log = LoggerFactory.getLogger(CassandraClient.class);

    private ConsistencyLevel readConsistencyLevel;
    private ConsistencyLevel writeConsistencyLevel;

    private String columnFamily;
    private String columnName;

    private Cassandra.Client client;
    private TTransport transport;

    private final Map<String, List<Mutation>> mutationMap = new HashMap<String, List<Mutation>>(1);
    private ColumnOrSuperColumn superColumn;

    CassandraClient() {
    }

    @Override
    public void close() throws Exception {
        transport.close();
    }

    @Override
    public void init(Configurator configurator) {
        if(log.isDebugEnabled()) {
            log.debug("Configurator: " + configurator);
        }
        try {
            log.info("Client initialization: " +
                    configurator.getHost(DEFAULT_HOST) +
                    ":" +
                    configurator.getPort(DEFAULT_PORT));
            transport = new TFramedTransport(new TSocket(
                    configurator.getHost(DEFAULT_HOST),
                    configurator.getPort(DEFAULT_PORT))
            );
            client = new Cassandra.Client(new TBinaryProtocol(transport));
            transport.open();

            String keyspace = configurator.getString(KEY_SPACE_PROPERTY, DEFAULT_KEY_SPACE);
            String replicationFactor = configurator.getString(REPLICATION_FACTOR_PROPERTY, DEFAULT_REPLICATION_FACTOR);
            columnFamily = configurator.getString(COLUMN_FAMILY_PROPERTY, DEFAULT_COLUMN_FAMILY);
            columnName = configurator.getString(COLUMN_NAME_PROPERTY, DEFAULT_COLUMN_NAME);

            KsDef ksDef;
            try {
                ksDef = client.describe_keyspace(keyspace);
                ksDef.strategy_options.put("replication_factor", replicationFactor);
                ksDef.cf_defs.clear();
                client.system_update_keyspace(ksDef);
            }
            catch (NotFoundException e) {
                List<CfDef> cfDefList = new ArrayList<>();
                CfDef cfDef = new CfDef(keyspace, columnFamily);
                cfDefList.add(cfDef);
                ksDef = new KsDef(keyspace, SimpleStrategy.class.getName(), cfDefList);
                Map<String, String> strategyOptions = new HashMap<>();
                strategyOptions.put("replication_factor", replicationFactor);
                ksDef.setStrategy_options(strategyOptions);
                client.system_add_keyspace(ksDef);
            }

            client.set_keyspace(keyspace);

            readConsistencyLevel = getConsistencyLevel(
                    configurator,
                    READ_CONSISTENCY_LEVEL_PROPERTY,
                    ConsistencyLevel.ONE
            );
            writeConsistencyLevel = getConsistencyLevel(
                    configurator,
                    WRITE_CONSISTENCY_LEVEL_PROPERTY,
                    ConsistencyLevel.ONE
            );
            columnFamily = configurator.getString(COLUMN_FAMILY_PROPERTY, DEFAULT_COLUMN_FAMILY);
            columnName = configurator.getString(COLUMN_NAME_PROPERTY, DEFAULT_COLUMN_NAME);

            client.truncate(columnFamily);
        } catch (TException e) {
            e.printStackTrace();
            log.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(String key, ByteBuffer value) {

        Map<ByteBuffer, Map<String, List<Mutation>>> record = new HashMap<>(1);

        ByteBuffer wrappedKey;

        ColumnOrSuperColumn superColumn = getSuperColumn();
        Column col = superColumn.column;
        try {
            wrappedKey = ByteBuffer.wrap(key.getBytes("UTF-8"));
            col.setName(ByteBuffer.wrap(columnName.getBytes("UTF-8")));
            col.setValue(value);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            log.error(e.getMessage());
            throw new RuntimeException(e);
        }
        col.setTimestamp(System.currentTimeMillis());

        record.put(wrappedKey, mutationMap);
        for (int i=0; i<10; i++) {
            try {
                client.batch_mutate(record, writeConsistencyLevel);
            } catch (TTransportException e) {
                e.printStackTrace();
                log.error(e.getMessage() + " TTransportException.Type: " + e.getType());
                continue;
            } catch (TException e) {
                e.printStackTrace();
                log.error(e.getMessage());
                continue;
            }
            if(log.isDebugEnabled()) {
                log.debug("Written key:" + key + " value: " + value);
            }
            return;
        }
    }

    @Override
    public ByteBuffer read(String key) {
        SlicePredicate predicate;
        ByteBuffer result = null;
        ColumnParent parent = new ColumnParent(columnFamily);

        try {
            ByteBuffer cn = ByteBuffer.wrap(columnName.getBytes("UTF-8"));
            predicate = new SlicePredicate().setSlice_range(
                    new SliceRange(cn, cn, false, 1)
            );
            List<ColumnOrSuperColumn> results = client.get_slice(
                    ByteBuffer.wrap(key.getBytes("UTF-8")),
                    parent,
                    predicate,
                    readConsistencyLevel
            );
            for (ColumnOrSuperColumn column : results) {
                result = column.column.value;

            }
            if(log.isDebugEnabled()) {
                log.debug("Read key:" + key + " value: " + result);
            }
            return result;
        } catch (TException e) {
            e.printStackTrace();
            log.error(e.getMessage());
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            log.error(e.getMessage());
        }
        return null;
    }

    private ConsistencyLevel getConsistencyLevel(Configurator configurator, String name, ConsistencyLevel def) {
        String levelName = configurator.getString(name, null);
        if (levelName != null && ConsistencyLevel.valueOf(levelName) != null) {
            return ConsistencyLevel.valueOf(levelName);
        }
        return def;
    }

    private void initMutationMap() {
        superColumn = new ColumnOrSuperColumn();
        superColumn.setColumn(new Column());
        List<Mutation> mutations = new ArrayList<Mutation>(1);
        mutations.add(new Mutation().setColumn_or_supercolumn(superColumn));
        mutationMap.put(columnFamily, mutations);
    }

    private ColumnOrSuperColumn getSuperColumn() {
        if(superColumn == null) {
            initMutationMap();
        }
        return superColumn;
    }
}
