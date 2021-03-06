package net.thumbtack.research.nosql.clients;

import net.thumbtack.research.nosql.Configurator;
import net.thumbtack.research.nosql.utils.StringSerializer;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
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

import java.nio.ByteBuffer;
import java.util.*;

public final class CassandraClient implements Client {
    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_PORT = 9160;
    private static final int DEFAULT_RETRIES = 1;
    private static final String KEY_SPACE_PROPERTY = "cassandra.keySpace";
    private static final String DEFAULT_KEY_SPACE = "key_space";
    private static final String COLUMN_FAMILY_PROPERTY = "cassandra.columnFamily";
    private static final String DEFAULT_COLUMN_FAMILY = "column_family";

    private static final String READ_CONSISTENCY_LEVEL_PROPERTY = "cassandra.readConsistencyLevel";
    private static final String WRITE_CONSISTENCY_LEVEL_PROPERTY = "cassandra.writeConsistencyLevel";

    private static final String REPLICATION_FACTOR_PROPERTY = "cassandra.replicationFactor";
    private static final String DEFAULT_REPLICATION_FACTOR = "1";
    private static final String REPLICATION_STRATEGY_PROPERTY = "cassandra.replicationStrategy";
    private static final String DEFAULT_REPLICATION_STRATEGY = "SimpleStrategy";
    private static final String STRATEGY_OPTIONS_PROPERTY = "cassandra.strategyOptions";
    private static final String DEFAULT_STRATEGY_OPTIONS = "{}";

    private static final String STRATEGY_REPLICATION_FACTOR_PROPERTY = "replication_factor";

    private static final Logger log = LoggerFactory.getLogger(CassandraClient.class);

    private static final StringSerializer ss = StringSerializer.get();

    private ConsistencyLevel readConsistencyLevel;
    private ConsistencyLevel writeConsistencyLevel;

    private String keySpace;
    private String columnFamily;
    private String replicationFactor;
    private String replicationStrategy;
    private String replicationOptions;

    private int retries;

    private Cassandra.Client client;
    private TTransport transport;

    private Map<String, ColumnOrSuperColumn> superColumns;

    private boolean slow;

    CassandraClient() {
    }

    @Override
    public void close() throws Exception {
        transport.close();
    }

    @Override
    public void init(Configurator configurator) throws ClientException {
        if(log.isDebugEnabled()) {
            log.debug("Configurator: " + configurator);
        }
        try {
            String host = configurator.getNextDbHost(DEFAULT_HOST);
            slow = configurator.isSlow(host);
            int port = configurator.getDbPort(DEFAULT_PORT);
            retries = configurator.getDbRetries(DEFAULT_RETRIES);
            log.debug("Client initialization: " + host + ":" + port);
            transport = new TFramedTransport(new TSocket(host, port));
            client = new Cassandra.Client(new TBinaryProtocol(transport));
            transport.open();

            keySpace = configurator.getString(KEY_SPACE_PROPERTY, DEFAULT_KEY_SPACE);
            replicationFactor = configurator.getString(REPLICATION_FACTOR_PROPERTY, DEFAULT_REPLICATION_FACTOR);
            replicationStrategy = configurator.getString(REPLICATION_STRATEGY_PROPERTY, DEFAULT_REPLICATION_STRATEGY);
            replicationOptions = configurator.getString(STRATEGY_OPTIONS_PROPERTY, DEFAULT_STRATEGY_OPTIONS);
            columnFamily = configurator.getString(COLUMN_FAMILY_PROPERTY, DEFAULT_COLUMN_FAMILY);

            setReplicationFactor();

            client.set_keyspace(keySpace);

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

            client.truncate(columnFamily);

            superColumns = new HashMap<>();
        } catch (TException e) {
            e.printStackTrace();
            log.error(e.getMessage());
            throw new ClientException(e);
        }
    }

    @Override
    public void write(String key, Map<String, ByteBuffer> data) throws ClientException {

        Map<ByteBuffer, Map<String, List<Mutation>>> record = new HashMap<>();
        Map<String, List<Mutation>> mutationMap = new HashMap<>();
        List<Mutation> mutations = new ArrayList<>();

        ByteBuffer wrappedKey = ss.toByteBuffer(key);

        for (String columnName: data.keySet()) {
            ColumnOrSuperColumn superColumn = getSuperColumn(columnName);
            superColumn.column.setValue(data.get(columnName));
            superColumn.column.setTimestamp(System.currentTimeMillis());

            mutations.add(new Mutation().setColumn_or_supercolumn(superColumn));
            mutationMap.put(columnFamily, mutations);

            record.put(wrappedKey, mutationMap);
        }


        Exception exception = null;
        for (int i=0; i<retries; i++) {
            try {
                client.batch_mutate(record, writeConsistencyLevel);
            } catch (TTransportException e) {
                e.printStackTrace();
                log.error(e.getMessage() + " TTransportException.Type: " + e.getType());
                exception = e;
                continue;
            } catch (TException e) {
                e.printStackTrace();
                log.error(e.getMessage());
                exception = e;
                continue;
            }
            if(log.isDebugEnabled()) {
                log.debug("Written key:" + ss.fromByteBuffer(wrappedKey) + " data: " + data);
            }
            return;
        }
        throw new ClientException(exception);
    }

    @Override
    public Map<String, ByteBuffer> read(String key, Set<String> columnNames) throws ClientException {
        Map<String, ByteBuffer> result = new HashMap<>();
        ColumnParent parent = new ColumnParent(columnFamily);
        List<ByteBuffer> wrapperColumnNames = new ArrayList<>();
        for (String cn: columnNames) {
            wrapperColumnNames.add(ss.toByteBuffer(cn));
        }
        SlicePredicate slicePredicate = new SlicePredicate()
                .setColumn_names(wrapperColumnNames);
        try {
            List<ColumnOrSuperColumn> readResult = client.get_slice(
                    ss.toByteBuffer(key),
                    parent,
                    slicePredicate,
                    readConsistencyLevel
            );
            for (ColumnOrSuperColumn column: readResult) {
                result.put(ss.fromByteBuffer(column.column.name), column.column.value);
            }
            return result;
        }catch (NotFoundException e) {
            log.debug(e.getMessage());
        } catch (TException e) {
            log.error(e.toString());
            throw new ClientException(e);
        }
        return result;
    }

    private ConsistencyLevel getConsistencyLevel(Configurator configurator, String name, ConsistencyLevel def) {
        String levelName = configurator.getString(name, null);
        if (levelName != null && ConsistencyLevel.valueOf(levelName) != null) {
            return ConsistencyLevel.valueOf(levelName);
        }
        return def;
    }

    private ColumnOrSuperColumn getSuperColumn(String name) {
        if(!superColumns.containsKey(name)) {
            ColumnOrSuperColumn superColumn = new ColumnOrSuperColumn();
            superColumn.setColumn(new Column());
            Column col = superColumn.column;
            col.setName(ss.toByteBuffer(name));
            superColumns.put(name, superColumn);
            return superColumn;
        }
        return superColumns.get(name);
    }

    private void setReplicationFactor() throws TException {
        KsDef ksDef;
        try {
            ksDef = client.describe_keyspace(keySpace);
            ksDef.strategy_options.clear();
            if (SimpleStrategy.class.getSimpleName().equals(replicationStrategy)) {
                ksDef.setStrategy_class(SimpleStrategy.class.getName());
                ksDef.strategy_options.put(STRATEGY_REPLICATION_FACTOR_PROPERTY, replicationFactor);
            }
            else if (NetworkTopologyStrategy.class.getSimpleName().equals(replicationStrategy)) {
                ksDef.setStrategy_class(NetworkTopologyStrategy.class.getName());
                ksDef.strategy_options.putAll(stringToMap(replicationOptions));
            }
            ksDef.cf_defs.clear();
            client.system_update_keyspace(ksDef);
        }
        catch (NotFoundException e) {
            List<CfDef> cfDefList = new ArrayList<>();
            CfDef cfDef = new CfDef(keySpace, columnFamily);
            cfDefList.add(cfDef);
            ksDef = new KsDef(keySpace, "", cfDefList);
            ksDef.setStrategy_options(new HashMap<String, String>());
            if (SimpleStrategy.class.getSimpleName().equals(replicationStrategy)) {
                ksDef.setStrategy_class(SimpleStrategy.class.getName());
                ksDef.strategy_options.put(STRATEGY_REPLICATION_FACTOR_PROPERTY, replicationFactor);
            }
            else if (NetworkTopologyStrategy.class.getSimpleName().equals(replicationStrategy)) {
                ksDef.setStrategy_class(NetworkTopologyStrategy.class.getName());
                ksDef.strategy_options.putAll(stringToMap(replicationOptions));
            }
            client.system_add_keyspace(ksDef);
        }
    }

    @Override
    public boolean isSlow() {
        return slow;
    }

    protected Map<String, String> stringToMap(String str) {
        Map<String, String> result = new HashMap<>();
        str = str.replace("{", "").replace("}", "");
        if (str.trim().length() == 0) {
            return  result;
        }
        for (String item: str.split(";")) {
            String[] array = item.split(":");
            result.put(array[0].trim(), array[1].trim());
        }
        return result;
    }

}
