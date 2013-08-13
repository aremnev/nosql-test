package net.thumbtack.research.nosql;

import org.apache.cassandra.thrift.*;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CassandraClient implements Database<Map<String, String>> {
    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_PORT = 3333;

    private static final String USERNAME_PROPERTY = "username";
    private static final String PASSWORD_PROPERTY = "password";

    private static final String KEY_SPACE_PROPERTY = "keySpace";
    private static final String COLUMN_FAMILY_PROPERTY = "columnFamily";
    private static final String DEFAULT_KEY_SPACE = "key_space";
    private static final String DEFAULT_COLUMN_FAMILY = "column_family";

    private static final String READ_CONSISTENCY_LEVEL_PROPERTY = "readConsistencyLevel";
    private static final String WRITE_CONSISTENCY_LEVEL_PROPERTY = "writeConsistencyLevel";

    private static final Logger log = LoggerFactory.getLogger(CassandraClient.class);
    public static final ByteBuffer emptyByteBuffer = ByteBuffer.wrap(new byte[0]);

    private ConsistencyLevel readConsistencyLevel;
    private ConsistencyLevel writeConsistencyLevel;

    private String columnFamily;

    private Cassandra.Client client;


    CassandraClient() {

    }

    @Override
    public void init(Configurator configurator) {
        try {
            TTransport transport = new TFramedTransport(new TSocket(
                    configurator.getHost(DEFAULT_HOST),
                    configurator.getPort(DEFAULT_PORT))
            );
            client = new Cassandra.Client(new TBinaryProtocol(transport));
            transport.open();
            authenticate(configurator);
            client.set_keyspace(configurator.getString(KEY_SPACE_PROPERTY, DEFAULT_KEY_SPACE));
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
        } catch (TException e) {
            e.printStackTrace();
            log.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(String key, Map<String, String> value) {
        List<Mutation> mutations = new ArrayList<Mutation>(value.size());
        Map<String, List<Mutation>> mutationMap = new HashMap<String, List<Mutation>>(1);
        Map<ByteBuffer, Map<String, List<Mutation>>> record = new HashMap<ByteBuffer, Map<String, List<Mutation>>>(1);

        try {
            ByteBuffer wrappedKey = ByteBuffer.wrap(key.getBytes("UTF-8"));

            Column col;
            ColumnOrSuperColumn superColumn;
            for (Map.Entry<String, String> entry : value.entrySet()) {
                col = new Column();
                col.setName(ByteBuffer.wrap(entry.getKey().getBytes("UTF-8")));
                col.setValue(ByteBuffer.wrap(entry.getValue().getBytes("UTF-8")));
                col.setTimestamp(System.currentTimeMillis());
                superColumn = new ColumnOrSuperColumn();
                superColumn.setColumn(col);
                mutations.add(new Mutation().setColumn_or_supercolumn(superColumn));
            }

            mutationMap.put(columnFamily, mutations);
            record.put(wrappedKey, mutationMap);

            client.batch_mutate(record, writeConsistencyLevel);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            log.error(e.getMessage());
        }
        catch (TException e) {
            e.printStackTrace();
            log.error(e.getMessage());
        }
    }

    @Override
    public Map<String, String> read(String key) {
        SlicePredicate predicate;
        Map<String, String> result = new HashMap<String, String>();
        ColumnParent parent = new ColumnParent(columnFamily);

        predicate = new SlicePredicate().setSlice_range(
            new SliceRange(emptyByteBuffer, emptyByteBuffer, false, 1000000)
        );
        try {
            List<ColumnOrSuperColumn> results = client.get_slice(
                ByteBuffer.wrap(key.getBytes("UTF-8")),
                parent,
                predicate,
                readConsistencyLevel
            );
            for (ColumnOrSuperColumn column : results) {
                ByteBuffer nameBuffer = column.column.name;
                ByteBuffer valueBuffer = column.column.value;
                result.put(
                    new String(
                        nameBuffer.array(),
                        nameBuffer.position() + nameBuffer.arrayOffset(),
                        nameBuffer.remaining()
                    ),
                    new String(
                        valueBuffer.array(),
                        valueBuffer.position() + valueBuffer.arrayOffset(),
                        valueBuffer.remaining()
                    )
                );
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

    private void authenticate(Configurator configurator) throws TException {
        String username = configurator.getString(USERNAME_PROPERTY, null);
        String password = configurator.getString(PASSWORD_PROPERTY, null);
        if(username != null) {
            Map<String, String> cred = new HashMap<String, String>(2);
            cred.put(USERNAME_PROPERTY, username);
            if(password != null) {
                cred.put(PASSWORD_PROPERTY, password);
            }
            AuthenticationRequest req = new AuthenticationRequest(cred);
            client.login(req);
        }
    }

    private ConsistencyLevel getConsistencyLevel(Configurator configurator, String name, ConsistencyLevel def) {
        String levelName = configurator.getString(name, null);
        if(levelName != null && ConsistencyLevel.valueOf(levelName) != null) {
            return ConsistencyLevel.valueOf(levelName);
        }
        return def;
    }
}
