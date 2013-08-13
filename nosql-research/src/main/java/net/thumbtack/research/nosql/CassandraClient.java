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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CassandraClient implements Database<Map<String, String>> {
    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_PORT = 3333;
    private static final String KEY_SPACE_PROPERTY = "keySpace";
    private static final String COLUMN_FAMILY_PROPERTY = "columnFamily";
    private static final String DEFAULT_KEY_SPACE = "key_space";
    private static final String DEFAULT_COLUMN_FAMILY = "column_family";

    private static final Logger log = LoggerFactory.getLogger(CassandraClient.class);
    public static final ByteBuffer emptyByteBuffer = ByteBuffer.wrap(new byte[0]);

    private ConsistencyLevel readConsistencyLevel = ConsistencyLevel.ONE;

    //TODO: Figure out what is this
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
//            Map<String, String> cred = new HashMap<String, String>(2);
//            cred.put("username", "username");
//            cred.put("password", "password");
//            AuthenticationRequest req = new AuthenticationRequest(cred);
            client.set_keyspace(configurator.getString(KEY_SPACE_PROPERTY, DEFAULT_KEY_SPACE));
            columnFamily = configurator.getString(COLUMN_FAMILY_PROPERTY, DEFAULT_COLUMN_FAMILY);
        } catch (TException e) {
            e.printStackTrace();
            log.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    @Override
    public void Write(String key, Map<String, String> value) {
        // TODO : implement write to database
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
}
