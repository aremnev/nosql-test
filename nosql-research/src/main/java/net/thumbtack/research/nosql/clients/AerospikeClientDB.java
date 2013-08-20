package net.thumbtack.research.nosql.clients;


import com.aerospike.client.*;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.WritePolicy;
import net.thumbtack.research.nosql.Configurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class AerospikeClientDB implements Client {
    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_PORT = 3000;

    private static final int DEFAULT_TIMEOUT = 3000;
    private static final int DEFAULT_RETRIES = 3;
    private static final int DEFAULT_SLEEP_BETWEEN_RETRIES = 10;

    private static final String NAMESPACE_PROPERTY = "aerospike.nameSpace";
    private static final String DEFAULT_NAMESPACE = "test";
    private static final String SET_NAME_PROPERTY = "aerospike.setName";
    private static final String DEFAULT_SET_NAME = "test";

    private static final Logger log = LoggerFactory.getLogger(AerospikeClientDB.class);

    private AerospikeClient client;
    private String nameSpace;
    private String setName;
    private WritePolicy writePolicy;

    private boolean slow;

    @Override
    public void init(Configurator configurator) throws ClientException {
        try {
            String host = configurator.getNextDbHost(DEFAULT_HOST);
            slow = configurator.isSlow(host);
            client = new AerospikeClient(
                    new ClientPolicy(),
                    host,
                    configurator.getDbPort(DEFAULT_PORT)
            );
            nameSpace = configurator.getString(NAMESPACE_PROPERTY, DEFAULT_NAMESPACE);
            setName = configurator.getString(SET_NAME_PROPERTY, DEFAULT_SET_NAME);

            writePolicy = new WritePolicy();
            writePolicy.timeout = DEFAULT_TIMEOUT;
            writePolicy.maxRetries = configurator.getDbRetries(DEFAULT_RETRIES);
            writePolicy.sleepBetweenRetries = DEFAULT_SLEEP_BETWEEN_RETRIES;
        } catch (AerospikeException e) {
            e.printStackTrace();
            log.error(e.getMessage());
            throw new ClientException(e);
        }
    }

    @Override
    public void write(String key, Map<String, ByteBuffer> data) throws ClientException {
        try {
            Bin[] bins = new Bin[data.size()];
            int i = 0;
            for (String name : data.keySet()) {
                bins[i] =  new Bin(name, data.get(name).array());
                i++;
            }
            client.put(writePolicy, createKey(key), bins);
        } catch (AerospikeException e) {
            log.error(e.toString());
            throw new ClientException(e);
        }
    }

    @Override
    public Map<String, ByteBuffer> read(String key, Set<String> columnNames) throws ClientException {
        Map<String, ByteBuffer> result = new HashMap<>();
        try {
            Record record;
            if(columnNames == null || columnNames.isEmpty()) {
               record = client.get(writePolicy, createKey(key));
            } else {
                record = client.get(writePolicy, createKey(key), columnNames.toArray(new String[columnNames.size()]));
            }
            for(String name : record.bins.keySet()) {
                result.put(name, ByteBuffer.wrap((byte[])record.bins.get(name)));
            }
            return result;
        } catch (AerospikeException e) {
            log.error(e.toString());
            throw new ClientException(e);
        } catch (NullPointerException e) {
            log.debug(e.getMessage());
        }
        return result;
    }

    @Override
    public void close() throws Exception {
        client.close();
    }

    private Key createKey(String key) throws AerospikeException {
        return new Key(nameSpace, setName, key);
    }

    @Override
    public boolean isSlow() {
        return slow;
    }
}
