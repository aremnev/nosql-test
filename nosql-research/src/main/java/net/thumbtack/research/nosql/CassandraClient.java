package net.thumbtack.research.nosql;

import org.apache.cassandra.thrift.AuthenticationRequest;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class CassandraClient implements Database<Map<String, String>> {
    private static final Logger log = LoggerFactory.getLogger(CassandraClient.class);

    private Cassandra.Client client;


    CassandraClient() {

    }

    @Override
    public void init(Configurator configurator) {
        try {
            TTransport transport = new TFramedTransport(new TSocket("localhost", 3333));
            client = new Cassandra.Client(new TBinaryProtocol(transport));
                transport.open();

            Map<String, String> cred = new HashMap<String, String>(2);
            cred.put("username", "username");
            cred.put("password", "password");
            AuthenticationRequest req = new AuthenticationRequest(cred);
            client.login(req);
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
        // TODO : impliment read from database
        return null;
    }
}
