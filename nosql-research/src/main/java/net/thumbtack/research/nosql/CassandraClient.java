package net.thumbtack.research.nosql;

import org.apache.cassandra.thrift.AuthenticationRequest;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.util.HashMap;
import java.util.Map;

public class CassandraClient {
    private Cassandra.Client client;

    CassandraClient() {

    }

    public void connect() throws TException {
        TTransport transport = new TFramedTransport(new TSocket("localhost", 3333));
        client = new Cassandra.Client(new TBinaryProtocol(transport));
        transport.open();

        Map<String, String> cred = new HashMap<String, String>(2);
        cred.put("username", "username");
        cred.put("password", "password");
        AuthenticationRequest req = new AuthenticationRequest(cred);
        client.login(req);
    }

    public Cassandra.Client getClient() {
        return client;
    }
}
