** Create Cassandra key space
    * connect to cassandra server: cassandra-cli -h <cassandra server address> -p 9160
    * create keyspace: create keyspace <keyspace name>;
    * select keyspace: use <keyspace name>;
    * create column family: create column family <column family name> with comparator=UTF8Type and key_validation_class=UTF8Type;
