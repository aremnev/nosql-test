package net.thumbtack.research.nosql.scenarios;

import net.thumbtack.research.nosql.Configurator;
import net.thumbtack.research.nosql.clients.Client;
import net.thumbtack.research.nosql.report.AggregatedReporter;
import net.thumbtack.research.nosql.report.Reporter;
import org.javasimon.Split;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Semaphore;

/**
 * User: vkornev
 * Date: 13.08.13
 * Time: 10:45
 * <p/>
 * Test database consistency. This test checking database consistency as close as possible to the real world.
 * This is test writing data with random key from fixed keys set. In parallel thread this test try to read data
 * few times with another random key from same key set and checks got values.
 * <p/>
 * Some usage advices:
 * <ul>
 * <li>count of servers in db.hosts parameter must be more then one
 * <li>set threads count as (x+2) * n, x - is count of servers in db.hosts parameters, and n - is count of writers
 * </ul>
 */
public final class ConsistencyCScenario extends Scenario {
    private static final Logger log = LoggerFactory.getLogger(ConsistencyCScenario.class);
    private static final Logger detailedLog = LoggerFactory.getLogger("detailed");
    private static final Logger rawLog = LoggerFactory.getLogger("rawLog");
    private static final String KEY_SET_SIZE_PROPERTY = "consistency_c.keySetSize";

    private static final String VALE_COLUMN = "1";
    private static final String DATA_COLUMN = "2";

    private static int roleIdx = 0;
    private static int rolesCount = 0;
    private static int readersCount = 0;
    private static String[] groupKeys;
    private static String[] groupReadKey;
    private static boolean[] groupIsWritting;

    private static Map<Long, ByteBuffer> groupReadValues;
    private static Semaphore groupReadSemaphore;
    private static Semaphore groupAggrSemaphore;
    private static Random groupRandomKeyIdx;

    private enum Role {
        writer, reader, aggregator;
    }

    private String[] keys;
    private String[] readKey;
    private boolean[] isWriting;
    private Role role;
    private long value;
    private Map<String, ByteBuffer> writeValues;
    private Set<String> readColumns;
    private Map<Long, ByteBuffer> readValues;
    private Semaphore readSemaphore;
    private Semaphore aggrSemaphore;
    private Random randomKeyIdx;
    private int keySetSize;

    @Override
    public void init(Client client, Configurator config) {
        super.init(client, config);
        synchronized (ConsistencyCScenario.class) {
            keySetSize = config.getInt(KEY_SET_SIZE_PROPERTY, 1000);
            if (rolesCount == 0) {
                readersCount = config.getDbHosts().length;
                rolesCount = readersCount + 2;
            }
            role = getRole();
            if (role.equals(Role.writer)) {
                this.writesCount = config.getScWrites() / (config.getScThreads() / rolesCount);
                groupKeys = createKeys();
                groupReadKey = new String[1];
                groupIsWritting = new boolean[1];
                groupReadValues = new LinkedHashMap<>();
                groupReadSemaphore = new Semaphore(readersCount);
                groupAggrSemaphore = new Semaphore(0);
                groupRandomKeyIdx = new Random();
                randomKeyIdx = new Random();
                value = 0;
                writeValues = new HashMap<>();
                writeValues.put(DATA_COLUMN, ss.toByteBuffer(generateString()));
            } else {
                randomKeyIdx = groupRandomKeyIdx;
                writesCount = Long.MAX_VALUE;
            }
            keys = groupKeys;
            readKey = groupReadKey;
            isWriting = groupIsWritting;
            readValues = groupReadValues;
            readSemaphore = groupReadSemaphore;
            aggrSemaphore = groupAggrSemaphore;
            setWriting(true);

            if (role.equals(Role.aggregator)) {
                setReadKey(getNextKey());
            }
            if (role.equals(Role.reader)) {
                readColumns = new HashSet<>();
                readColumns.add(VALE_COLUMN);
            }

            log.debug("Create consistency_c scenario with role " + role.name());
        }
    }

    @Override
    protected void action() throws Exception {
        switch (role) {
            case writer: {
                write();
                break;
            }
            case aggregator: {
                aggrSemaphore.acquire(readersCount);
                aggregation();
                setReadKey(getNextKey());
                readSemaphore.release(readersCount);
                break;
            }
            case reader: {
                readSemaphore.acquire(1);
                read();
                aggrSemaphore.release(1);
                break;
            }
        }
        if (!isWriting()) {
            close();
        }
    }

    @Override
    public void close() {
        super.close();
        try {
            db.close();
        } catch (Exception e) {
            e.printStackTrace();
            log.error(e.getMessage());
        }
        if (role.equals(Role.writer)) {
            setWriting(false);
        }
    }

    private Role getRole() {
        Role r;
        if (roleIdx >= rolesCount) {
            roleIdx = 0;
        }
        switch (roleIdx++) {
            case 0: r = Role.writer; break;
            case 1: r = Role.aggregator; break;
            default: r = Role.reader;
        }
        return r;
    }

    private void write() throws Exception {
        writeValues.put(VALE_COLUMN, ls.toByteBuffer(value));
        Split writeSplit = Reporter.startEvent();
        db.write(getNextKey(), writeValues);
        onWrite(writeSplit);
        value++;
    }

    private void read() throws Exception {
        synchronized (getReadKey()) {
            Split readSplit = Reporter.startEvent();
            Map<String, ByteBuffer> data = db.read(getReadKey(), readColumns);
            readValues.put(System.nanoTime(), data.get(VALE_COLUMN));
            onRead(readSplit);
        }
    }

    private void aggregation() {
        StringBuilder detailedString = new StringBuilder();
        long oldTimestamp = 0;
        long firstValue = 0;
        for (Long time : readValues.keySet()) {
            long value = 0L;
            ByteBuffer buffer = readValues.get(time);
            if (buffer != null) {
                value = ls.fromByteBuffer(readValues.get(time));
            }
            if (oldTimestamp == 0) {
                oldTimestamp = value;
                firstValue = value;
            }
            if (oldTimestamp > value) {
                Reporter.addEvent(Reporter.STOPWATCH_FAILURE);
                AggregatedReporter.addEvent(AggregatedReporter.EVENT_OLD_VALUE);
            }
            if (detailedLog.isDebugEnabled()) {
                if (firstValue == value) {
                    detailedString.append("1\t");
                } else if (firstValue < value) {
                    detailedString.append("2\t");
                } else {
                    detailedString.append("0\t");
                }
            }
            if (rawLog.isDebugEnabled()) {
                rawLog.debug(getReadKey() + "\t{}\t{}", time, value);
            }
            oldTimestamp = value;
        }
        if (detailedLog.isDebugEnabled()) {
            detailedLog.debug("{}\t{}", getReadKey(), detailedString.toString());
        }

        readValues.clear();
    }

    private String[] createKeys() {
        String[] ks = new String[keySetSize];
        for (int i=0; i<keySetSize; i++) {
            ks[i] = UUID.randomUUID().toString();
        }
        return ks;
    }

    private String getNextKey() {
        return keys[randomKeyIdx.nextInt(keys.length)];
    }

    private String getReadKey() {
        return readKey[0];
    }

    private void setReadKey(String k) {
        readKey[0] = k;
    }

    public boolean isWriting() {
        return isWriting[0];
    }

    public void setWriting(boolean writing) {
        isWriting[0] = writing;
    }
}
