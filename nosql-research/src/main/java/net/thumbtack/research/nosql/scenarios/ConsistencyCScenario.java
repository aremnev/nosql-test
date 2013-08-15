package net.thumbtack.research.nosql.scenarios;

import net.thumbtack.research.nosql.Configurator;
import net.thumbtack.research.nosql.clients.Database;
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

    private static int roleIdx = 0;
    private static int rolesCount = 0;
    private static int readersCount = 0;
    private static final char DELIMITER = '\t';
    private static String[] groupKeys;
    private static String[] groupReadKey;
    private static Map<Long, ByteBuffer> groupReadValues;
    private static Semaphore groupReadSemaphore;
    private static Semaphore groupAggrSemaphore;
    private static Random groupRandomKeyIdx;

    private enum Role {
        writer, reader, aggregator;
    }

    private String[] keys;
    private String[] readKey;
    private Role role;
    private Map<Long, ByteBuffer> readValues;
    private Semaphore readSemaphore;
    private Semaphore aggrSemaphore;
    private Random randomKeyIdx;
    private int keySetSize;

    @Override
    public void init(Database database, Configurator config) {
        super.init(database, config);
        synchronized (ConsistencyCScenario.class) {
            keySetSize = config.getInt(KEY_SET_SIZE_PROPERTY, 1000);
            if (rolesCount == 0) {
                readersCount = config.getDbHosts().length;
                rolesCount = readersCount + 2;
            }
            this.writesCount = config.getScWrites() / (config.getScThreads() / rolesCount);
            role = getRole();
            if (role.equals(Role.writer)) {
                groupKeys = createKeys();
                groupReadKey = new String[1];
                groupReadValues = new LinkedHashMap<>();
                groupReadSemaphore = new Semaphore(readersCount);
                groupAggrSemaphore = new Semaphore(0);
                groupRandomKeyIdx = new Random();
                randomKeyIdx = new Random();
            } else {
                randomKeyIdx = groupRandomKeyIdx;
            }
            keys = groupKeys;
            readKey = groupReadKey;
            readValues = groupReadValues;
            readSemaphore = groupReadSemaphore;
            aggrSemaphore = groupAggrSemaphore;

            if (role.equals(Role.aggregator)) {
                setReadKey(getNextKey());
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
        Split writeSplit = Reporter.startEvent();
        String prefix = System.nanoTime() + "" + DELIMITER;
        String value = generateString(prefix);
        db.write(getNextKey(), ss.toByteBuffer(value));
        Reporter.addEvent(Reporter.STOPWATCH_WRITE, writeSplit);
    }

    private void read() throws Exception {
        synchronized (getReadKey()) {
            Split readSplit = Reporter.startEvent();
            ByteBuffer buffer = db.read(getReadKey());
            readValues.put(System.nanoTime(), buffer);
            Reporter.addEvent(Reporter.STOPWATCH_READ, readSplit);
        }
    }

    private void aggregation() {
        StringBuilder detailedString = new StringBuilder();
        long oldTimestamp = 0;
        long firstValue = 0;
        for (Long time : readValues.keySet()) {
            long value = getTimestamp(readValues.get(time));
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

    private long getTimestamp(ByteBuffer buffer) {
        if (buffer != null) {
            byte[] bytes = buffer.array();
            for (int i = buffer.position(); i < bytes.length; i++) {
                if (bytes[i] == DELIMITER) {
                    return Long.valueOf(new String(Arrays.copyOfRange(bytes, buffer.position(), i)));
                }
            }
        }
        return 0L;
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
}