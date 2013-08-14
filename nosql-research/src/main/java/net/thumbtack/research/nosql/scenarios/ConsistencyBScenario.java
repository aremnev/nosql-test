package net.thumbtack.research.nosql.scenarios;

import net.thumbtack.research.nosql.report.AggregatedReporter;
import net.thumbtack.research.nosql.Configurator;
import net.thumbtack.research.nosql.report.Reporter;
import net.thumbtack.research.nosql.clients.Database;
import org.javasimon.Split;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.*;

/**
 * User: vkornev
 * Date: 13.08.13
 * Time: 10:45
 * <p/>
 * Test database consistency. This test checking real database consistency with consistency level ALL.
 * This is test try to do more then one reads and get new values of record before old values of record in same time.
 * <p/>
 * Some usage advices:
 *  count of servers in db.hosts parameter must be more then one
 *  set threads count as (x+1) * n, x - is count of servers in db.hosts parameters, and n - is count of writers
 *  set read and write consistency to ALL
 */
public class ConsistencyBScenario extends Scenario {
    private static final Logger log = LoggerFactory.getLogger(ConsistencyBScenario.class);

    private static String groupKey;
    private static List<Long> groupReadValues;
    private static int roleIdx = 0;
    private static int rolesCount = 0;
    private static final char DELIMETER = '\t';

    private static long start = System.nanoTime();

    private enum Role {
        writer, reader;
    }

    private String key;
    private Role role;
    private List<Long> readValues;

    @Override
    public void init(Database database, Configurator config) {
        super.init(database, config);
        this.writesCount = config.getScWrites();
        synchronized (ConsistencyBScenario.class) {
            if (rolesCount == 0) {
                rolesCount = config.getDbHosts().length + 1;
            }
            role = getRole();
            if (role.equals(Role.writer)) {
                groupKey = UUID.randomUUID().toString();
                groupReadValues = new ArrayList<>();
            }
            key = groupKey;
            readValues = groupReadValues;
            log.debug("Create consistency_b scenario with role " + role.name() + " and key " + key);
        }
    }

    @Override
    protected void action() throws Exception {
        if (role.equals(Role.writer)) {
            write();
        } else {
            read();
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
        long oldTimestamp = 0;
        for (long newTimestamp : readValues) {
            if (oldTimestamp > newTimestamp) {
                Reporter.addEvent(Reporter.STOPWATCH_VALUE_FAILURE);
                Reporter.addEvent(Reporter.STOPWATCH_FAILURE);
//                AggregatedReporter.addEvent(AggregatedReporter.EVENT_OLD_VALUE);
            }
            oldTimestamp = newTimestamp;
        }
    }

    private Role getRole() {
        if (roleIdx >= rolesCount) {
            roleIdx = 0;
        }
        if (roleIdx++ == 0) {
            return Role.writer;
        }
        return Role.reader;
    }

    private void write() {
        Split writeSplit = Reporter.startEvent();
        String prefix = System.nanoTime() + "" + DELIMETER;
        String value = generateString(prefix);
        db.write(key, ss.toByteBuffer(value));
        Reporter.addEvent(Reporter.STOPWATCH_WRITE, writeSplit);
    }

    private void read() {
        Long value = 0L;
        try {
            synchronized (key) {
                // read
                Split readSplit = Reporter.startEvent();
                Long now = System.nanoTime();
                ByteBuffer buffer = db.read(key);
                Reporter.addEvent(Reporter.STOPWATCH_READ, readSplit);

                if (buffer != null) {
                    byte[] bytes = buffer.array();
                    for (int i = buffer.position(); i < bytes.length; i++) {
                        if (bytes[i] == DELIMETER) {
                            value = Long.valueOf(new String(Arrays.copyOfRange(bytes, buffer.position(), i)));
                            break;
                        }
                    }
                }

                readValues.add(value);
            }
        } catch (Exception e) {
            readValues.add(value);
            throw e;
        }
    }
}
