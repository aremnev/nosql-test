package net.thumbtack.research.nosql.scenarios;

import net.thumbtack.research.nosql.Configurator;
import net.thumbtack.research.nosql.ResearcherReport;
import net.thumbtack.research.nosql.clients.Database;
import org.javasimon.Split;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import java.util.Queue;
import java.util.UUID;

/**
 * User: vkornev
 * Date: 13.08.13
 * Time: 10:45
 * <p/>
 * Test database consistency
 */
public class ConsistencyBScenario extends Scenario {
    private static final Logger log = LoggerFactory.getLogger(ConsistencyBScenario.class);

    private static String groupKey;
    private static List<Long> groupReadValues;
    private static int roleIdx = 0;
    private static int rolesCount = 0;
    private char delimeter = '-';

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
            log.info("Create consistency_b scenario with role " + role.name() + " and key " + key);
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
                ResearcherReport.addEvent(ResearcherReport.STOPWATCH_FAILURE);
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
        Split writeSplit = ResearcherReport.startEvent();
        String value = generateString(System.nanoTime() + delimeter + "");
        db.write(key, ss.toByteBuffer(value));
        ResearcherReport.addEvent(ResearcherReport.STOPWATCH_WRITE, writeSplit);
    }

    private void read() {
        try {
            synchronized (key) {
                Split readSplit = ResearcherReport.startEvent();
                ByteBuffer buffer = db.read(key);
                Long value = 0L;
                if(buffer != null) {
                    byte[] bytes = buffer.array();
                    for (int i = buffer.position(); i < bytes.length ;i++) {
                        if (bytes[i] == delimeter) {
                            value = Long.valueOf(new String(Arrays.copyOfRange(bytes, buffer.position(), i)));
                            break;
                        }
                    }
                }
                ResearcherReport.addEvent(ResearcherReport.STOPWATCH_READ, readSplit);
                readValues.add(value);
            }
        } catch (Exception e) {
            readValues.add(0L);
            throw e;
        }
    }
}
