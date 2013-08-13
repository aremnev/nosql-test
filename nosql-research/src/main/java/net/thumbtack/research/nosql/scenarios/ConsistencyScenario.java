package net.thumbtack.research.nosql.scenarios;

import net.thumbtack.research.nosql.clients.Database;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;

/**
 * User: vkornev
 * Date: 13.08.13
 * Time: 10:45
 *
 * Test database consistency
 */
public class ConsistencyScenario extends Scenario {
    private static final Logger log = LoggerFactory.getLogger(ConsistencyScenario.class);
    private String key;

    @Override
    public void init(Database database, long writesCount, List<Long> successfulWrites, List<Long> failedWrites) {
        super.init(database, writesCount, successfulWrites, failedWrites);
        key = UUID.randomUUID().toString();
    }

    @Override
    protected void action() throws Exception {
        ByteBuffer value = ByteBuffer.wrap(UUID.randomUUID().toString().getBytes("UTF-8"));
        db.write(key, value);
        ByteBuffer newValue = db.read(key);
        if (!value.equals(newValue)) {
            log.warn("Written and read values are different. " +
                    "Key: " + key +
                    " New data: " + new String(newValue.array(), "UTF-8") +
                    "; Old data: " + new String(value.array(), "UTF-8"));
            fw++;
        }
        else {
            sw++;
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
}
