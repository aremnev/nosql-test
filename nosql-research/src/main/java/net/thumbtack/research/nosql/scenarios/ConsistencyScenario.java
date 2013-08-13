package net.thumbtack.research.nosql.scenarios;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
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

    @Override
    protected void action() throws Exception {
        String key = UUID.randomUUID().toString();
        ByteBuffer value = ByteBuffer.wrap(key.getBytes("UTF-8"));
        db.write(key, value);
        if (!value.equals(db.read(key))) {
            log.warn("Wrote and read values is different. Key: " + key);
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
