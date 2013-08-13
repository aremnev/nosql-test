package net.thumbtack.research.nosql.scenarios;

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

    @Override
    protected void action() throws Exception {
        String key = UUID.randomUUID().toString();
        ByteBuffer value = ByteBuffer.wrap(key.getBytes("UTF-8"));
        db.write(key, value);
    }
}
