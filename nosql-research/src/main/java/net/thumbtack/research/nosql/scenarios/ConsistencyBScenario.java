package net.thumbtack.research.nosql.scenarios;

import net.thumbtack.research.nosql.Configurator;
import net.thumbtack.research.nosql.clients.Database;
import net.thumbtack.research.nosql.utils.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * User: vkornev
 * Date: 13.08.13
 * Time: 10:45
 *
 * Test database consistency
 */
public class ConsistencyBScenario extends Scenario {
    private static final Logger log = LoggerFactory.getLogger(ConsistencyBScenario.class);
    private static final StringSerializer ss = StringSerializer.get();
    private String key;

    @Override
    public void init(Database database, Configurator config) {
        super.init(database, config);
        synchronized (ConsistencyBScenario.class) {

        }
    }

    @Override
    protected void action() throws Exception {
        String writtenValue = UUID.randomUUID().toString();
        db.write(key, ss.toByteBuffer(writtenValue));
        String readValue = ss.fromByteBuffer(db.read(key));
        if (!writtenValue.equals(readValue)) {
            log.warn("Written and read values are different. " +
                    "Key: " + key +
                    " Written data: " + writtenValue +
                    "; Read data: " + readValue);
        }
        else {
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
