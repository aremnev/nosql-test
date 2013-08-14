package net.thumbtack.research.nosql.scenarios;

import net.thumbtack.research.nosql.Configurator;
import net.thumbtack.research.nosql.ResearcherReport;
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
public class ConsistencyAScenario extends Scenario {
    private static final Logger log = LoggerFactory.getLogger(ConsistencyAScenario.class);
    private static final StringSerializer ss = StringSerializer.get();
    private String key;

    @Override
    public void init(Database database, Configurator config) {
        super.init(database, config);
        key = UUID.randomUUID().toString();
    }

    @Override
    protected void action() throws Exception {
        String writtenValue = UUID.randomUUID().toString();
        db.write(key, ss.toByteBuffer(writtenValue));
        String readValue = ss.fromByteBuffer(db.read(key));

        if (!writtenValue.equals(readValue)) {
	        ResearcherReport.addValueFailure();
	        ResearcherReport.addFailure();
            log.warn("Written and read values for key {} are different. Written: {}, Read: {} ",
		            new Object [] { key, writtenValue, readValue } );
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
