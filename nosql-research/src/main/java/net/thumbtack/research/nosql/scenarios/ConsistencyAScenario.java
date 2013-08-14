package net.thumbtack.research.nosql.scenarios;

import net.thumbtack.research.nosql.Configurator;
import net.thumbtack.research.nosql.ResearcherReport;
import net.thumbtack.research.nosql.clients.Database;
import org.javasimon.Split;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * User: vkornev
 * Date: 13.08.13
 * Time: 10:45
 * <p/>
 * Test database consistency.
 */
public class ConsistencyAScenario extends Scenario {
    private static final Logger log = LoggerFactory.getLogger(ConsistencyAScenario.class);
    private String key;

    @Override
    public void init(Database database, Configurator config) {
        super.init(database, config);
        key = UUID.randomUUID().toString();
    }

    @Override
    protected void action() throws Exception {
        String writtenValue = UUID.randomUUID().toString();

	    // write
	    Split writeSplit = ResearcherReport.startEvent();
	    db.write(key, ss.toByteBuffer(writtenValue));
	    ResearcherReport.addEvent(ResearcherReport.STOPWATCH_WRITE, writeSplit);

	    // read
	    Split readSplit = ResearcherReport.startEvent();
	    String readValue = ss.fromByteBuffer(db.read(key));
	    ResearcherReport.addEvent(ResearcherReport.STOPWATCH_READ, readSplit);

        // compare
	    if (!writtenValue.equals(readValue)) {
	        ResearcherReport.addEvent(ResearcherReport.STOPWATCH_VALUE_FAILURE);
	        ResearcherReport.addEvent(ResearcherReport.STOPWATCH_FAILURE);
	        log.warn("Written and read values for key {} are different", new Object[]{key});
            if(log.isDebugEnabled()) {
	            log.debug("Key: {}, Written: {}, Read: {} ", new Object [] { key, writtenValue, readValue } );
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
}
