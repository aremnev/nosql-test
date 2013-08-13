package net.thumbtack.research.nosql.scenarios;

import net.thumbtack.research.nosql.clients.Database;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;

/**
 * User: vkornev
 * Date: 13.08.13
 * Time: 10:22
 *
 * Base NoSQL database test scenario interface
 */
public abstract class Scenario implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(Scenario.class);
    protected Database db;
    protected long writesCount;

    public void init(Database database, long writesCount) {
        db = database;
        this.writesCount = writesCount;
    }

    @Override
    public void run() {
        for (long i=0; i < writesCount; i++) {
            try {
                action();
            } catch (Exception e) {
                e.printStackTrace();
                log.error(e.getMessage());
            }
        }
    }

    protected abstract void action() throws Exception;
}
