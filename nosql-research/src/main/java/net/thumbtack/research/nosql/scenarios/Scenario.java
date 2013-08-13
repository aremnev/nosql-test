package net.thumbtack.research.nosql.scenarios;

import net.thumbtack.research.nosql.clients.Database;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Set;

/**
 * User: vkornev
 * Date: 13.08.13
 * Time: 10:22
 * <p/>
 * Base NoSQL database test scenario interface
 */
public abstract class Scenario implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(Scenario.class);

    protected Database db;
    protected long writesCount;
    protected List<Long> successfulWrites;
    protected List<Long> failedWrites;
    protected long sw;
    protected long fw;
    protected boolean isRunning = false;

    public void init(Database database, long writesCount, List<Long> successfulWrites, List<Long> failedWrites) {
        this.db = database;
        this.writesCount = writesCount;
        this.successfulWrites = successfulWrites;
        this.failedWrites = failedWrites;
        this.sw = 0;
        this.fw = 0;
    }

    @Override
    public void run() {
        this.isRunning = true;

        for (long i = 0; i < writesCount; i++) {
            synchronized (this) {
                if (!isRunning) return;
                try {
                    action();
                } catch (Exception e) {
                    e.printStackTrace();
                    log.error(e.getMessage());
                }
            }
        }

        close();
    }

    protected abstract void action() throws Exception;

    public synchronized void close() {
        isRunning = false;
        synchronized (Scenario.class) {
            successfulWrites.add(sw);
            failedWrites.add(fw);
        }
    }
}
