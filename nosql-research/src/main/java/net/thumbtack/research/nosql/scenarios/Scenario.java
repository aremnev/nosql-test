package net.thumbtack.research.nosql.scenarios;

import net.thumbtack.research.nosql.Configurator;
import net.thumbtack.research.nosql.Reporter;
import net.thumbtack.research.nosql.clients.Database;
import net.thumbtack.research.nosql.utils.LongSerializer;
import net.thumbtack.research.nosql.utils.StringSerializer;
import org.javasimon.Split;
import org.javasimon.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * User: vkornev
 * Date: 13.08.13
 * Time: 10:22
 * <p/>
 * Base NoSQL database test scenario interface
 */
public abstract class Scenario implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(Scenario.class);

    protected static final StringSerializer ss = StringSerializer.get();
    protected static final LongSerializer ls = LongSerializer.get();

    protected Database db;
    protected long writesCount;
    protected boolean isRunning = false;
    protected Configurator config;

	protected Stopwatch actionStopwatch;

	public void init(Database database, Configurator config) {
        this.db = database;
        this.config = config;
        this.writesCount = this.config.getScWrites() / this.config.getScThreads();
    }

    @Override
    public void run() {
        this.isRunning = true;

        for (long i = 0; i < writesCount; i++) {
            synchronized (this) {
                if (!isRunning) return;
                try {
                    Split split = Reporter.startEvent();
	                action();
	                Reporter.addEvent(Reporter.STOPWATCH_ACTION, split);
                } catch (Exception e) {
	                Reporter.addEvent(Reporter.STOPWATCH_FAILURE);
                    log.error(e.getMessage());
                }
            }
        }

        close();
    }

    protected abstract void action() throws Exception;

    public synchronized void close() {
        isRunning = false;
    }
}
