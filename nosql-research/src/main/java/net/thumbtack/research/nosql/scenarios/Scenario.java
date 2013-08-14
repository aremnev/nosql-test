package net.thumbtack.research.nosql.scenarios;

import net.thumbtack.research.nosql.ResearcherReport;
import net.thumbtack.research.nosql.clients.Database;
import org.javasimon.SimonManager;
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

    protected Database db;
    protected long writesCount;
    protected boolean isRunning = false;

	protected Stopwatch actionStopwatch;

	public void init(Database database, long writesCount) {
        this.db = database;
        this.writesCount = writesCount;
    }

    @Override
    public void run() {
        this.isRunning = true;

        for (long i = 0; i < writesCount; i++) {
            synchronized (this) {
                if (!isRunning) return;
                try {
                    Split split = ResearcherReport.actions.start();
	                action();
	                split.stop();
                } catch (Exception e) {
	                ResearcherReport.addFailure();
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
