package net.thumbtack.research.nosql.scenarios;

import net.thumbtack.research.nosql.Configurator;
import net.thumbtack.research.nosql.clients.Client;
import net.thumbtack.research.nosql.report.Reporter;
import net.thumbtack.research.nosql.utils.LongSerializer;
import net.thumbtack.research.nosql.utils.StringSerializer;
import org.javasimon.Split;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

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

    protected Client db;
    protected long writesCount;
    protected boolean isRunning = false;
    protected Configurator config;

    private long stringSize;

    public void init(Client client, Configurator config) {
        this.db = client;
        this.config = config;
        this.writesCount = this.config.getScWrites() / this.config.getScThreads();
        this.stringSize = this.config.getSCStringSize();
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
                    log.error("Cause: {}; Stack trace: {}", e, e.getStackTrace());
                }
            }
        }

        close();
    }

    protected abstract void action() throws Exception;

    public synchronized void close() {
        isRunning = false;
    }

    public final String generateString() {
        char[] chars = "abcdefghijklmnopqrstuvwxyz".toCharArray();
        StringBuilder sb = new StringBuilder();
        Random random = new Random();
        for (int i = 0; i < stringSize; i++) {
            char c = chars[random.nextInt(chars.length)];
            sb.append(c);
        }
        return sb.toString();
    }

    protected void onRead(Split readSplit) {
        Reporter.addEvent(Reporter.STOPWATCH_READ, readSplit);
        Reporter.addEvent(Reporter.STOPWATCH_READ_TIME_SERIES, readSplit);
    }

    protected void onWrite(Split writeSplit) {
        Reporter.addEvent(Reporter.STOPWATCH_WRITE, writeSplit);
        Reporter.addEvent(Reporter.STOPWATCH_WRITE_TIME_SERIES, writeSplit);
    }
}
