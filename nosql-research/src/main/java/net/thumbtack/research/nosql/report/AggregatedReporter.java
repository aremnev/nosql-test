package net.thumbtack.research.nosql.report;

import net.thumbtack.research.nosql.Configurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * Event to be registered/aggregated
 */
class Event {
    private long mark;

    Event(long v) {
        this.mark = v;
    }

    public int hashCode() {
        return (int)mark;
    }

    public boolean equals(Object o) {
        return ((Event)o).mark == this.mark;
    }
}

public class AggregatedReporter {

    private static final Logger tslog = LoggerFactory.getLogger("timeseries");

    public static final int EVENT_OLD_VALUE = 1;

    public static final int BUFFER_SIZE = 100000;

    private static BatchUpdater<Event> eventUpdater;

    public static void configure(Configurator config) {
        eventUpdater = new BatchUpdater<Event>("aggregated-event", BUFFER_SIZE, config.getReportFlushInterval()) {{
            addEvent(EVENT_OLD_VALUE, new FlushEvent<Event>() {
                public void flush(Collection<Event> buffer) {
                    long readCount = Reporter.getCount(Reporter.STOPWATCH_READ_TIME_SERIES);
                    double readMean = Reporter.getMean(Reporter.STOPWATCH_READ_TIME_SERIES);
                    long writeCount = Reporter.getCount(Reporter.STOPWATCH_WRITE_TIME_SERIES);
                    double writeMean = Reporter.getMean(Reporter.STOPWATCH_WRITE_TIME_SERIES);
                    Reporter.reset(Reporter.STOPWATCH_READ_TIME_SERIES);
                    Reporter.reset(Reporter.STOPWATCH_WRITE_TIME_SERIES);
                    tslog.debug("{}\t{}\t{}\t{}\t{}\t{}", new Object[]{
                            System.nanoTime(),
                            readCount,
                            readMean,
                            writeCount,
                            writeMean,
                            buffer.size()
                    });
                }
            });
        }};
    }

    public static void addEvent(int type) {
        Event event = new Event(System.nanoTime());
        eventUpdater.add(type, event);
    }

    public static void stop() {
        eventUpdater.cleanup();
    }

}