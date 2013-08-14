package net.thumbtack.research.nosql.report;

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

    public static final int FLUSH_INTERVAL = 1000;
    public static final int BUFFER_SIZE = 100000;

    private static BatchUpdater<Event> eventUpdater = new BatchUpdater<Event>("aggregated-event", BUFFER_SIZE, FLUSH_INTERVAL) {{
        addEvent(EVENT_OLD_VALUE, new FlushEvent<Event>() {
            public void flush(Collection<Event> buffer) {
                tslog.debug("{}\t{}", new Object[]{ System.nanoTime(), buffer.size()});
            }
        });
    }};

    public static void addEvent(int type) {
        Event event = new Event(System.nanoTime());
        eventUpdater.add(type, event);
    }

    public static void stop() {
        eventUpdater.cleanup();
    }

}