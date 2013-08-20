package net.thumbtack.research.nosql.report;

import org.javasimon.SimonManager;
import org.javasimon.Split;

/**
 * Used to track test events and timings
 */
public class Reporter {

	private static final long NANOS_IN_MILLI = 1000000;

	public static final String STOPWATCH_SCENARIO = "scenario";
	public static final String STOPWATCH_ACTION = "action";
	public static final String STOPWATCH_READ = "read";
	public static final String STOPWATCH_READ_TIME_SERIES = "read_time_series";
	public static final String STOPWATCH_WRITE = "write";
	public static final String STOPWATCH_WRITE_TIME_SERIES = "write_time_series";
	public static final String STOPWATCH_FAILURE = "failure";
	public static final String STOPWATCH_VALUE_FAILURE = "valueFailure";


	public static Split startEvent() {
		Split result = new Split();
		return result;
	}

	public static void addEvent(final String stopwatch) {
		SimonManager.getStopwatch(stopwatch).addSplit(new Split().stop());
	}

	public static void addEvent(final String stopwatch, final Split event) {
		SimonManager.getStopwatch(stopwatch).addSplit(event);
	}

	public static long getCount(final String stopwatch) {
		return SimonManager.getStopwatch(stopwatch).getCounter();
	}

    public static void reset(final String stopwatch) {
        SimonManager.getStopwatch(stopwatch).reset();
    }

	public static double getTotal(final String stopwatch) {
		return (double)SimonManager.getStopwatch(stopwatch).getTotal() / NANOS_IN_MILLI;
	}

	public static double getMin(final String stopwatch) {
		return (double)SimonManager.getStopwatch(stopwatch).getMin() / NANOS_IN_MILLI;
	}

	public static double getMean(final String stopwatch) {
		return (double)SimonManager.getStopwatch(stopwatch).getMean() / NANOS_IN_MILLI;
	}

	public static double getMax(final String stopwatch) {
		return (double)SimonManager.getStopwatch(stopwatch).getMax() / NANOS_IN_MILLI;
	}

}
