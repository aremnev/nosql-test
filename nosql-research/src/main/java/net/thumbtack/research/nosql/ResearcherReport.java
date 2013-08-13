package net.thumbtack.research.nosql;

import org.javasimon.SimonManager;
import org.javasimon.Split;
import org.javasimon.Stopwatch;

/**
 * Used to track test events and timings
 */
public class ResearcherReport {

	public static final Stopwatch actions = SimonManager.getStopwatch("action");
	public static final Stopwatch failures = SimonManager.getStopwatch("failure");
	public static final Stopwatch valueFailures = SimonManager.getStopwatch("value_failure");

	public static void addFailure() {
		failures.addSplit(new Split().stop());
	}

	public static void addValueFailure() {
		valueFailures.addSplit(new Split().stop());
	}

}
