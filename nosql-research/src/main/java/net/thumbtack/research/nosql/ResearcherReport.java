package net.thumbtack.research.nosql;

import org.javasimon.SimonManager;
import org.javasimon.Split;
import org.javasimon.Stopwatch;

import java.util.HashMap;

/**
 * Used to track test events and timings
 */
public class ResearcherReport {

	public static final String STOPWATCH_SCENARIO = "scenario";
	public static final String STOPWATCH_ACTION = "action";
	public static final String STOPWATCH_FAILURE = "failure";
	public static final String STOPWATCH_VALUE_FAILURE = "valueFailure";


	public static final Stopwatch scenario = SimonManager.getStopwatch(STOPWATCH_SCENARIO);
	public static final Stopwatch actions = SimonManager.getStopwatch(STOPWATCH_ACTION);
	public static final Stopwatch failures = SimonManager.getStopwatch(STOPWATCH_FAILURE);
	public static final Stopwatch valueFailures = SimonManager.getStopwatch(STOPWATCH_VALUE_FAILURE);

	private static final HashMap<String, Stopwatch> stopwatchMap = new HashMap<String, Stopwatch>() {{
		put(STOPWATCH_SCENARIO, scenario);
		put(STOPWATCH_SCENARIO, scenario);
		put(STOPWATCH_SCENARIO, scenario);
		put(STOPWATCH_SCENARIO, scenario);
	}};


	public static void addEvent(String stopwatch) {
		SimonManager.getStopwatch(stopwatch).addSplit(new Split().stop());
	}

	public static void addValueFailure() {
		valueFailures.addSplit(new Split().stop());
	}

	public static

}
