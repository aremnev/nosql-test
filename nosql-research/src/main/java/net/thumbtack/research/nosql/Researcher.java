package net.thumbtack.research.nosql;

import net.thumbtack.research.nosql.clients.Database;
import net.thumbtack.research.nosql.clients.DatabasePool;
import net.thumbtack.research.nosql.scenarios.Scenario;
import net.thumbtack.research.nosql.scenarios.ScenarioPool;
import org.apache.commons.cli.*;
import org.javasimon.Split;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static net.thumbtack.research.nosql.ResearcherReport.*;

/**
 * User: vkornev
 * Date: 12.08.13
 * Time: 18:52
 *
 * Main class. This is main runnable class.
 * Command line arguments
 *  -c,--config     -   config file name
 *  -h,--help       -   show help message
 */
public class Researcher {
    private static final Logger log = LoggerFactory.getLogger(Researcher.class);

    private static final String CLI_CONFIG = "config";
    private static final String CLI_HELP = "help";

    public static void main(String[] args) throws ParseException {

        Options options = getOptions();
        CommandLine commandLine = new GnuParser().parse(options, args);

        if (!isCommandLineValid(commandLine, options)) return;

        Configurator config = new Configurator(commandLine.getOptionValue(CLI_CONFIG));

        int threadsCount = config.getScThreads();

        ThreadPoolExecutor threadPool = new ThreadPoolExecutor(threadsCount, threadsCount, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingDeque<Runnable>());

        List<Database> dbs = new ArrayList<>(threadsCount);
        Database db;

	    log.info("Initializing {} clients...", threadsCount);
        for (int i=0; i < threadsCount; i++) {
            try {
                db = DatabasePool.get(config.getDbName());
                db.init(config);
                dbs.add(db);
            } catch (Exception e) {
                e.printStackTrace();
                log.error(e.getMessage());
                throw new RuntimeException(e);
            }
        }

        List<Scenario> scs = new ArrayList<>(threadsCount);
	    log.info("Scheduling tests...");
	    Split scenarioSplit = startEvent();
        for (Database initDB : dbs) {
            try {
                Scenario sc = ScenarioPool.get(config.getScName());
                sc.init(initDB, config);
                scs.add(sc);
                threadPool.submit(sc);
            } catch (Exception e) {
                e.printStackTrace();
                log.error(e.getMessage());
                throw new RuntimeException(e);
            }
        }

	    log.info("Running tests with {} actions...", config.getScWrites());
        while (threadPool.getActiveCount() > 0) {}
	    addEvent(STOPWATCH_SCENARIO, scenarioSplit);

	    log.info("Shutting down clients...");
        threadPool.shutdown();

	    log.info("--- Tests complete ---");

	    printReport();
    }

	private static void printReport() {
		log.info("Total time: {}ms", getTotal(STOPWATCH_SCENARIO));
		log.info("Total writes: " + getCount(STOPWATCH_ACTION));
		log.info("Total failures: " + getCount(STOPWATCH_FAILURE));
		log.info("Incomplete writes: " + getCount(STOPWATCH_VALUE_FAILURE));
		log.info("Average throughput: {} req/sec", getCount(STOPWATCH_ACTION) / getTotal(STOPWATCH_SCENARIO) * 1000);
		log.info("Action timings:\t total={}ms, \tmin={}ms, \tmean={}ms, \tmax={}ms",
				new Object[]{
						getTotal(STOPWATCH_ACTION),
						getMin(STOPWATCH_ACTION),
						getMean(STOPWATCH_ACTION),
						getMax(STOPWATCH_ACTION)
				}
		);
		log.info("Writing timings:\t total={}ms, \tmin={}ms, \tmean={}ms, \tmax={}ms",
				new Object[]{
						getTotal(STOPWATCH_WRITE),
						getMin(STOPWATCH_WRITE),
						getMean(STOPWATCH_WRITE),
						getMax(STOPWATCH_WRITE)
				}
		);
		log.info("Reading timings:\t total={}ms, \tmin={}ms, \tmean={}ms, \tmax={}ms",
				new Object[]{
						getTotal(STOPWATCH_READ),
						getMin(STOPWATCH_READ),
						getMean(STOPWATCH_READ),
						getMax(STOPWATCH_READ)
				}
		);
	}

	private static Options getOptions() {
        return  new Options()
                .addOption(CLI_CONFIG.substring(0, 1), CLI_CONFIG, true, "Config file name")
                .addOption(CLI_HELP.substring(0, 1), CLI_HELP, false, "Show this is help");
    }

    private static boolean isCommandLineValid(CommandLine commandLine, Options options) {
        if (commandLine.hasOption(CLI_HELP)
                || !commandLine.hasOption(CLI_CONFIG)) {
            new HelpFormatter().printHelp("nosql-research -c <config file name> [-h]", options);
            return false;
        }
        return true;
    }

}
