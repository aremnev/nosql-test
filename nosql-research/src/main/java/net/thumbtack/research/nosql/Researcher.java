package net.thumbtack.research.nosql;

import net.thumbtack.research.nosql.clients.Database;
import net.thumbtack.research.nosql.clients.DatabasePool;
import net.thumbtack.research.nosql.scenarios.Scenario;
import net.thumbtack.research.nosql.scenarios.ScenarioPool;
import org.apache.commons.cli.*;
import org.javasimon.SimonManager;
import org.javasimon.Stopwatch;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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
        long writesCount = config.getScWrites();

        ThreadPoolExecutor threadPool = new ThreadPoolExecutor(threadsCount, threadsCount, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingDeque<Runnable>());

        List<Database> dbs = new ArrayList<>(threadsCount);
        Database db;

	    log.info("Initializing clients...");
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
        for (Database initDB : dbs) {
            try {
                Scenario sc = ScenarioPool.get(config.getScName());
                sc.init(initDB, writesCount / threadsCount);
                scs.add(sc);
                threadPool.submit(sc);
            } catch (Exception e) {
                e.printStackTrace();
                log.error(e.getMessage());
                throw new RuntimeException(e);
            }
        }

	    log.info("Running tests...");
        while (threadPool.getActiveCount() > 0) {}

	    log.info("Shutting down clients...");
        threadPool.shutdown();

        long successful = 0;
        long failed = 0;
        for (Scenario s: scs) {
            successful += s.getSw();
            failed += s.getFw();
        }
	    log.info("Tests complete");

	    log.warn("---------------------------------------------------------------------");
        log.warn("Total writes: " + ResearcherReport.actions.getCounter());
	    log.warn("Total failures: " + ResearcherReport.failures.getCounter());
        log.warn("Incomplete writes: " + ResearcherReport.valueFailures.getCounter());
	    log.warn("Action timings: total={}ms, min={}ms, mean={}ms, max={}ms",
			    new Object [] {
					    new Long(ResearcherReport.actions.getTotal() / 1000000),
					    new Long(ResearcherReport.actions.getMin() / 1000000),
					    new Double(ResearcherReport.actions.getMean() / 1000000),
					    new Long(ResearcherReport.actions.getMax() / 1000000)
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
