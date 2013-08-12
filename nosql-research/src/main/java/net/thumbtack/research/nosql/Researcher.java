package net.thumbtack.research.nosql;

import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * User: vkornev
 * Date: 12.08.13
 * Time: 18:52
 *
 * Main class. This is main runnable class.
 * Command line arguments
 *  c config      -   config file name
 *  d database    -   database name. Supported databases: cassandra.
 */
public class Researcher {
    private static final Logger log = LoggerFactory.getLogger(Researcher.class);

    private static final String CLI_CONFIG = "config";
    private static final String CLI_DATABASE = "database";
    private static final String CLI_HELP = "help";

    public static void main(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption(CLI_CONFIG.substring(0,1), CLI_CONFIG, true, "config file name");
        options.addOption(CLI_DATABASE.substring(0,1), CLI_DATABASE, true, "database name. Supported databases: cassandra.");
        options.addOption(CLI_HELP.substring(0,1), CLI_HELP, false, "Show this is help");
        CommandLine commandLine = new GnuParser().parse(options, args);

        if (!commandLine.hasOption(CLI_CONFIG) || !commandLine.hasOption(CLI_DATABASE) || commandLine.hasOption("h")) {
            new HelpFormatter().printHelp("nosql-research -c <config file name> -d <database> [-h]", options);
        }

        Configurator config = new Configurator(commandLine.getOptionValue(CLI_CONFIG));
        Database db;
        try {
            db = DatabasePool.get(commandLine.getOptionValue(CLI_DATABASE));
        } catch (Exception e) {
            e.printStackTrace();
            log.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
