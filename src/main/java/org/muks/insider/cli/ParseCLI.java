package org.muks.insider.cli;

import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by 15692 on 16/08/16.
 *
 * Command line yamlparser, based on apache commons CLI
 */

public class ParseCLI {
    private static Logger LOG = LoggerFactory.getLogger(ParseCLI.class);

    private Options CLI_OPTIONS = new Options();
    private String HELP_TEXT;
    String[] CLI_ARGS;


    public ParseCLI(String helpText) {
        this.HELP_TEXT = helpText;
    }


    /**
     * Eg: How to fetch command line provided ag: if (line.hasOption("cliArg1")) { DEFINITIONS_DIR =
     * line.getOptionValue("cliArg1"); }
     */
    public CommandLine getCommandLine(String[] cliArgs, int expectedArgsCount, ParserOptions optionsList)
            throws ParseException {
        CommandLine line;

        CLI_OPTIONS.addOption("help", false, "Help!");
        CLI_OPTIONS.addOption("version", false, "1.0");

        /** add options */
        for (ParserOptionEntity optionEntity : optionsList.getOptionsList()) {
            CLI_OPTIONS.addOption(optionEntity.name, optionEntity.hasArg, optionEntity.description);
        }


        this.CLI_ARGS = cliArgs;
        CommandLineParser parser = new DefaultParser();

        // parse the command line arguments
        line = parser.parse(CLI_OPTIONS, CLI_ARGS);

        try {
            if (line.getOptions().length < expectedArgsCount || line.hasOption("help")) {
                helpFormatter(HELP_TEXT);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }

        return line;
    }

    /**
     * Help text formatter
     */
    private void helpFormatter(String helpTextWithUsage) throws ParseException {
        /** Automatically generate the help statement */
        //System.out.println("# Warning: Some of the mandatory arguments are NOT defined.");
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(helpTextWithUsage, CLI_OPTIONS);

        System.out.println("");
        throw new ParseException("\n# Some of the mandatory arguments are missing/NOT defined.");
    }
}
