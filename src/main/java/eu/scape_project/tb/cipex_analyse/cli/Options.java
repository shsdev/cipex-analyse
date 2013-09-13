/*
 *  Copyright 2012 The SCAPE Project Consortium.
 * 
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 * 
 *       http://www.apache.org/licenses/LICENSE-2.0
 * 
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *  under the License.
 */
package eu.scape_project.tb.cipex_analyse.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;

/**
 * Command line interface options.
 *
 * @author Sven Schlarb https://github.com/shsdev
 * @version 0.1
 */
public class Options {

    // Statics to set up command line arguments
    public static final String HELP_FLG = "h";
    public static final String HELP_OPT = "help";
    public static final String HELP_OPT_DESC = "print this message [optional].";
    public static final String DIR_FLG = "d";
    public static final String DIR_OPT = "dir";
    public static final String DIR_OPT_DESC = "Local mode (default): Directory with container file(s), Hadoop mode (option -m): HDFS directory containing (the) text file(s) listing HDFS paths to container files. [required].";
    public static org.apache.commons.cli.Options OPTIONS = new org.apache.commons.cli.Options();
    public static final String USAGE = "hadoop jar "
            + "target/cipex-analyse-1.0-SNAPSHOT-jar-with-dependencies.jar "
            + "";

    static {
        OPTIONS.addOption(HELP_FLG, HELP_OPT, false, HELP_OPT_DESC);
        OPTIONS.addOption(DIR_FLG, DIR_OPT, true, DIR_OPT_DESC);
    }

    public static void initOptions(CommandLine cmd, CliConfig pc) {
        // dir
        String dirStr;
        if (!(cmd.hasOption(DIR_OPT) && cmd.getOptionValue(DIR_OPT) != null)) {
            exit("No directory given.", 1);
        } else {
            dirStr = cmd.getOptionValue(DIR_OPT);
            pc.setDirStr(dirStr);
            System.out.println("Directory: " + dirStr);
        }
    }

    public static void exit(String msg, int status) {
        if (status > 0) {
            System.out.println(msg);
        } else {
            System.out.println(msg);
        }
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(USAGE, OPTIONS, true);
        System.exit(status);
    }
}
