/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast;

import joptsimple.BuiltinHelpFormatter;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import java.io.OutputStream;


public final class CliUtils {

    private static final int HELP_WIDTH = 160;
    private static final int HELP_INDENTATION = 2;

    private CliUtils() {
    }

    @SuppressWarnings("PMD.PreserveStackTrace")
    public static OptionSet initOptionsWithHelp(OptionParser parser, String[] args) {
        return initOptionsWithHelp(parser, null, args);
    }

    public static OptionSet initOptionsWithHelp(OptionParser parser, String help, String[] args) {
        try {
            OptionSpec helpSpec = parser.accepts("help", "Shows the help.").forHelp();
            OptionSet options = parser.parse(args);

            if (options.has(helpSpec)) {
                if (help != null) {
                    printHelp(help);
                }
                printHelp(parser, System.out);
            }

            return options;
        } catch (OptionException e) {
            throw new CommandLineExitException(e.getMessage() + ". Use --help to get overview of the help options.");
        }
    }

    public static OptionSet initOptionsOnlyWithHelp(OptionParser parser, String content, String[] args) {
        try {
            OptionSpec helpSpec = parser.accepts("help", "Shows the help.").forHelp();
            OptionSet options = parser.parse(args);

            if (options.has(helpSpec)) {
                printHelp(content);
                printHelp(parser, System.out);
            }

            if (options.nonOptionArguments().size() > 0) {
                printHelp(content);
                printHelp(parser, System.out);
            }

            return options;
        } catch (OptionException e) {
            throw new CommandLineExitException(e.getMessage() + ". Use --help to get overview of the help options.");
        }
    }

    private static void printHelp(String content) {
        System.out.println(content);
        System.out.println();
    }

    public static void printHelp(OptionParser parser, OutputStream sink) {
        try {
            parser.formatHelpWith(new BuiltinHelpFormatter(HELP_WIDTH, HELP_INDENTATION));
            parser.printHelpOn(sink);
        } catch (Exception e) {
            throw new CommandLineExitException("Could not print command line help", e);
        }
    }
}
