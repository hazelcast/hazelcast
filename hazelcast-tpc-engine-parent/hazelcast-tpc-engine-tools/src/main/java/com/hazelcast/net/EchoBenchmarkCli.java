/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.net;

import com.hazelcast.internal.tpcengine.ReactorType;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import static com.hazelcast.CliUtils.getToolsVersion;
import static com.hazelcast.CliUtils.printHelp;
import static com.hazelcast.GitInfo.getBuildTime;
import static com.hazelcast.GitInfo.getCommitIdAbbrev;

public class EchoBenchmarkCli {

    private final OptionParser parser = new OptionParser();

    private final OptionSpec<String> runtimeSpec = parser
            .accepts("runtime", "The duration of the test, e.g. 60s")
            .withRequiredArg().ofType(String.class)
            .defaultsTo("60s");

    private final OptionSpec<String> ioengineSpec = parser
            .accepts("ioengine", "The type of ioengine (AKA reactortype). "
                    + "Available options: iouring or nio. "
                    + "The nio option can't be used with fio. ")
            .withRequiredArg().ofType(String.class)
            .defaultsTo("iouring");

    public static void main(String[] args) throws Exception {
        EchoBenchmarkCli cli = new EchoBenchmarkCli();
        cli.run(args);
    }

    private void run(String[] args) throws Exception {
        System.out.println("EchoBenchmark");

        System.out.printf("Version: %s, Commit: %s, Build Time: %s%n",
                getToolsVersion(), getCommitIdAbbrev(), getBuildTime());

        OptionSpec helpSpec = parser.accepts("help", "Shows the help.").forHelp();
        OptionSet options = parser.parse(args);

        if (options.has(helpSpec)) {
            printHelp(parser, System.out);
            return;
        }

        EchoBenchmark benchmark = new EchoBenchmark();
        String runtime = options.valueOf(runtimeSpec);
        if (!runtime.endsWith("s")) {
            System.out.println("Runtime needs to end with 's'");
        }

        String runtimeSec = runtime.substring(0, runtime.length() - 1).trim();
        benchmark.runtimeSeconds = Integer.parseInt(runtimeSec);
        benchmark.spin = false;
        benchmark.reactorType = ReactorType.fromString(options.valueOf(ioengineSpec));
        benchmark.run();
        System.exit(0);
    }
}
