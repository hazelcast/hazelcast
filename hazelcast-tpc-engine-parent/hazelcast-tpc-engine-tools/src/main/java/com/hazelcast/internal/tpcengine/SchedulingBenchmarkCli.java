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

package com.hazelcast.internal.tpcengine;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import static com.hazelcast.internal.tpcengine.CliUtils.getToolsVersion;
import static com.hazelcast.internal.tpcengine.CliUtils.printHelp;
import static com.hazelcast.internal.tpcengine.GitInfo.getBuildTime;
import static com.hazelcast.internal.tpcengine.GitInfo.getCommitIdAbbrev;

/**
 * Tests how fast the Reactor can schedule tasks.
 */
public class SchedulingBenchmarkCli {
    private final OptionParser parser = new OptionParser();

    private final OptionSpec<String> runtimeSpec = parser
            .accepts("runtime", "The duration of the test, e.g. 60s")
            .withRequiredArg().ofType(String.class)
            .defaultsTo("60s");

    private final OptionSpec<String> reactorTypeSpec = parser
            .accepts("reactortype", "The type of reactor.  Available options: iouring or nio.")
            .withRequiredArg().ofType(String.class)
            .defaultsTo("nio");

    private final OptionSpec<Integer> reactorCntSpec = parser
            .accepts("reactorCnt", "The number of reactors.")
            .withRequiredArg().ofType(Integer.class)
            .defaultsTo(1);

    private final OptionSpec<String> affinitySpec = parser
            .accepts("affinity", "The CPU affinity")
            .withRequiredArg().ofType(String.class)
            .defaultsTo(null);

    public static void main(String[] args) throws Exception {
        SchedulingBenchmarkCli cli = new SchedulingBenchmarkCli();
        cli.run(args);
    }

    private void run(String[] args) throws Exception {
        System.out.println("SchedulingBenchmark");

        System.out.printf("Version: %s, Commit: %s, Build Time: %s%n",
                getToolsVersion(), getCommitIdAbbrev(), getBuildTime());

        OptionSpec helpSpec = parser.accepts("help", "Shows the help.").forHelp();
        OptionSet options = parser.parse(args);

        if (options.has(helpSpec)) {
            printHelp(parser, System.out);
            return;
        }

        SchedulingBenchmark benchmark = new SchedulingBenchmark();
        String runtime = options.valueOf(runtimeSpec);
        if (!runtime.endsWith("s")) {
            System.out.println("Runtime needs to end with 's'");
        }

        String runtimeSec = runtime.substring(0, runtime.length() - 1).trim();
        benchmark.runtimeSeconds = Integer.parseInt(runtimeSec);
        benchmark.reactorType = ReactorType.fromString(options.valueOf(reactorTypeSpec));
        benchmark.reactorCnt = options.valueOf(reactorCntSpec);
        benchmark.affinity = options.valueOf(affinitySpec);
        benchmark.run();
        System.exit(0);
    }
}
