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

package com.hazelcast.internal.tpcengine.net;

import com.hazelcast.internal.tpcengine.ReactorType;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import static com.hazelcast.internal.tpcengine.CliUtils.getToolsVersion;
import static com.hazelcast.internal.tpcengine.CliUtils.printHelp;
import static com.hazelcast.internal.tpcengine.GitInfo.getBuildTime;
import static com.hazelcast.internal.tpcengine.GitInfo.getCommitIdAbbrev;

public class NetworkBenchmarkCli {

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

    private final OptionSpec<Integer> serverReactorsSpec = parser
            .accepts("serverReactors", "The number of server reactors")
            .withRequiredArg().ofType(Integer.class)
            .defaultsTo(1);

    private final OptionSpec<Integer> clientReactorsSpec = parser
            .accepts("clientReactors", "The number of client reactors")
            .withRequiredArg().ofType(Integer.class)
            .defaultsTo(1);

    public static void main(String[] args) throws Exception {
        NetworkBenchmarkCli cli = new NetworkBenchmarkCli();
        cli.run(args);
    }

    private void run(String[] args) throws Exception {
        System.out.println("Echo Benchmark");

        System.out.printf("Version: %s, Commit: %s, Build Time: %s%n",
                getToolsVersion(), getCommitIdAbbrev(), getBuildTime());

        OptionSpec helpSpec = parser.accepts("help", "Shows the help.").forHelp();
        OptionSet options = parser.parse(args);

        if (options.has(helpSpec)) {
            printHelp(parser, System.out);
            return;
        }

        NetworkBenchmark benchmark = new NetworkBenchmark();
        String runtime = options.valueOf(runtimeSpec);
        if (!runtime.endsWith("s")) {
            System.out.println("Runtime needs to end with 's'");
        }

        String runtimeSec = runtime.substring(0, runtime.length() - 1).trim();
        benchmark.runtimeSeconds = Integer.parseInt(runtimeSec);
        benchmark.spin = false;
        benchmark.reactorType = ReactorType.fromString(options.valueOf(ioengineSpec));
        benchmark.serverReactorCount = options.valueOf(serverReactorsSpec);
        benchmark.clientReactorCount = options.valueOf(clientReactorsSpec);
        benchmark.run();
        System.exit(0);
    }
}
