/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.commandline;

import com.hazelcast.core.server.HazelcastMemberStarter;
import com.hazelcast.jet.function.RunnableEx;
import picocli.CommandLine;

import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;

import static com.hazelcast.instance.BuildInfoProvider.getBuildInfo;
import static com.hazelcast.internal.util.StringUtil.isNullOrEmpty;
import static picocli.CommandLine.Command;
import static picocli.CommandLine.Option;

/**
 * Main command class for Hazelcast operations
 */
@Command(name = "hz", description = "Command line utility to start Hazelcast server process."
        + "%n%nGlobal options are:%n",
        versionProvider = VersionProvider.class, mixinStandardHelpOptions = true, sortOptions = false)
public class HazelcastServerCommandLine {

    private final RunnableEx start;

    public HazelcastServerCommandLine() {
        start = () -> HazelcastMemberStarter.main(new String[]{});
    }

    HazelcastServerCommandLine(RunnableEx start) {
        this.start = start;
    }

    public static void main(String[] args) {
        runCommandLine(args);
    }

    private static void runCommandLine(String[] args) {
        PrintStream out = System.out;
        PrintStream err = System.err;
        CommandLine cmd = new CommandLine(new HazelcastServerCommandLine())
                .setOut(createPrintWriter(out))
                .setErr(createPrintWriter(err))
                .setTrimQuotes(true)
                .setExecutionExceptionHandler(new ExceptionHandler());
        cmd.execute(args);

        String version = getBuildInfo().getVersion();
        cmd.getCommandSpec().usageMessage().header("Hazelcast " + version);
        if (args.length == 0) {
            cmd.usage(out);
        }
    }

    private static PrintWriter createPrintWriter(PrintStream printStream) {
        return new PrintWriter(new OutputStreamWriter(printStream, StandardCharsets.UTF_8));
    }

    @Command(description = "Starts a new Hazelcast member", mixinStandardHelpOptions = true, sortOptions = false)
    void start(
            @Option(names = {"-c", "--config"}, paramLabel = "<file>", description = "Use <file> for Hazelcast "
                    + "configuration. "
                    + "Accepted formats are XML and YAML. ")
                    String configFilePath,
            @Option(names = {"-p", "--port"}, paramLabel = "<port>",
                    description = "Bind to the specified <port>. Please note that if the specified port is in use, "
                            + "it will auto-increment to the first free port. (default: 5701)")
                    String port,
            @Option(names = {"-i", "--interface"}, paramLabel = "<interface>",
                    description = "Bind to the specified <interface>.")
                    String hzInterface) {
        if (!isNullOrEmpty(configFilePath)) {
            System.setProperty("hazelcast.config", configFilePath);
        }
        if (!isNullOrEmpty(port)) {
            System.setProperty("hz.network.port.port", port);
        }
        if (!isNullOrEmpty(hzInterface)) {
            System.setProperty("hz.network.interfaces.enabled", "true");
            System.setProperty("hz.socket.bind.any", "false");
            System.setProperty("hz.network.interfaces.interfaces.interface1", hzInterface);
        }

        start.run();
    }

}
