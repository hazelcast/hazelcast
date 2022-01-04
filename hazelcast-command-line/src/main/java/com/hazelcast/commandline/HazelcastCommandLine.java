/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import picocli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;

import static com.hazelcast.instance.BuildInfoProvider.getBuildInfo;
import static com.hazelcast.internal.util.StringUtil.isNullOrEmpty;
import static picocli.CommandLine.Command;
import static picocli.CommandLine.Option;

/**
 * Main command class for Hazelcast operations
 */
@Command(name = "hz", description = "Utility for the Hazelcast operations." + "%n%n"
        + "Global options are:%n", versionProvider = VersionProvider.class, mixinStandardHelpOptions = true, sortOptions = false)
class HazelcastCommandLine implements Runnable {

    static final String WORKING_DIRECTORY = System.getProperty("hazelcast.commandline.workingdirectory", ".");
    static final String LOGGING_PROPERTIES_FINE_LEVEL = "/config/hazelcast-fine-level-logging.properties";
    static final String LOGGING_PROPERTIES_FINEST_LEVEL = "/config/hazelcast-finest-level-logging.properties";
    static final String CLASSPATH_SEPARATOR = ":";
    static final int MIN_JAVA_VERSION_FOR_MODULAR_OPTIONS = 9;
    final PrintStream out;
    final PrintStream err;
    @CommandLine.Spec
    CommandLine.Model.CommandSpec spec;
    ProcessExecutor processExecutor;

    HazelcastCommandLine(PrintStream out, PrintStream err, ProcessExecutor processExecutor) {
        this.out = out;
        this.err = err;
        this.processExecutor = processExecutor;
    }

    public static void main(String[] args) throws IOException {
        runCommandLine(args);
    }

    private static void runCommandLine(String[] args)
            throws IOException {
        PrintStream out = System.out;
        PrintStream err = System.err;
        ProcessExecutor processExecutor = new ProcessExecutor();
        CommandLine cmd = new CommandLine(new HazelcastCommandLine(out, err, processExecutor))
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

    @Override
    public void run() {
    }

    @Command(description = "Starts a new Hazelcast member", mixinStandardHelpOptions = true, sortOptions = false)
    void start(
            @Option(names = {"-c", "--config"}, paramLabel = "<file>", description = "Use <file> for Hazelcast configuration. "
                    + "Accepted formats are XML and YAML. ")
                    String configFilePath,
            @Option(names = {"-p", "--port"}, paramLabel = "<port>",
                    description = "Bind to the specified <port>. Please note that if the specified port is in use, "
                            + "it will auto-increment to the first free port. (default: 5701)")
                    String port,
            @Option(names = {"-i", "--interface"}, paramLabel = "<interface>",
                    description = "Bind to the specified <interface>.")
                    String hzInterface,
            @Option(names = {"-j", "--jar"}, paramLabel = "<path>", split = ",", description = "Add <path> to Hazelcast "
                    + "classpath (Use ',' to separate multiple paths). You can add jars, classes, "
                    + "or the directories that contain classes/jars.")
                    String[] additionalClassPath,
            @Option(names = {"-J", "--JAVA_OPTS"}, paramLabel = "<option>", parameterConsumer = JavaOptionsConsumer.class,
                    split = ",", description = "Specify additional Java <option> (Use ',' to separate multiple options).")
                    List<String> javaOptions,
            @Option(names = {"-d", "--daemon"},
                    description = "Starts in daemon mode.")
                    boolean daemon,
            @Option(names = {"-v", "--verbose"},
                    description = "Output with FINE level verbose logging.")
                    boolean verbose,
            @Option(names = {"-vv", "--vverbose"},
                    description = "Output with FINEST level verbose logging.")
                    boolean finestVerbose)
            throws IOException, InterruptedException {
        List<String> args = new ArrayList<>();
        if (!isNullOrEmpty(configFilePath)) {
            args.add("-Dhazelcast.config=" + configFilePath);
        }
        if (!isNullOrEmpty(port)) {
            args.add("-Dnetwork.port=" + port);
        }
        if (!isNullOrEmpty(hzInterface)) {
            args.add("-Dnetwork.interface=" + hzInterface);
        }

        args.add("-Djava.net.preferIPv4Stack=true");
        addLogging(args, verbose, finestVerbose);

        if (javaOptions != null && !javaOptions.isEmpty()) {
            addJavaOptionsToArgs(javaOptions, args);
        }

        buildAndStartJavaProcess(args, additionalClassPath, daemon);
    }

    private void addJavaOptionsToArgs(List<String> javaOptions, List<String> args) {
        Set<String> userProvidedArgs = args.stream()
                .map(arg -> arg.split("=")[0])
                .collect(Collectors.toSet());

        for (String opt : javaOptions) {
            if (!userProvidedArgs.contains(opt.split("=")[0])) {
                args.add(opt);
            }
        }
    }

    private void buildAndStartJavaProcess(List<String> parameters, String[] additionalClassPath, boolean daemon)
            throws IOException, InterruptedException {
        List<String> commandList = new ArrayList<>();
        StringBuilder classpath = new StringBuilder(System.getProperty("java.class.path"));
        if (additionalClassPath != null) {
            for (String path : additionalClassPath) {
                classpath.append(CLASSPATH_SEPARATOR).append(path);
            }
        }
        String path = System.getProperty("java.home") + "/bin/java";
        commandList.add(path);
        commandList.add("-cp");
        commandList.add(classpath.toString());
        commandList.addAll(parameters);
        fixModularJavaOptions(commandList);
        commandList.add(HazelcastMember.class.getName());
        processExecutor.buildAndStart(commandList, getRedirect(daemon), daemon);
    }

    private ProcessBuilder.Redirect getRedirect(boolean daemon) {
        Clock clock = Clock.systemDefaultZone();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd.HH.mm.ss").withZone(clock.getZone());
        String formattedDate = formatter.format(clock.instant());
        String logFile = WORKING_DIRECTORY + "/logs/hazelcast." + formattedDate + ".out";
        if (daemon) {
            err.println("Starting Hazelcast in daemon mode. Standard out and error will be written to " + logFile);
        }
        return daemon ? ProcessBuilder.Redirect.to(new File(logFile)) : ProcessBuilder.Redirect.INHERIT;
    }

    private void fixModularJavaOptions(List<String> commandList) {
        double javaVersion = Double.parseDouble(System.getProperty("java.specification.version"));
        if (javaVersion >= MIN_JAVA_VERSION_FOR_MODULAR_OPTIONS) {
            commandList.add("--add-modules");
            commandList.add("java.se");
            commandList.add("--add-exports");
            commandList.add("java.base/jdk.internal.ref=ALL-UNNAMED");
            commandList.add("--add-opens");
            commandList.add("java.base/java.lang=ALL-UNNAMED");
            commandList.add("--add-opens");
            commandList.add("java.base/java.nio=ALL-UNNAMED");
            commandList.add("--add-opens");
            commandList.add("java.base/sun.nio.ch=ALL-UNNAMED");
            commandList.add("--add-opens");
            commandList.add("java.management/sun.management=ALL-UNNAMED");
            commandList.add("--add-opens");
            commandList.add("jdk.management/com.sun.management.internal=ALL-UNNAMED");
        }
    }

    void addLogging(List<String> args, boolean verbose, boolean finestVerbose) {
        if (verbose) {
            args.add("-Djava.util.logging.config.file=" + WORKING_DIRECTORY + LOGGING_PROPERTIES_FINE_LEVEL);
        }
        if (finestVerbose) {
            args.add("-Djava.util.logging.config.file=" + WORKING_DIRECTORY + LOGGING_PROPERTIES_FINEST_LEVEL);
        }
    }

    /**
     * {@code picocli.CommandLine.IParameterConsumer} implementation to handle Java options.
     * Please see the details <a href=https://github.com/remkop/picocli/issues/1125>here</a>.
     */
    static class JavaOptionsConsumer implements CommandLine.IParameterConsumer {
        public void consumeParameters(Stack<String> args, CommandLine.Model.ArgSpec argSpec,
                                      CommandLine.Model.CommandSpec commandSpec) {
            if (args.isEmpty()) {
                throw new CommandLine.ParameterException(commandSpec.commandLine(),
                        "Missing required parameter for option '--JAVA_OPTS' (<option>)");
            }
            List<String> list = argSpec.getValue();
            if (list == null) {
                list = new ArrayList<>();
                argSpec.setValue(list);
            }
            String arg = args.pop();
            String[] splitArgs = arg.split(argSpec.splitRegex());
            Collections.addAll(list, splitArgs);
        }
    }
}
