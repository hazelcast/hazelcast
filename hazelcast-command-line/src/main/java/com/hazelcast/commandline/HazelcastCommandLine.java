/*
 * Copyright 2020 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.hazelcast.commandline;

import picocli.CommandLine;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
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
class HazelcastCommandLine
        extends AbstractCommandLine {

    HazelcastCommandLine(PrintStream out, PrintStream err, ProcessExecutor processExecutor) {
        super(out, err, processExecutor);
    }

    public static void main(String[] args)
            throws IOException {
        runCommandLine(args);
    }

    private static void runCommandLine(String[] args)
            throws IOException {
        PrintStream out = System.out;
        PrintStream err = System.err;
        ProcessExecutor processExecutor = new ProcessExecutor();
        CommandLine cmd = new CommandLine(new HazelcastCommandLine(out, err, processExecutor))
                .addSubcommand("mc", new ManagementCenterCommandLine(out, err, processExecutor))
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
                    + "classpath (Use ',' to separate multiple paths). You can add jars, classes, or the directories that contain classes/jars.")
                    String[] additionalClassPath,
            @Option(names = {"-J", "--JAVA_OPTS"}, paramLabel = "<option>", parameterConsumer = JavaOptionsConsumer.class,
                    split = ",", description = "Specify additional Java <option> (Use ',' to separate multiple options).")
                    List<String> javaOptions,
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

        if (javaOptions != null && javaOptions.size() > 0) {
            addJavaOptionsToArgs(javaOptions, args);
        }

        buildAndStartJavaProcess(args, additionalClassPath);
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

    private void buildAndStartJavaProcess(List<String> parameters, String[] additionalClassPath)
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
        processExecutor.buildAndStart(commandList);
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

}
