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

import com.hazelcast.core.server.HazelcastMemberStarter;
import com.hazelcast.internal.util.OsHelper;
import org.apache.logging.log4j.Level;
import picocli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.lang.ProcessBuilder.Redirect;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
public class HazelcastServerCommandLine {

    static final String CLASSPATH_SEPARATOR = ":";
    static final int MIN_JAVA_VERSION_FOR_MODULAR_OPTIONS = 9;
    private static final File NULL_FILE = new File(OsHelper.isWindows() ? "NUL" : "/dev/null");
    private final String workingDirectory;
    private final PrintStream err;
    private final ProcessExecutor processExecutor;

    HazelcastServerCommandLine(PrintStream err, ProcessExecutor processExecutor) {
        this(err, processExecutor, System::getenv);
    }

    /**
     * Just for testing
     */
    HazelcastServerCommandLine(PrintStream err, ProcessExecutor processExecutor, EnvVariableProvider envVariableProvider) {
        this.err = err;
        this.processExecutor = processExecutor;
        this.workingDirectory = envVariableProvider.getVariable("HAZELCAST_HOME");
    }

    public static void main(String[] args) {
        runCommandLine(args);
    }

    private static void runCommandLine(String[] args) {
        PrintStream out = System.out;
        PrintStream err = System.err;
        ProcessExecutor processExecutor = new ProcessExecutor();
        CommandLine cmd = new CommandLine(new HazelcastServerCommandLine(err, processExecutor))
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
            args.add("-Dhz.network.port.port=" + port);
        }
        if (!isNullOrEmpty(hzInterface)) {
            args.add("-Dhz.network.interfaces.enabled=true");
            args.add("-Dhz.socket.bind.any=false");
            args.add("-Dhz.network.interfaces.interfaces.interface1=" + hzInterface);
        }

        Map<String, String> environment = new HashMap<>();
        environment.putAll(setupLoggingLevel(verbose, finestVerbose));

        if (javaOptions != null && !javaOptions.isEmpty()) {
            addJavaOptionsToArgs(javaOptions, args);
        }

        buildAndStartJavaProcess(args, environment, additionalClassPath, daemon);
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

    private void buildAndStartJavaProcess(List<String> parameters, Map<String, String> environment,
                                          String[] additionalClassPath, boolean daemon)
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
        commandList.add(HazelcastMemberStarter.class.getName());
        processExecutor.buildAndStart(commandList, environment, getRedirectOutput(daemon), getRedirectError(daemon), daemon);
    }

    private Redirect getRedirectOutput(boolean daemon) {
        if (daemon) {
            err.println("Starting Hazelcast in daemon mode. Standard out is disabled, use logger configuration instead");
        }
        return daemon ? Redirect.to(NULL_FILE) : Redirect.INHERIT;
    }

    private Redirect getRedirectError(boolean daemon) {
        Clock clock = Clock.systemDefaultZone();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd.HH.mm.ss").withZone(clock.getZone());
        String formattedDate = formatter.format(clock.instant());
        String logFile = workingDirectory + "/logs/hazelcast." + formattedDate + ".err";
        if (daemon) {
            err.println("Error output will be written to " + logFile);
        }
        return daemon ? Redirect.to(new File(logFile)) : Redirect.INHERIT;
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
            commandList.add("java.base/sun.nio.ch=ALL-UNNAMED");
            commandList.add("--add-opens");
            commandList.add("java.management/sun.management=ALL-UNNAMED");
            commandList.add("--add-opens");
            commandList.add("jdk.management/com.sun.management.internal=ALL-UNNAMED");
        }
    }

    private static Map<String, String> setupLoggingLevel(boolean verbose, boolean finestVerbose) {
        Map<String, String> envs = new HashMap<>();
        if (verbose) {
            envs.put("LOGGING_LEVEL", Level.DEBUG.name());
        }
        if (finestVerbose) {
            envs.put("LOGGING_LEVEL", Level.TRACE.name());
        }
        return envs;
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

    @FunctionalInterface
    interface EnvVariableProvider {
        String getVariable(String name);
    }
}
