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

package com.hazelcast.jet.server;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.client.config.YamlClientConfigBuilder;
import com.hazelcast.client.impl.ClientDelegatingFuture;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.management.MCClusterMetadata;
import com.hazelcast.client.impl.protocol.codec.MCGetClusterMetadataCodec;
import com.hazelcast.client.impl.spi.ClientClusterService;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.cluster.Cluster;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.ConsumerEx;
import com.hazelcast.instance.BuildInfo;
import com.hazelcast.instance.impl.HazelcastBootstrap;
import com.hazelcast.internal.util.FutureUtil;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.JobStateSnapshot;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.core.JobNotFoundException;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.impl.JetClientInstanceImpl;
import com.hazelcast.jet.impl.JobSummary;
import com.hazelcast.jet.server.HazelcastCommandLine.HazelcastVersionProvider;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlColumnMetadata;
import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlRowMetadata;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.jline.reader.EOFError;
import org.jline.reader.EndOfFileException;
import org.jline.reader.History;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.ParsedLine;
import org.jline.reader.Parser;
import org.jline.reader.SyntaxError;
import org.jline.reader.UserInterruptException;
import org.jline.reader.impl.DefaultParser;
import org.jline.terminal.Terminal;
import org.jline.terminal.Terminal.Signal;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;
import org.jline.utils.InfoCmp;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.DefaultExceptionHandler;
import picocli.CommandLine.ExecutionException;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Help.Visibility;
import picocli.CommandLine.HelpCommand;
import picocli.CommandLine.ITypeConverter;
import picocli.CommandLine.IVersionProvider;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ParseResult;
import picocli.CommandLine.RunAll;

import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.LogManager;

import static com.hazelcast.instance.BuildInfoProvider.getBuildInfo;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.jet.Util.idToString;
import static com.hazelcast.jet.impl.util.Util.toLocalDateTime;
import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static java.util.Collections.emptyList;
import static java.util.concurrent.TimeUnit.SECONDS;

@SuppressWarnings({"unused", "MismatchedQueryAndUpdateOfCollection"})
@Command(
        name = "hazelcast",
        description = "Utility to perform operations on a Hazelcast cluster.%n" +
                "By default it uses the file config/hazelcast-client.yaml to configure the client connection." +
                "%n%n" +
                "Global options are:%n",
        versionProvider = HazelcastVersionProvider.class,
        mixinStandardHelpOptions = true,
        sortOptions = false,
        subcommands = {HelpCommand.class}
)
public class HazelcastCommandLine implements Runnable {

    private static final int MAX_STR_LENGTH = 24;
    private static final int WAIT_INTERVAL_MILLIS = 100;
    private static final int PRIMARY_COLOR = AttributedStyle.YELLOW;
    private static final int SECONDARY_COLOR = 12;

    private final Function<ClientConfig, HazelcastInstance> hzClientFn;
    private final PrintStream out;
    private final PrintStream err;

    @Option(names = {"-f", "--config"},
            description = "Optional path to a client config XML/YAML file." +
                    " The default is to use config/hazelcast-client.yaml.",
            order = 0
    )
    private File config;

    @Option(names = {"-a", "--addresses"},
            split = ",",
            arity = "1..*",
            paramLabel = "<hostname>:<port>",
            description = "[DEPRECATED] Optional comma-separated list of Jet node addresses in the format" +
                    " <hostname>:<port>, if you want to connect to a cluster other than the" +
                    " one configured in the configuration file. Use --targets instead.",
            order = 1
    )
    private List<String> addresses;

    @Option(names = {"-n", "--cluster-name"},
            description = "[DEPRECATED] The cluster name to use when connecting to the cluster " +
                    "specified by the <addresses> parameter. Use --targets instead.",
            defaultValue = "dev",
            showDefaultValue = Visibility.ALWAYS,
            order = 2
    )
    private String clusterName;

    @Mixin(name = "targets")
    private TargetsMixin targetsMixin;

    @Mixin(name = "verbosity")
    private Verbosity verbosity;

    public HazelcastCommandLine(Function<ClientConfig, HazelcastInstance> hzClientFn, PrintStream out, PrintStream err) {
        this.hzClientFn = hzClientFn;
        this.out = out;
        this.err = err;
    }

    public static void main(String[] args) {
        runCommandLine(
                config -> (HazelcastInstance) HazelcastClient.newHazelcastClient(config).getJet(),
                System.out,
                System.err,
                true,
                args
        );
    }

    @Override
    public void run() {
        // top-level command, do nothing
    }

    @Command(description = "Starts the SQL shell [BETA]")
    public void sql(@Mixin(name = "verbosity") Verbosity verbosity,
                    @Mixin(name = "targets") TargetsMixin targets
    ) {
        runWithJet(targets, verbosity, true, jet -> {
            LineReader reader = LineReaderBuilder.builder().parser(new MultilineParser())
                    .variable(LineReader.SECONDARY_PROMPT_PATTERN, new AttributedStringBuilder()
                            .style(AttributedStyle.BOLD.foreground(SECONDARY_COLOR)).append("%M%P > ").toAnsi())
                    .variable(LineReader.INDENTATION, 2)
                    .option(LineReader.Option.DISABLE_EVENT_EXPANSION, true)
                    .appName("hazelcast-sql")
                    .build();

            AtomicReference<SqlResult> activeSqlResult = new AtomicReference<>();
            reader.getTerminal().handle(Signal.INT, signal -> {
                SqlResult r = activeSqlResult.get();
                if (r != null) {
                    r.close();
                }
            });

            PrintWriter writer = reader.getTerminal().writer();
            writer.println(sqlStartingPrompt(jet));
            writer.flush();

            for (; ; ) {
                String command;
                try {
                    command = reader.readLine(new AttributedStringBuilder()
                            .style(AttributedStyle.DEFAULT.foreground(SECONDARY_COLOR))
                            .append("sql> ").toAnsi()).trim();
                } catch (EndOfFileException | IOError e) {
                    // Ctrl+D, and kill signals result in exit
                    writer.println(SQLCliConstants.EXIT_PROMPT);
                    writer.flush();
                    break;
                } catch (UserInterruptException e) {
                    // Ctrl+C cancels the not-yet-submitted query
                    continue;
                }

                command = command.trim();
                if (command.length() > 0 && command.charAt(command.length() - 1) == ';') {
                    command = command.substring(0, command.length() - 1).trim();
                } else if (command.lastIndexOf(";") >= 0) {
                    String errorPrompt = new AttributedStringBuilder()
                            .style(AttributedStyle.BOLD.foreground(PRIMARY_COLOR))
                            .append("There are non-whitespace characters after the semicolon")
                            .toAnsi();
                    writer.println(errorPrompt);
                    writer.flush();
                    continue;
                }

                if ("".equals(command)) {
                    continue;
                }
                if ("clear".equalsIgnoreCase(command)) {
                    reader.getTerminal().puts(InfoCmp.Capability.clear_screen);
                    continue;
                }
                if ("help".equalsIgnoreCase(command)) {
                    writer.println(helpPrompt(jet));
                    writer.flush();
                    continue;
                }
                if ("history".equalsIgnoreCase(command)) {
                    History hist = reader.getHistory();
                    ListIterator<History.Entry> iterator = hist.iterator();
                    while (iterator.hasNext()) {
                        History.Entry entry = iterator.next();
                        if (iterator.hasNext()) {
                            String entryLine = new AttributedStringBuilder()
                                    .style(AttributedStyle.BOLD.foreground(PRIMARY_COLOR))
                                    .append(String.valueOf(entry.index() + 1))
                                    .append(" - ")
                                    .append(entry.line())
                                    .toAnsi();
                            writer.println(entryLine);
                            writer.flush();
                        } else {
                            // remove the "history" command from the history
                            iterator.remove();
                            hist.resetIndex();
                        }
                    }
                    continue;
                }
                if ("exit".equalsIgnoreCase(command)) {
                    writer.println(SQLCliConstants.EXIT_PROMPT);
                    writer.flush();
                    break;
                }

                executeSqlCmd(jet, command, reader.getTerminal(), activeSqlResult);
            }
        });
    }

    @Command(description = "Submits a job to the cluster")
    public void submit(
            @Mixin(name = "verbosity") Verbosity verbosity,
            @Mixin(name = "targets") TargetsMixin targets,
            @Option(names = {"-s", "--snapshot"},
                    paramLabel = "<snapshot name>",
                    description = "Name of the initial snapshot to start the job from"
            ) String snapshotName,
            @Option(names = {"-n", "--name"},
                    paramLabel = "<name>",
                    description = "Name of the job"
            ) String name,
            @Option(names = {"-c", "--class"},
                    paramLabel = "<class>",
                    description = "Fully qualified name of the main class inside the JAR file"
            ) String mainClass,
            @Parameters(index = "0",
                    paramLabel = "<jar file>",
                    description = "The jar file to submit"
            ) File file,
            @Parameters(index = "1..*",
                    paramLabel = "<arguments>",
                    description = "Arguments to pass to the supplied jar file"
            ) List<String> params
    ) throws Exception {
        if (params == null) {
            params = emptyList();
        }
        this.verbosity.merge(verbosity);
        configureLogging();
        if (!file.exists()) {
            throw new Exception("File " + file + " could not be found.");
        }
        printf("Submitting JAR '%s' with arguments %s", file, params);
        if (name != null) {
            printf("Using job name '%s'", name);
        }
        if (snapshotName != null) {
            printf("Will restore the job from the snapshot with name '%s'", snapshotName);
        }

        targetsMixin.replace(targets);

        HazelcastBootstrap.executeJar(
                () -> getJetClient(false),
                file.getAbsolutePath(), snapshotName, name, mainClass, params);
    }

    @Command(description = "Suspends a running job")
    public void suspend(
            @Mixin(name = "verbosity") Verbosity verbosity,
            @Mixin(name = "targets") TargetsMixin targets,
            @Parameters(index = "0",
                    paramLabel = "<job name or id>",
                    description = "Name of the job to suspend"
            ) String name
    ) {
        runWithJet(targets, verbosity, false, jet -> {
            Job job = getJob(jet, name);
            assertJobRunning(name, job);
            printf("Suspending job %s...", formatJob(job));
            job.suspend();
            waitForJobStatus(job, JobStatus.SUSPENDED);
            println("Job suspended.");
        });
    }

    @Command(
            description = "Cancels a running job"
    )
    public void cancel(
            @Mixin(name = "verbosity") Verbosity verbosity,
            @Mixin(name = "targets") TargetsMixin targets,
            @Parameters(index = "0",
                    paramLabel = "<job name or id>",
                    description = "Name of the job to cancel"
            ) String name
    ) {
        runWithJet(targets, verbosity, false, jet -> {
            Job job = getJob(jet, name);
            assertJobActive(name, job);
            printf("Cancelling job %s", formatJob(job));
            job.cancel();
            waitForJobStatus(job, JobStatus.FAILED);
            println("Job cancelled.");
        });
    }

    @Command(
            name = "save-snapshot",
            description = "Exports a named snapshot from a job and optionally cancels it"
    )
    public void saveSnapshot(
            @Mixin(name = "verbosity") Verbosity verbosity,
            @Mixin(name = "targets") TargetsMixin targets,
            @Parameters(index = "0",
                    paramLabel = "<job name or id>",
                    description = "Name of the job to take the snapshot from")
                    String jobName,
            @Parameters(index = "1",
                    paramLabel = "<snapshot name>",
                    description = "Name of the snapshot")
                    String snapshotName,
            @Option(names = {"-C", "--cancel"},
                    description = "Cancel the job after taking the snapshot")
                    boolean isTerminal
    ) {
        runWithJet(targets, verbosity, false, jet -> {
            Job job = getJob(jet, jobName);
            assertJobActive(jobName, job);
            if (isTerminal) {
                printf("Saving snapshot with name '%s' from job '%s' and cancelling the job...",
                        snapshotName, formatJob(job)
                );
                job.cancelAndExportSnapshot(snapshotName);
                waitForJobStatus(job, JobStatus.FAILED);
            } else {
                printf("Saving snapshot with name '%s' from job '%s'...", snapshotName, formatJob(job));
                job.exportSnapshot(snapshotName);
            }
            printf("Exported snapshot '%s'.", snapshotName);
        });
    }

    @Command(
            name = "delete-snapshot",
            description = "Deletes a named snapshot"
    )
    public void deleteSnapshot(
            @Mixin(name = "verbosity") Verbosity verbosity,
            @Mixin(name = "targets") TargetsMixin targets,
            @Parameters(index = "0",
                    paramLabel = "<snapshot name>",
                    description = "Name of the snapshot")
                    String snapshotName
    ) {
        runWithJet(targets, verbosity, false, jet -> {
            JobStateSnapshot jobStateSnapshot = jet.getJet().getJobStateSnapshot(snapshotName);
            if (jobStateSnapshot == null) {
                throw new JetException(String.format("Didn't find a snapshot named '%s'", snapshotName));
            }
            jobStateSnapshot.destroy();
            printf("Deleted snapshot '%s'.", snapshotName);
        });
    }

    @Command(
            description = "Restarts a running job"
    )
    public void restart(
            @Mixin(name = "verbosity") Verbosity verbosity,
            @Mixin(name = "targets") TargetsMixin targets,
            @Parameters(index = "0",
                    paramLabel = "<job name or id>",
                    description = "Name of the job to restart")
                    String name
    ) {
        runWithJet(targets, verbosity, false, jet -> {
            Job job = getJob(jet, name);
            assertJobRunning(name, job);
            println("Restarting job " + formatJob(job) + "...");
            job.restart();
            waitForJobStatus(job, JobStatus.RUNNING);
            println("Job restarted.");
        });
    }

    @Command(
            description = "Resumes a suspended job"
    )
    public void resume(
            @Mixin(name = "verbosity") Verbosity verbosity,
            @Mixin(name = "targets") TargetsMixin targets,
            @Parameters(index = "0",
                    paramLabel = "<job name or id>",
                    description = "Name of the job to resume")
                    String name
    ) {
        runWithJet(targets, verbosity, false, jet -> {
            Job job = getJob(jet, name);
            if (job.getStatus() != JobStatus.SUSPENDED) {
                throw new RuntimeException("Job '" + name + "' is not suspended. Current state: " + job.getStatus());
            }
            println("Resuming job " + formatJob(job) + "...");
            job.resume();
            waitForJobStatus(job, JobStatus.RUNNING);
            println("Job resumed.");
        });
    }

    @Command(
            name = "list-jobs",
            description = "Lists running jobs on the cluster"
    )
    public void listJobs(
            @Mixin(name = "verbosity") Verbosity verbosity,
            @Mixin(name = "targets") TargetsMixin targets,
            @Option(names = {"-a", "--all"},
                    description = "Lists all jobs including completed and failed ones")
                    boolean listAll
    ) {
        runWithJet(targets, verbosity, false, jet -> {
            JetClientInstanceImpl client = (JetClientInstanceImpl) jet.getJet();
            List<JobSummary> summaries = client.getJobSummaryList();
            String format = "%-19s %-18s %-23s %s";
            printf(format, "ID", "STATUS", "SUBMISSION TIME", "NAME");
            summaries.stream()
                    .filter(job -> listAll || isActive(job.getStatus()))
                    .forEach(job -> {
                        String idString = idToString(job.getJobId());
                        String name = job.getName().equals(idString) ? "N/A" : job.getName();
                        printf(format, idString, job.getStatus(), toLocalDateTime(job.getSubmissionTime()), name);
                    });
        });
    }

    @Command(
            name = "list-snapshots",
            description = "Lists exported snapshots on the cluster"
    )
    public void listSnapshots(
            @Mixin(name = "verbosity") Verbosity verbosity,
            @Mixin(name = "targets") TargetsMixin targets,
            @Option(names = {"-F", "--full-job-name"},
                    description = "Don't trim job name to fit, can break layout")
                    boolean fullJobName) {
        runWithJet(targets, verbosity, false, jet -> {
            Collection<JobStateSnapshot> snapshots = jet.getJet().getJobStateSnapshots();
            printf("%-23s %-15s %-24s %s", "TIME", "SIZE (bytes)", "JOB NAME", "SNAPSHOT NAME");
            snapshots.stream()
                    .sorted(Comparator.comparing(JobStateSnapshot::name))
                    .forEach(ss -> {
                        LocalDateTime creationTime = toLocalDateTime(ss.creationTime());
                        String jobName = ss.jobName() == null ? Util.idToString(ss.jobId()) : ss.jobName();
                        if (!fullJobName) {
                            jobName = shorten(jobName);
                        }
                        printf("%-23s %-,15d %-24s %s", creationTime, ss.payloadSize(), jobName, ss.name());
                    });
        });
    }

    @Command(
            description = "Shows current cluster state and information about members"
    )
    public void cluster(
            @Mixin(name = "verbosity") Verbosity verbosity,
            @Mixin(name = "targets") TargetsMixin targets
    ) {
        runWithJet(targets, verbosity, false, jet -> {
            JetClientInstanceImpl client = (JetClientInstanceImpl) jet.getJet();
            HazelcastClientInstanceImpl hazelcastClient = client.getHazelcastClient();
            ClientClusterService clientClusterService = hazelcastClient.getClientClusterService();
            MCClusterMetadata clusterMetadata =
                    FutureUtil.getValue(getClusterMetadata(hazelcastClient, clientClusterService.getMasterMember()));

            Cluster cluster = client.getCluster();

            println("State: " + clusterMetadata.getCurrentState());
            println("Version: " + clusterMetadata.getJetVersion());
            println("Size: " + cluster.getMembers().size());

            println("");

            String format = "%-24s %-19s";
            printf(format, "ADDRESS", "UUID");
            cluster.getMembers().forEach(member -> printf(format, member.getAddress(), member.getUuid()));
        });
    }

    private CompletableFuture<MCClusterMetadata> getClusterMetadata(HazelcastClientInstanceImpl client, Member member) {
        checkNotNull(member);

        ClientInvocation invocation = new ClientInvocation(
                client,
                MCGetClusterMetadataCodec.encodeRequest(),
                null,
                member.getUuid()
        );

        return new ClientDelegatingFuture<>(
                invocation.invoke(),
                client.getSerializationService(),
                clientMessage -> {
                    MCGetClusterMetadataCodec.ResponseParameters response =
                            MCGetClusterMetadataCodec.decodeResponse(clientMessage);

                    MCClusterMetadata metadata = new MCClusterMetadata();
                    metadata.setCurrentState(ClusterState.getById(response.currentState));
                    metadata.setClusterTime(response.clusterTime);
                    metadata.setMemberVersion(response.memberVersion);
                    metadata.setJetVersion(response.jetVersion);
                    return metadata;
                }
        );
    }

    private void runWithJet(TargetsMixin targets, Verbosity verbosity, boolean retryClusterConnectForever,
                            ConsumerEx<HazelcastInstance> consumer) {
        this.targetsMixin.replace(targets);
        this.verbosity.merge(verbosity);
        configureLogging();
        HazelcastInstance jet = getJetClient(retryClusterConnectForever);
        try {
            consumer.accept(jet);
        } finally {
            jet.shutdown();
        }
    }

    private HazelcastInstance getJetClient(boolean retryClusterConnectForever) {
        return uncheckCall(() -> hzClientFn.apply(getClientConfig(retryClusterConnectForever)));
    }

    @SuppressFBWarnings(value = "DLS_DEAD_LOCAL_STORE", justification = "Generates false positive")
    private ClientConfig getClientConfig(boolean retryClusterConnectForever) throws IOException {
        ClientConfig config;
        if (isYaml()) {
            config = new YamlClientConfigBuilder(this.config).build();
        } else if (isConfigFileNotNull()) {
            config = new XmlClientConfigBuilder(this.config).build();
        } else if (addresses != null) {
            // Whole default configuration is ignored if addresses is provided.
            // This doesn't make much sense, but but addresses is deprecated, so will leave as is until it can be
            // removed in next major version
            ClientConfig c = new ClientConfig();
            c.getNetworkConfig().addAddress(addresses.toArray(new String[0]));
            c.setClusterName(clusterName);
            return c;
        } else {
            config = ClientConfig.load();
        }

        if (targetsMixin.getTargets() != null) {
            config.getNetworkConfig().setAddresses(targetsMixin.getAddresses());
            config.setClusterName(targetsMixin.getClusterName());
        }

        if (retryClusterConnectForever) {
            final double expBackoffMultiplier = 1.25;
            final long clusterConnectTimeoutMillis = Long.MAX_VALUE;
            final int maxBackOffMillis = (int) SECONDS.toMillis(15);
            config.getConnectionStrategyConfig().getConnectionRetryConfig()
                    .setClusterConnectTimeoutMillis(clusterConnectTimeoutMillis)
                    .setMultiplier(expBackoffMultiplier)
                    .setMaxBackoffMillis(maxBackOffMillis);
        }
        return config;
    }

    private boolean isYaml() {
        return isConfigFileNotNull() &&
                (config.getPath().endsWith(".yaml") || config.getPath().endsWith(".yml"));
    }

    private boolean isConfigFileNotNull() {
        return config != null;
    }

    private void configureLogging() {
        HazelcastBootstrap.configureLogging();
        Level logLevel = Level.WARNING;
        if (verbosity.isVerbose) {
            println("Verbose mode is on, setting logging level to INFO");
            logLevel = Level.INFO;
        }
        LogManager.getLogManager().getLogger("").setLevel(logLevel);
    }

    private void printf(String format, Object... objects) {
        out.printf(format + "%n", objects);
    }

    private void println(String msg) {
        out.println(msg);
    }

    private void executeSqlCmd(
            HazelcastInstance jet,
            String command,
            Terminal terminal,
            AtomicReference<SqlResult> activeSqlResult
    ) {
        PrintWriter out = terminal.writer();
        try (SqlResult sqlResult = jet.getSql().execute(command)) {
            activeSqlResult.set(sqlResult);

            // if it's a result with an update count, just print it
            if (sqlResult.updateCount() != -1) {
                String message = new AttributedStringBuilder()
                        .style(AttributedStyle.BOLD.foreground(PRIMARY_COLOR))
                        .append("OK")
                        .toAnsi();
                out.println(message);
                return;
            }
            SqlRowMetadata rowMetadata = sqlResult.getRowMetadata();
            int[] colWidths = determineColumnWidths(rowMetadata);
            Alignment[] alignments = determineAlignments(rowMetadata);

            // this is a result with rows. Print the header and rows, watch for concurrent cancellation
            printMetadataInfo(rowMetadata, colWidths, alignments, out);

            int rowCount = 0;
            for (SqlRow row : sqlResult) {
                rowCount++;
                printRow(row, colWidths, alignments, out);
            }

            // bottom line after all the rows
            printSeparatorLine(sqlResult.getRowMetadata().getColumnCount(), colWidths, out);

            String message = new AttributedStringBuilder()
                    .style(AttributedStyle.BOLD.foreground(PRIMARY_COLOR))
                    .append(String.valueOf(rowCount))
                    .append(" row(s) selected")
                    .toAnsi();
            out.println(message);
        } catch (HazelcastSqlException e) {
            // the query failed to execute
            String errorPrompt = new AttributedStringBuilder()
                    .style(AttributedStyle.BOLD.foreground(PRIMARY_COLOR))
                    .append(e.getMessage())
                    .toAnsi();
            out.println(errorPrompt);
        }
    }

    private String sqlStartingPrompt(HazelcastInstance jet) {
        JetClientInstanceImpl client = (JetClientInstanceImpl) jet;
        HazelcastClientInstanceImpl hazelcastClient = client.getHazelcastClient();
        ClientClusterService clientClusterService = hazelcastClient.getClientClusterService();
        MCClusterMetadata clusterMetadata =
                FutureUtil.getValue(getClusterMetadata(hazelcastClient, clientClusterService.getMasterMember()));
        Cluster cluster = client.getCluster();
        Set<Member> members = cluster.getMembers();
        String versionString = "Hazelcast Platform " + clusterMetadata.getMemberVersion();
        return new AttributedStringBuilder()
                .style(AttributedStyle.BOLD.foreground(PRIMARY_COLOR))
                .append("Connected to ")
                .append(versionString)
                .append(" at ")
                .append(members.iterator().next().getAddress().toString())
                .append(" (+")
                .append(String.valueOf(members.size() - 1))
                .append(" more)\n")
                .append("Type 'help' for instructions")
                .toAnsi();
    }

    private String helpPrompt(HazelcastInstance jet) {
        JetClientInstanceImpl client = (JetClientInstanceImpl) jet;
        HazelcastClientInstanceImpl hazelcastClient = client.getHazelcastClient();
        ClientClusterService clientClusterService = hazelcastClient.getClientClusterService();
        AttributedStringBuilder builder = new AttributedStringBuilder()
                .style(AttributedStyle.BOLD.foreground(PRIMARY_COLOR))
                .append("Available Commands:\n")
                .append("  clear    Clears the terminal screen\n")
                .append("  exit     Exits from the SQL console\n")
                .append("  help     Provides information about available commands\n")
                .append("  history  Shows the command history of the current session\n")
                .append("Hints:\n")
                .append("  Semicolon completes a query\n")
                .append("  Ctrl+C cancels a streaming query\n")
                .append("  https://docs.hazelcast.com/");
        return builder.toAnsi();
    }

    private enum Alignment {
        LEFT, RIGHT
    }

    static void runCommandLine(
            Function<ClientConfig, HazelcastInstance> jetClientFn,
            PrintStream out, PrintStream err,
            boolean shouldExit,
            String[] args
    ) {
        CommandLine cmd = new CommandLine(new HazelcastCommandLine(jetClientFn, out, err));
        cmd.getSubcommands().get("submit").setStopAtPositional(true);

        String version = getBuildInfo().getVersion();
        cmd.getCommandSpec().usageMessage().header("Hazelcast " + version);

        if (args.length == 0) {
            cmd.usage(out);
        } else {
            DefaultExceptionHandler<List<Object>> excHandler =
                    new ExceptionHandler<List<Object>>().useErr(err).useAnsi(Ansi.AUTO);
            if (shouldExit) {
                excHandler.andExit(1);
            }
            List<Object> parsed = cmd.parseWithHandlers(new RunAll().useOut(out).useAnsi(Ansi.AUTO), excHandler, args);
            // only top command was executed
            if (parsed != null && parsed.size() == 1) {
                cmd.usage(out);
            }
        }
    }

    private static Job getJob(HazelcastInstance jet, String nameOrId) {
        Job job = jet.getJet().getJob(nameOrId);
        if (job == null) {
            job = jet.getJet().getJob(Util.idFromString(nameOrId));
            if (job == null) {
                throw new JobNotFoundException("No job with name or id '" + nameOrId + "' was found");
            }
        }
        return job;
    }

    /**
     * If name is longer than the {@code length}, shorten it and add an
     * asterisk so that the resulting string has {@code length} length.
     */
    private static String shorten(String name) {
        if (name.length() <= MAX_STR_LENGTH) {
            return name;
        }
        return name.substring(0, MAX_STR_LENGTH - 1) + "*";
    }

    private static String formatJob(Job job) {
        return "id=" + idToString(job.getId())
                + ", name=" + job.getName()
                + ", submissionTime=" + toLocalDateTime(job.getSubmissionTime());
    }

    private static void assertJobActive(String name, Job job) {
        if (!isActive(job.getStatus())) {
            throw new RuntimeException("Job '" + name + "' is not active. Current state: " + job.getStatus());
        }
    }

    private static void assertJobRunning(String name, Job job) {
        if (job.getStatus() != JobStatus.RUNNING) {
            throw new RuntimeException("Job '" + name + "' is not running. Current state: " + job.getStatus());
        }
    }

    private static void waitForJobStatus(Job job, JobStatus status) {
        while (job.getStatus() != status) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(WAIT_INTERVAL_MILLIS));
        }
    }

    private static boolean isActive(JobStatus status) {
        return status != JobStatus.FAILED && status != JobStatus.COMPLETED;
    }

    private static int[] determineColumnWidths(SqlRowMetadata metadata) {
        int colCount = metadata.getColumnCount();
        int[] colWidths = new int[colCount];
        for (int i = 0; i < colCount; i++) {
            SqlColumnMetadata colMetadata = metadata.getColumn(i);
            SqlColumnType type = colMetadata.getType();
            String colName = colMetadata.getName();
            switch (type) {
                case BOOLEAN:
                    colWidths[i] = determineColumnWidth(colName, SQLCliConstants.BOOLEAN_FORMAT_LENGTH);
                    break;
                case DATE:
                    colWidths[i] = determineColumnWidth(colName, SQLCliConstants.DATE_FORMAT_LENGTH);
                    break;
                case TIMESTAMP_WITH_TIME_ZONE:
                    colWidths[i] = determineColumnWidth(colName, SQLCliConstants.TIMESTAMP_WITH_TIME_ZONE_FORMAT_LENGTH);
                    break;
                case DECIMAL:
                    colWidths[i] = determineColumnWidth(colName, SQLCliConstants.DECIMAL_FORMAT_LENGTH);
                    break;
                case REAL:
                    colWidths[i] = determineColumnWidth(colName, SQLCliConstants.REAL_FORMAT_LENGTH);
                    break;
                case DOUBLE:
                    colWidths[i] = determineColumnWidth(colName, SQLCliConstants.DOUBLE_FORMAT_LENGTH);
                    break;
                case INTEGER:
                    colWidths[i] = determineColumnWidth(colName, SQLCliConstants.INTEGER_FORMAT_LENGTH);
                    break;
                case NULL:
                    colWidths[i] = determineColumnWidth(colName, SQLCliConstants.NULL_FORMAT_LENGTH);
                    break;
                case TINYINT:
                    colWidths[i] = determineColumnWidth(colName, SQLCliConstants.TINYINT_FORMAT_LENGTH);
                    break;
                case SMALLINT:
                    colWidths[i] = determineColumnWidth(colName, SQLCliConstants.SMALLINT_FORMAT_LENGTH);
                    break;
                case TIMESTAMP:
                    colWidths[i] = determineColumnWidth(colName, SQLCliConstants.TIMESTAMP_FORMAT_LENGTH);
                    break;
                case BIGINT:
                    colWidths[i] = determineColumnWidth(colName, SQLCliConstants.BIGINT_FORMAT_LENGTH);
                    break;
                case VARCHAR:
                    colWidths[i] = determineColumnWidth(colName, SQLCliConstants.VARCHAR_FORMAT_LENGTH);
                    break;
                case OBJECT:
                    colWidths[i] = determineColumnWidth(colName, SQLCliConstants.OBJECT_FORMAT_LENGTH);
                    break;
                default:
                    throw new UnsupportedOperationException(type.toString());
            }
        }
        return colWidths;
    }

    private static int determineColumnWidth(String header, int typeLength) {
        return Math.max(Math.min(header.length(), SQLCliConstants.VARCHAR_FORMAT_LENGTH), typeLength);
    }

    private static Alignment[] determineAlignments(SqlRowMetadata metadata) {
        int colCount = metadata.getColumnCount();
        Alignment[] alignments = new Alignment[colCount];
        for (int i = 0; i < colCount; i++) {
            SqlColumnMetadata colMetadata = metadata.getColumn(i);
            SqlColumnType type = colMetadata.getType();
            String colName = colMetadata.getName();
            switch (type) {
                case BIGINT:
                case DECIMAL:
                case DOUBLE:
                case INTEGER:
                case REAL:
                case SMALLINT:
                case TINYINT:
                    alignments[i] = Alignment.RIGHT;
                    break;
                case BOOLEAN:
                case DATE:
                case NULL:
                case OBJECT:
                case TIMESTAMP:
                case VARCHAR:
                case TIMESTAMP_WITH_TIME_ZONE:
                default:
                    alignments[i] = Alignment.LEFT;
            }
        }
        return alignments;
    }

    private static void printMetadataInfo(SqlRowMetadata metadata, int[] colWidths,
                                          Alignment[] alignments, PrintWriter out) {
        int colCount = metadata.getColumnCount();
        printSeparatorLine(colCount, colWidths, out);
        AttributedStringBuilder builder = new AttributedStringBuilder()
                .style(AttributedStyle.BOLD.foreground(SECONDARY_COLOR));
        builder.append("|");
        for (int i = 0; i < colCount; i++) {
            String colName = metadata.getColumn(i).getName();
            colName = sanitize(colName, colWidths[i]);
            builder.style(AttributedStyle.BOLD.foreground(PRIMARY_COLOR));
            appendAligned(colWidths[i], colName, alignments[i], builder);
            builder.style(AttributedStyle.BOLD.foreground(SECONDARY_COLOR));
            builder.append('|');
        }
        out.println(builder.toAnsi());
        printSeparatorLine(colCount, colWidths, out);
        out.flush();
    }

    private static void printRow(SqlRow row, int[] colWidths, Alignment[] alignments, PrintWriter out) {
        AttributedStringBuilder builder = new AttributedStringBuilder()
                .style(AttributedStyle.BOLD.foreground(SECONDARY_COLOR));
        builder.append("|");
        int columnCount = row.getMetadata().getColumnCount();
        for (int i = 0; i < columnCount; i++) {
            String colString = row.getObject(i) != null ? sanitize(row.getObject(i).toString(), colWidths[i]) : "NULL";
            builder.style(AttributedStyle.BOLD.foreground(PRIMARY_COLOR));
            appendAligned(colWidths[i], colString, alignments[i], builder);
            builder.style(AttributedStyle.BOLD.foreground(SECONDARY_COLOR));
            builder.append('|');
        }
        out.println(builder.toAnsi());
        out.flush();
    }

    private static String sanitize(String s, int width) {
        s = s.replace("\n", "\\n");
        if (s.length() > width) {
            s = s.substring(0, width - 1) + "\u2026";
        }
        return s;
    }

    private static void appendAligned(int width, String s, Alignment alignment, AttributedStringBuilder builder) {
        int padding = width - s.length();
        assert padding >= 0;

        if (alignment == Alignment.RIGHT) {
            appendPadding(builder, padding, ' ');
        }
        builder.append(s);
        if (alignment == Alignment.LEFT) {
            appendPadding(builder, padding, ' ');
        }
    }

    private static void appendPadding(AttributedStringBuilder builder, int length, char paddingChar) {
        for (int i = 0; i < length; i++) {
            builder.append(paddingChar);
        }
    }

    private static void printSeparatorLine(int colSize, int[] colWidths, PrintWriter out) {
        AttributedStringBuilder builder = new AttributedStringBuilder()
                .style(AttributedStyle.BOLD.foreground(SECONDARY_COLOR));
        builder.append('+');
        for (int i = 0; i < colSize; i++) {
            appendPadding(builder, colWidths[i], '-');
            builder.append('+');
        }
        out.println(builder.toAnsi());
    }

    public static class HazelcastVersionProvider implements IVersionProvider {

        @Override
        public String[] getVersion() {
            BuildInfo buildInfo = getBuildInfo();
            return new String[]{
                    "Hazelcast " + buildInfo.getVersion(),
                    "Revision " + buildInfo.getRevision(),
                    "Build " + buildInfo.getBuild()
            };
        }
    }

    public static class Verbosity {

        @Option(names = {"-v", "--verbosity"},
                description = {"Show logs from Jet client and full stack trace of errors"},
                order = 1
        )
        private boolean isVerbose;

        void merge(Verbosity other) {
            isVerbose |= other.isVerbose;
        }
    }

    public static class TargetsMixin {

        @Option(names = {"-t", "--targets"},
                description = "The cluster name and addresses to use if you want to connect to a "
                        + "cluster other than the one configured in the configuration file. "
                        + "At least one address is required. The cluster name is optional.",
                paramLabel = "[<cluster-name>@]<hostname>:<port>[,<hostname>:<port>]",
                converter = TargetsMixin.Converter.class)
        private Targets targets;

        private Targets getTargets() {
            return targets;
        }

        public String getClusterName() {
            return targets.clusterName;
        }

        public List<String> getAddresses() {
            return targets.addresses;
        }

        public void replace(TargetsMixin targets) {
            if (targets.getTargets() != null) {
                this.targets = targets.getTargets();
            }
        }

        public static class Targets {
            private String clusterName = "dev";
            private List<String> addresses = Collections.emptyList();
        }

        public static class Converter implements ITypeConverter<TargetsMixin.Targets> {
            @Override
            public Targets convert(String value) {
                Targets targets = new Targets();
                if (value == null) {
                    return targets;
                }

                String[] values;
                if (value.contains("@")) {
                    values = value.split("@");
                    targets.clusterName = values[0];
                    targets.addresses = Arrays.asList(values[1].split(","));
                } else {
                    targets.addresses = Arrays.asList(value.split(","));
                }

                return targets;
            }
        }
    }

    static class ExceptionHandler<R> extends DefaultExceptionHandler<R> {
        @Override
        public R handleExecutionException(ExecutionException ex, ParseResult parseResult) {
            // find top level command
            CommandLine cmdLine = ex.getCommandLine();
            while (cmdLine.getParent() != null) {
                cmdLine = cmdLine.getParent();
            }
            HazelcastCommandLine jetCmd = cmdLine.getCommand();
            if (jetCmd.verbosity.isVerbose) {
                ex.printStackTrace(err());
            } else {
                err().println("ERROR: " + peel(ex.getCause()).getMessage());
                err().println();
                err().println("To see the full stack trace, re-run with the -v/--verbosity option");
            }
            if (hasExitCode()) {
                exit(exitCode());
            }
            throw ex;
        }

        static Throwable peel(Throwable e) {
            if (e instanceof InvocationTargetException) {
                return e.getCause();
            }
            return e;
        }
    }

    /**
     * A parser for SQL-like inputs. Commands are terminated with a semicolon.
     * It is adapted from
     * <a href="https://github.com/julianhyde/sqlline/blob/master/src/main/java/sqlline/SqlLineParser.java">
     * SqlLineParser</a>
     * which is licensed under the BSD-3-Clause License.
     */
    private static final class MultilineParser extends DefaultParser {

        private MultilineParser() {
        }

        @Override
        public ParsedLine parse(String line, int cursor, Parser.ParseContext context) throws SyntaxError {
            super.setQuoteChars(new char[]{'\'', '"'});
            super.setEofOnUnclosedQuote(true);
            stateCheck(line, cursor);
            return new ArgumentList(line, Collections.emptyList(), -1, -1,
                    cursor, "'", -1, -1);
        }

        private void stateCheck(String line, int cursor) {
            boolean containsNonWhitespaceData = false;
            int quoteStart = -1;
            int oneLineCommentStart = -1;
            int multiLineCommentStart = -1;
            int lastSemicolonIdx = -1;
            for (int i = 0; i < line.length(); i++) {
                // If a one line comment, a multiline comment or a quote is not started before,
                // check if the character we're on is a quote character
                if (oneLineCommentStart == -1
                        && multiLineCommentStart == -1
                        && quoteStart < 0
                        && (isQuoteChar(line, i))) {
                    // Start a quote block
                    quoteStart = i;
                    containsNonWhitespaceData = true;
                } else {
                    char currentChar = line.charAt(i);
                    if (quoteStart >= 0) {
                        // In a quote block
                        if ((line.charAt(quoteStart) == currentChar) && !isEscaped(line, i)) {
                            // End the block; arg could be empty, but that's fine
                            quoteStart = -1;
                        }
                    } else if (oneLineCommentStart == -1 &&
                            line.regionMatches(i, "/*", 0, "/*".length())) {
                        // Enter the multiline comment block
                        multiLineCommentStart = i;
                        containsNonWhitespaceData = true;
                    } else if (multiLineCommentStart >= 0) {
                        // In a multiline comment block
                        if (i - multiLineCommentStart > 2 &&
                                line.regionMatches(i - 1, "*/", 0, "*/".length())) {
                            // End the multiline block
                            multiLineCommentStart = -1;
                        }
                    } else if (oneLineCommentStart == -1 &&
                            line.regionMatches(i, "--", 0, "--".length())) {
                        // Enter the one line comment block
                        oneLineCommentStart = i;
                        containsNonWhitespaceData = true;
                    } else if (oneLineCommentStart >= 0) {
                        // In a one line comment
                        if (currentChar == '\n') {
                            // End the one line comment block
                            oneLineCommentStart = -1;
                        }
                    } else {
                        // Not in a quote or comment block
                        if (currentChar == ';') {
                            lastSemicolonIdx = i;
                        } else if (!Character.isWhitespace(currentChar)) {
                            containsNonWhitespaceData = true;
                        }
                    }
                }
            }

            if (SQLCliConstants.COMMAND_SET.contains(line.trim().toLowerCase(Locale.US))) {
                return;
            }
            // These EOFError exceptions are captured in LineReader's
            // readLine() method and it points out that the command
            // being written to console is not finalized and command
            // won't be read
            if (isEofOnEscapedNewLine() && isEscapeChar(line, line.length() - 1)) {
                throw new EOFError(-1, cursor, "Escaped new line");
            }

            if (isEofOnUnclosedQuote() && quoteStart >= 0) {
                throw new EOFError(-1, quoteStart, "Missing closing quote",
                        line.charAt(quoteStart) == '\'' ? "quote" : "dquote");
            }

            if (oneLineCommentStart != -1) {
                throw new EOFError(-1, cursor, "One line comment");
            }

            if (multiLineCommentStart != -1) {
                throw new EOFError(-1, cursor, "Missing end of comment", "**");
            }

            if (containsNonWhitespaceData &&
                    (lastSemicolonIdx == -1 || lastSemicolonIdx >= cursor)) {
                throw new EOFError(-1, cursor, "Missing semicolon (;)");
            }
        }

    }

    private static class SQLCliConstants {

        static final Set<String> COMMAND_SET = new HashSet<>(Arrays.asList("clear", "exit", "help", "history"));
        static final String EXIT_PROMPT = new AttributedStringBuilder()
                .style(AttributedStyle.BOLD.foreground(PRIMARY_COLOR))
                .append("Exiting from SQL console")
                .toAnsi();
        static final Integer BOOLEAN_FORMAT_LENGTH = 5;
        static final Integer BIGINT_FORMAT_LENGTH = 20;
        static final Integer DATE_FORMAT_LENGTH = 10;
        static final Integer DECIMAL_FORMAT_LENGTH = 25; // it has normally unlimited precision
        static final Integer DOUBLE_FORMAT_LENGTH = 25; // it has normally unlimited precision
        static final Integer INTEGER_FORMAT_LENGTH = 12;
        static final Integer NULL_FORMAT_LENGTH = 4;
        static final Integer REAL_FORMAT_LENGTH = 25; // it has normally unlimited precision
        static final Integer OBJECT_FORMAT_LENGTH = 20; // it has normally unlimited precision
        static final Integer TINYINT_FORMAT_LENGTH = 4;
        static final Integer SMALLINT_FORMAT_LENGTH = 6;
        static final Integer TIMESTAMP_FORMAT_LENGTH = 19;
        static final Integer TIMESTAMP_WITH_TIME_ZONE_FORMAT_LENGTH = 25;
        static final Integer VARCHAR_FORMAT_LENGTH = 20; // it has normally unlimited precision
    }
}
