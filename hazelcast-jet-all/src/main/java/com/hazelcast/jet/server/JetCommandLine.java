/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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


import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.core.Cluster;
import com.hazelcast.instance.JetBuildInfo;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.JobStateSnapshot;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.core.JobNotFoundException;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.impl.ClusterMetadata;
import com.hazelcast.jet.impl.JetClientInstanceImpl;
import com.hazelcast.jet.impl.JobSummary;
import com.hazelcast.jet.impl.config.XmlJetConfigBuilder;
import com.hazelcast.jet.server.JetCommandLine.JetVersionProvider;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.DefaultExceptionHandler;
import picocli.CommandLine.ExecutionException;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Help.Visibility;
import picocli.CommandLine.HelpCommand;
import picocli.CommandLine.IVersionProvider;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ParseResult;
import picocli.CommandLine.RunAll;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.LogManager;

import static com.hazelcast.instance.BuildInfoProvider.getBuildInfo;
import static com.hazelcast.jet.Util.idToString;
import static com.hazelcast.jet.impl.util.Util.toLocalDateTime;
import static java.util.Collections.emptyList;

@Command(
        name = "jet",
        description = "Utility for interacting with a Hazelcast Jet cluster." +
                "%n%n" +
                "The command line tool uses the Jet client to connect and perform operations " +
                "on the cluster. By default, the client config XML inside the config path will " +
                "be used for the connection." +
                "%n%n" +
                "Global options are:%n",
        versionProvider = JetVersionProvider.class,
        mixinStandardHelpOptions = true,
        sortOptions = false,
        subcommands = {HelpCommand.class}
)
public class JetCommandLine implements Callable<Void> {

    private static final int MAX_STR_LENGTH = 24;
    private static final int WAIT_INTERVAL_MILLIS = 100;

    private final Function<ClientConfig, JetInstance> jetClientFn;
    private final PrintStream out;
    private final PrintStream err;

    @Option(names = {"-f", "--config"},
            description = "Optional path to a client config XML file. " +
                    "By default config/hazelcast-client.xml is used." +
                    "If this option is specified then the addresses and group name options are ignored.",
            order = 0
    )
    private File configXml;

    @Option(names = {"-a", "--addresses"},
            split = ",",
            arity = "1..*",
            paramLabel = "<hostname>:<port>",
            description = "Optional comma-separated list of Jet node addresses in the format " +
                    "<hostname>:<port> to connect to another cluster than the " +
                    "one configured in config/hazelcast-client.xml",
            order = 1
    )
    private List<String> addresses;

    @Option(names = {"-g", "--group"},
            description = "The group name to use when connecting to the cluster " +
                    "specified by the <addresses> parameter. ",
            defaultValue = "jet",
            showDefaultValue = Visibility.ALWAYS,
            order = 2
    )
    private String groupName;

    @Mixin(name = "verbosity")
    private Verbosity verbosity;

    public JetCommandLine(Function<ClientConfig, JetInstance> jetClientFn, PrintStream out, PrintStream err) {
        this.jetClientFn = jetClientFn;
        this.out = out;
        this.err = err;
    }

    public static void main(String[] args) {
        runCommandLine(Jet::newJetClient, System.out, System.err, true, args);
    }

    static void runCommandLine(
            Function<ClientConfig, JetInstance> jetClientFn,
            PrintStream out, PrintStream err,
            boolean shouldExit,
            String[] args
    ) {
        CommandLine cmd = new CommandLine(new JetCommandLine(jetClientFn, out, err));

        String jetVersion = getBuildInfo().getJetBuildInfo().getVersion();
        cmd.getCommandSpec().usageMessage().header("Hazelcast Jet " + jetVersion);

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

    @Override
    public Void call() {
        return null;
    }

    @Command(description = "Submits a job to the cluster",
            mixinStandardHelpOptions = true
    )
    public void submit(
            @Mixin(name = "verbosity") Verbosity verbosity,
            @Option(names = {"-s", "--snapshot"},
                    paramLabel = "<snapshot name>",
                    description = "Name of initial snapshot to start the job from"
            ) String snapshotName,
            @Option(names = {"-n", "--name"},
                    paramLabel = "<name>",
                    description = "Name of the job"
            ) String name,
            @Parameters(index = "0",
                    paramLabel = "<jar file>",
                    description = "The jar file to submit"
            ) File file,
            @Parameters(index = "1..*",
                    paramLabel = "<arguments>",
                    description = "arguments to pass to the supplied jar file"
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
        printf("Submitting JAR '%s' with arguments %s%n", file, params);
        JetBootstrap.executeJar(getClientConfig(), file.getAbsolutePath(), snapshotName, name, params);
    }

    @Command(
            description = "Suspends a running job",
            mixinStandardHelpOptions = true
    )
    public void suspend(
            @Mixin(name = "verbosity") Verbosity verbosity,
            @Parameters(index = "0",
                    paramLabel = "<job name or id>",
                    description = "Name of the job to suspend"
            ) String name
    ) throws IOException {
        runWithJet(verbosity, jet -> {
            Job job = getJob(jet, name);
            assertJobRunning(name, job);
            printf("Suspending job %s...%n", formatJob(job));
            job.suspend();
            waitForJobStatus(job, JobStatus.SUSPENDED);
            println("Job was successfully suspended.");
        });
    }

    @Command(
            description = "Cancels a running job"
    )
    public void cancel(
            @Mixin(name = "verbosity") Verbosity verbosity,
            @Parameters(index = "0",
                    paramLabel = "<job name or id>",
                    description = "Name of the job to terminate"
            ) String name
    ) throws IOException {
        runWithJet(verbosity, jet -> {
            Job job = getJob(jet, name);
            assertJobActive(name, job);
            printf("Cancelling job %s...%n", formatJob(job));
            job.cancel();
            waitForJobStatus(job, JobStatus.FAILED);
            println("Job was successfully terminated.");
        });
    }

    @Command(
            name = "save-snapshot",
            description = "Exports a named snapshot from a job and optionally cancels it"
    )
    public void saveSnapshot(
            @Mixin(name = "verbosity") Verbosity verbosity,
            @Parameters(index = "0",
                    paramLabel = "<job name or id>",
                    description = "Name of the job to terminate")
                    String jobName,
            @Parameters(index = "1",
                    paramLabel = "<snapshot name>",
                    description = "Name of the snapshot")
                    String snapshotName,
            @Option(names = {"-C", "--cancel"},
                    description = "Cancel the job after taking the snapshot")
                    boolean isTerminal
    ) throws IOException {
        runWithJet(verbosity, jet -> {
            Job job = getJob(jet, jobName);
            assertJobActive(jobName, job);
            if (isTerminal) {
                printf("Saving snapshot with name '%s' from job '%s' and terminating the job...%n",
                        snapshotName, formatJob(job)
                );
                job.cancelAndExportSnapshot(snapshotName);
                waitForJobStatus(job, JobStatus.FAILED);
            } else {
                printf("Saving snapshot with name '%s' from job '%s'...%n", snapshotName, formatJob(job));
                job.exportSnapshot(snapshotName);
            }
            printf("Snapshot '%s' was successfully exported.%n", snapshotName);
        });
    }

    @Command(
            name = "delete-snapshot",
            description = "Deletes a named snapshot"
    )
    public void deleteSnapshot(
            @Mixin(name = "verbosity") Verbosity verbosity,
            @Parameters(index = "0",
                    paramLabel = "<snapshot name>",
                    description = "Name of the snapshot")
                    String snapshotName
    ) throws IOException {
        runWithJet(verbosity, jet -> {
            JobStateSnapshot jobStateSnapshot = jet.getJobStateSnapshot(snapshotName);
            if (jobStateSnapshot == null) {
                throw new JetException(String.format("No snapshot with name '%s' was found", snapshotName));
            }
            jobStateSnapshot.destroy();
            printf("Snapshot '%s' was successfully deleted.%n", snapshotName);
        });
    }

    @Command(
            description = "Restarts a running job"
    )
    public void restart(
            @Mixin(name = "verbosity") Verbosity verbosity,
            @Parameters(index = "0",
                    paramLabel = "<job name or id>",
                    description = "Name of the job to restart")
                    String name
    ) throws IOException {
        runWithJet(verbosity, jet -> {
            Job job = getJob(jet, name);
            assertJobRunning(name, job);
            println("Restarting job " + formatJob(job) + "...");
            job.restart();
            waitForJobStatus(job, JobStatus.RUNNING);
            println("Job was successfully restarted.");
        });
    }

    @Command(
            description = "Resumes a suspended job"
    )
    public void resume(
            @Mixin(name = "verbosity") Verbosity verbosity,
            @Parameters(index = "0",
                    paramLabel = "<job name or id>",
                    description = "Name of the job to resume")
                    String name
    ) throws IOException {
        runWithJet(verbosity, jet -> {
            Job job = getJob(jet, name);
            if (job.getStatus() != JobStatus.SUSPENDED) {
                throw new RuntimeException("Job '" + name + "' is not suspended. Current state: " + job.getStatus());
            }
            println("Resuming job " + formatJob(job) + "...");
            job.resume();
            waitForJobStatus(job, JobStatus.RUNNING);
            println("Job was successfully resumed.");
        });
    }

    @Command(
            name = "list-jobs",
            description = "Lists running jobs on the cluster"
    )
    public void listJobs(
            @Mixin(name = "verbosity") Verbosity verbosity,
            @Option(names = {"-a", "--all"},
                    description = "Lists all jobs including completed and failed ones")
                    boolean listAll
    ) throws IOException {
        runWithJet(verbosity, jet -> {
            JetClientInstanceImpl client = (JetClientInstanceImpl) jet;
            List<JobSummary> summaries = client.getJobSummaryList();
            String format = "%-24s %-19s %-18s %-23s%n";
            printf(format, "NAME", "ID", "STATUS", "SUBMISSION TIME");
            summaries.stream()
                    .filter(job -> listAll || isActive(job.getStatus()))
                    .forEach(job -> {
                        String idString = idToString(job.getJobId());
                        String name = job.getName().equals(idString) ? "N/A" : shorten(job.getName(), MAX_STR_LENGTH);
                        printf(format,
                                name, idString, job.getStatus(), toLocalDateTime(job.getSubmissionTime())
                        );
                    });
        });
    }

    @Command(
            name = "list-snapshots",
            description = "Lists exported snapshots on the cluster"
    )
    public void listSnapshots(
            @Mixin(name = "verbosity") Verbosity verbosity
    ) throws IOException {
        runWithJet(verbosity, jet -> {
            Collection<JobStateSnapshot> snapshots = jet.getJobStateSnapshots();
            printf("%-24s %-15s %-23s %-24s%n", "NAME", "SIZE (bytes)", "TIME", "JOB NAME");
            snapshots.forEach(ss -> {
                String jobName = ss.jobName() == null ? Util.idToString(ss.jobId()) : ss.jobName();
                jobName = shorten(jobName, MAX_STR_LENGTH);
                String ssName = shorten(ss.name(), MAX_STR_LENGTH);
                LocalDateTime creationTime = toLocalDateTime(ss.creationTime());
                printf("%-24s %-,15d %-23s %-24s%n", ssName, ss.payloadSize(), creationTime, jobName);
            });
        });
    }

    @Command(
            description = "Shows current cluster state and information about members"
    )
    public void cluster(
            @Mixin(name = "verbosity") Verbosity verbosity
    ) throws IOException {
        runWithJet(verbosity, jet -> {
            JetClientInstanceImpl client = (JetClientInstanceImpl) jet;
            ClusterMetadata clusterMetadata = ((JetClientInstanceImpl) jet).getClusterMetadata();
            Cluster cluster = client.getCluster();

            println("State: " + clusterMetadata.getState());
            println("Version: " + clusterMetadata.getVersion());
            println("Size: " + cluster.getMembers().size());

            println("");

            String format = "%-24s %-19s%n";
            printf(format, "ADDRESS", "UUID");
            cluster.getMembers().forEach(member -> printf(format, member.getAddress(), member.getUuid()));
        });
    }

    private void runWithJet(Verbosity verbosity, Consumer<JetInstance> consumer) throws IOException {
        this.verbosity.merge(verbosity);
        configureLogging();
        JetInstance jet = jetClientFn.apply(getClientConfig());
        try {
            consumer.accept(jet);
        } finally {
            jet.shutdown();
        }
    }

    private ClientConfig getClientConfig() throws IOException {
        if (configXml != null) {
            return new XmlClientConfigBuilder(configXml).build();
        }
        if (addresses != null) {
            ClientConfig config = new ClientConfig();
            config.getNetworkConfig().addAddress(addresses.toArray(new String[0]));
            config.getGroupConfig().setName(groupName);
            return config;
        }
        return XmlJetConfigBuilder.getClientConfig();
    }

    private void configureLogging() throws IOException {
        StartServer.configureLogging();
        Level logLevel = Level.WARNING;
        if (verbosity.isVerbose) {
            println("Verbose mode is on, setting logging level to INFO");
            logLevel = Level.INFO;
        }
        LogManager.getLogManager().getLogger("").setLevel(logLevel);
    }

    private static Job getJob(JetInstance jet, String nameOrId) {
        Job job = jet.getJob(nameOrId);
        if (job == null) {
            job = jet.getJob(Util.idFromString(nameOrId));
            if (job == null) {
                throw new JobNotFoundException("No job with name or id '" + nameOrId + "' was found");
            }
        }
        return job;
    }

    private void printf(String format, Object... objects) {
        out.printf(format, objects);
    }

    private void println(String msg) {
        out.println(msg);
    }

    private static String shorten(String name, int length) {
        return name.substring(0, Math.min(name.length(), length));
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

    public static class JetVersionProvider implements IVersionProvider {

        @Override
        public String[] getVersion() {
            JetBuildInfo jetBuildInfo = getBuildInfo().getJetBuildInfo();
            return new String[]{
                    "Hazelcast Jet " + jetBuildInfo.getVersion(),
                    "Revision " + jetBuildInfo.getRevision(),
                    "Build " + jetBuildInfo.getBuild()
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

    static class ExceptionHandler<R> extends DefaultExceptionHandler<R> {
        @Override
        public R handleExecutionException(ExecutionException ex, ParseResult parseResult) {
            // find top level command
            CommandLine cmdLine = ex.getCommandLine();
            while (cmdLine.getParent() != null) {
                cmdLine = cmdLine.getParent();
            }
            JetCommandLine jetCmd = cmdLine.getCommand();
            if (jetCmd.verbosity.isVerbose) {
                ex.printStackTrace(err());
            } else {
                err().println("ERROR: " + ex.getCause().getMessage());
                err().println();
                err().println("To see the full stack trace, re-run with the -v/--verbosity option");
            }
            if (hasExitCode()) {
                exit(exitCode());
            }
            throw ex;
        }
    }
}
