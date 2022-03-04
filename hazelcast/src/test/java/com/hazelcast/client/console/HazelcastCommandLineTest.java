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

package com.hazelcast.client.console;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.cluster.Address;
import com.hazelcast.collection.IList;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.annotation.SerializationSamplesExcluded;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.hazelcast.client.console.HazelcastCommandLine.runCommandLine;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class, SerializationSamplesExcluded.class})
public class HazelcastCommandLineTest extends JetTestSupport {

    private static final String SOURCE_NAME = "source";
    private static final String SINK_NAME = "sink";
    private static final int ITEM_COUNT = 1000;

    private static Path testJobJarFile;
    private static File xmlConfiguration;
    private static File yamlConfiguration;

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private ByteArrayOutputStream baosOut;
    private ByteArrayOutputStream baosErr;

    private PrintStream out;
    private PrintStream err;
    private HazelcastInstance hz;
    private IMap<Integer, Integer> sourceMap;
    private IList<Integer> sinkList;
    private HazelcastInstance client;

    @BeforeClass
    public static void beforeClass() throws IOException {
        createJarFile();
        xmlConfiguration = new File(HazelcastCommandLineTest.class.getResource("hazelcast-client-test.xml").getPath());
        yamlConfiguration = new File(HazelcastCommandLineTest.class.getResource("hazelcast-client-test.yaml").getPath());
    }

    public static void createJarFile() throws IOException {
        testJobJarFile = Files.createTempFile("testjob-", ".jar");
        IOUtil.copy(HazelcastCommandLineTest.class.getResourceAsStream("testjob-with-hz-bootstrap.jar"),
                testJobJarFile.toFile());
    }

    @AfterClass
    public static void afterClass() {
        IOUtil.deleteQuietly(testJobJarFile.toFile());
    }

    @Before
    public void before() {
        Config cfg = smallInstanceConfig();
        cfg.getJetConfig().setResourceUploadEnabled(true);
        cfg.getMapConfig(SOURCE_NAME).getEventJournalConfig().setEnabled(true);
        String clusterName = randomName();
        cfg.setClusterName(clusterName);
        hz = createHazelcastInstance(cfg);
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName(clusterName);
        client = createHazelcastClient(clientConfig);
        resetOut();

        Address address = hz.getCluster().getLocalMember().getAddress();
        System.setProperty("member", address.getHost() + ":" + address.getPort());
        System.setProperty("group", clusterName);
        sourceMap = hz.getMap(SOURCE_NAME);
        IntStream.range(0, ITEM_COUNT).forEach(i -> sourceMap.put(i, i));
        sinkList = hz.getList(SINK_NAME);
        assertTrueEventually(() -> {
            if (!isJarFileExists()) {
                createJarFile();
            }
            assertTrue(isJarFileExists());
        });
    }

    public boolean isJarFileExists() {
        return Files.exists(testJobJarFile);
    }

    @After
    public void after() {
        String stdOutput = captureOut();
        if (stdOutput.length() > 0) {
            System.out.println("--- Captured standard output");
            System.out.println(stdOutput);
            System.out.println("--- End of captured standard output");
        }
        String errOutput = captureErr();
        if (errOutput.length() > 0) {
            System.out.println("--- Captured error output");
            System.out.println(errOutput);
            System.out.println("--- End of captured error output");
        }
    }

    @Test
    public void test_listJobs() {
        // Given
        Job job = newJob();

        // When
        run("list-jobs");

        // Then
        String actual = captureOut();
        assertContains(actual, job.getName());
        assertContains(actual, job.getIdString());
        assertContains(actual, job.getStatus().toString());
    }

    @Test
    public void test_listJobs_dirtyName() {
        // Given
        String jobName = "job\n\tname\u0000";
        Job job = newJob(jobName);

        // When
        run("list-jobs");

        // Then
        String actual = captureOut();
        assertContains(actual, jobName);
        assertContains(actual, job.getIdString());
        assertContains(actual, job.getStatus().toString());
    }

    @Test
    public void test_cancelJob_byJobName() {
        // Given
        Job job = newJob();

        // When
        run("cancel", job.getName());

        // Then
        assertJobStatusEventually(job, JobStatus.FAILED);
    }

    @Test
    public void test_cancelJob_byJobId() {
        // Given
        Job job = newJob();

        // When
        run("cancel", job.getIdString());

        // Then
        assertJobStatusEventually(job, JobStatus.FAILED);
    }

    @Test
    public void test_cancelJob_invalidNameOrId() {
        // When
        // Then
        exception.expectMessage("No job with name or id 'invalid' was found");
        run("cancel", "invalid");
    }

    @Test
    public void test_cancelJob_jobNotActive() {
        // Given
        Job job = newJob();
        job.cancel();
        assertJobStatusEventually(job, JobStatus.FAILED);

        // When
        // Then
        exception.expectMessage("is not active");
        run("cancel", job.getName());
    }

    @Test
    public void test_suspendJob_byJobName() {
        // Given
        Job job = newJob();

        // When
        run("suspend", job.getName());

        // Then
        assertJobStatusEventually(job, JobStatus.SUSPENDED);
    }

    @Test
    public void test_suspendJob_byJobId() {
        // Given
        Job job = newJob();

        // When
        run("suspend", job.getIdString());

        // Then
        assertJobStatusEventually(job, JobStatus.SUSPENDED);
    }

    @Test
    public void test_suspendJob_invalidNameOrId() {
        // When
        // Then
        exception.expectMessage("No job with name or id 'invalid' was found");
        run("cancel", "invalid");
    }

    @Test
    public void test_suspendJob_jobNotRunning() {
        // Given
        Job job = newJob();
        job.cancel();
        assertJobStatusEventually(job, JobStatus.FAILED);

        // When
        // Then
        exception.expectMessage("is not running");
        run("suspend", job.getName());
    }

    @Test
    public void test_resumeJob_byJobName() {
        // Given
        Job job = newJob();
        assertJobStatusEventually(job, JobStatus.RUNNING);
        job.suspend();
        assertJobStatusEventually(job, JobStatus.SUSPENDED);

        // When
        run("resume", job.getName());

        // Then
        assertJobStatusEventually(job, JobStatus.RUNNING);
    }

    @Test
    public void test_resumeJob_byJobId() {
        // Given
        Job job = newJob();
        assertJobStatusEventually(job, JobStatus.RUNNING);
        job.suspend();
        assertJobStatusEventually(job, JobStatus.SUSPENDED);

        // When
        run("resume", job.getIdString());

        // Then
        assertJobStatusEventually(job, JobStatus.RUNNING);
    }

    @Test
    public void test_resumeJob_invalidNameOrId() {
        // When
        // Then
        exception.expectMessage("No job with name or id 'invalid' was found");
        run("resume", "invalid");
    }

    @Test
    public void test_resumeJob_jobNotSuspended() {
        // Given
        Job job = newJob();
        assertJobStatusEventually(job, JobStatus.RUNNING);

        // When
        // Then
        exception.expectMessage("is not suspended");
        run("resume", job.getName());
    }

    @Test
    public void test_restartJob_byJobName() {
        // Given
        Job job = newJob();
        assertTrueEventually(() -> assertEquals(ITEM_COUNT, sinkList.size()));

        // When
        run("restart", job.getName());

        // Then
        // we expect the same items to be read again due to lack of snapshots
        assertTrueEventually(() -> assertEquals(ITEM_COUNT * 2, sinkList.size()));
    }

    @Test
    public void test_restartJob_byJobId() {
        // Given
        Job job = newJob();
        assertTrueEventually(() -> assertEquals(ITEM_COUNT, sinkList.size()));

        // When
        run("restart", job.getIdString());

        // Then
        // we expect the same items to be read again due to lack of snapshots
        assertTrueEventually(() -> assertEquals(ITEM_COUNT * 2, sinkList.size()));
    }

    @Test
    public void test_restartJob_invalidNameOrId() {
        // When
        // Then
        exception.expectMessage("No job with name or id 'invalid' was found");
        run("restart", "invalid");
    }

    @Test
    public void test_restartJob_jobNotRunning() {
        // Given
        Job job = newJob();
        assertJobStatusEventually(job, JobStatus.RUNNING);
        job.suspend();
        assertJobStatusEventually(job, JobStatus.SUSPENDED);

        // When
        // Then
        exception.expectMessage("is not running");
        run("restart", job.getName());
    }

    @Test
    public void test_saveSnapshot_invalidNameOrId() {
        // When
        // Then
        exception.expectMessage("No job with name or id 'invalid' was found");
        run("save-snapshot", "invalid", "my-snapshot");
    }

    @Test
    public void test_saveSnapshot_jobNotActive() {
        // Given
        Job job = newJob();
        assertJobStatusEventually(job, JobStatus.RUNNING);
        job.cancel();
        assertJobStatusEventually(job, JobStatus.FAILED);

        // When
        // Then
        exception.expectMessage("is not active");
        run("save-snapshot", job.getIdString(), "my-snapshot");
    }

    @Test
    public void test_listSnapshots() {
        // Given
        // When
        run("list-snapshots");

        // Then
        String actual = captureOut();
        assertTrue("output should contain one line (the table header), but contains:\n" + actual,
                actual.trim().indexOf('\n') < 0 && !actual.isEmpty());
    }

    @Test
    public void test_cluster() {
        // When
        run("cluster");

        // Then
        String actual = captureOut();
        assertContains(actual, hz.getCluster().getLocalMember().getUuid().toString());
        assertContains(actual, "ACTIVE");
    }

    @Test
    public void test_verbosity() {
        testVerbosity("cancel", "jobName", "-v");
        testVerbosity("-v", "cancel", "jobName");
        testVerbosity("cluster", "-v");
        testVerbosity("-v", "cluster");
        testVerbosity("delete-snapshot", "snapshotName", "-v");
        testVerbosity("-v", "delete-snapshot", "snapshotName");
        testVerbosity("list-jobs", "-v");
        testVerbosity("-v", "list-jobs", "-v");
        testVerbosity("list-snapshots", "-v");
        testVerbosity("-v", "list-snapshots");
        testVerbosity("restart", "jobName", "-v");
        testVerbosity("-v", "restart", "jobName");
        testVerbosity("resume", "jobName", "-v");
        testVerbosity("-v", "resume", "jobName");
        testVerbosity("save-snapshot", "jobName", "snapshotName", "-v");
        testVerbosity("-v", "save-snapshot", "jobName", "snapshotName");
        testVerbosity("submit", "-v", "job.jar");
        testVerbosity("-v", "submit", "job.jar");
        testVerbosity("suspend", "jobName", "-v");
        testVerbosity("-v", "suspend", "jobName");
    }

    @Test
    public void test_submit() {
        run("submit", testJobJarFile.toString());
        assertTrueEventually(() -> assertEquals(1, hz.getJet().getJobs().size()));
        Job job = hz.getJet().getJobs().get(0);
        assertJobStatusEventually(job, JobStatus.RUNNING);
        assertNull(job.getName());
    }

    @Test
    public void test_submit_clientShutdownWhenDone() {
        run("submit", testJobJarFile.toString());
        assertTrueEventually(() -> assertEquals(1, hz.getJet().getJobs().size()));
        Job job = hz.getJet().getJobs().get(0);
        assertJobStatusEventually(job, JobStatus.RUNNING);
        assertFalse("Instance should be shut down", client.getLifecycleService().isRunning());
    }

    @Test
    public void test_submit_nameUsed() {
        run("submit", "-n", "fooName", testJobJarFile.toString());
        assertTrueEventually(() -> assertEquals(1, hz.getJet().getJobs().size()), 5);
        Job job = hz.getJet().getJobs().get(0);
        assertEquals("fooName", job.getName());
    }

    @Test
    public void test_submit_withClassName() {
        run("submit", "--class", "com.hazelcast.jet.testjob.TestJob", testJobJarFile.toString());
        assertTrueEventually(() -> assertEquals(1, hz.getJet().getJobs().size()), 5);
        Job job = hz.getJet().getJobs().get(0);
        assertJobStatusEventually(job, JobStatus.RUNNING);
        assertNull(job.getName());
    }

    @Test
    public void test_submit_withWrongClassName() {
        exception.expectMessage("ClassNotFoundException");
        run("submit", "--class", "com.hazelcast.jet.testjob.NonExisting", testJobJarFile.toString());
    }

    @Test
    public void test_submit_argsPassing() {
        run("submit", testJobJarFile.toString(), "--jobOption", "fooValue");
        assertTrueEventually(() -> assertContains(captureOut(), " with arguments [--jobOption, fooValue]"));
    }

    @Test
    public void test_submit_with_JetBootstrap() throws IOException {
        Path testJarWithJetBootstrap = Files.createTempFile("testjob-with-jet-bootstrap-", ".jar");
        IOUtil.copy(HazelcastCommandLineTest.class.getResourceAsStream("testjob-with-jet-bootstrap.jar"),
                testJarWithJetBootstrap.toFile());
        run("submit", testJarWithJetBootstrap.toString());
        assertTrueEventually(() -> assertEquals(1, hz.getJet().getJobs().size()));
        Job job = hz.getJet().getJobs().get(0);
        assertJobStatusEventually(job, JobStatus.RUNNING);
        assertNull(job.getName());
        IOUtil.deleteQuietly(testJarWithJetBootstrap.toFile());
    }

    @Test
    public void test_submit_job_with_hazelcast_classes() throws IOException {
        PrintStream oldErr = System.err;
        System.setErr(new PrintStream(err));
        Path testJarFile = Files.createTempFile("testjob-with-hazelcast-codebase-", ".jar");
        IOUtil.copy(HazelcastCommandLineTest.class.getResourceAsStream("testjob-with-hazelcast-codebase.jar"), testJarFile.toFile());
        try {
            run("submit", testJarFile.toString());
            String actual = captureErr();
            String pathToClass = Paths.get("com", "hazelcast", "jet", "testjob", "HazelcastBootstrap.class").toString();
            assertThat(actual).contains("WARNING: Hazelcast code detected in the jar: " + pathToClass + ". Hazelcast dependency should be set with the 'provided' scope or equivalent.");
        } finally {
            System.setErr(oldErr);
            IOUtil.deleteQuietly(testJarFile.toFile());
        }
    }

    @Test
    public void testTargetsMixin() {
        String target = "foobar@127.0.0.1:5701,127.0.0.1:5702";

        testTargetsCommand("cancel", "-t", target, "jobName");
        testTargetsCommand("-t", target, "cancel", "jobName");

        testTargetsCommand("cluster", "-t", target);
        testTargetsCommand("-t", target, "cluster");

        testTargetsCommand("delete-snapshot", "snapshotName", "-t", target);
        testTargetsCommand("-t", target, "delete-snapshot", "snapshotName");

        testTargetsCommand("list-jobs", "-t", target);
        testTargetsCommand("-t", target, "list-jobs");

        testTargetsCommand("list-snapshots", "-t", target);
        testTargetsCommand("-t", target, "list-snapshots");

        testTargetsCommand("restart", "jobName", "-t", target);
        testTargetsCommand("-t", target, "restart", "jobName");

        testTargetsCommand("resume", "jobName", "-t", target);
        testTargetsCommand("-t", target, "resume", "jobName");

        testTargetsCommand("save-snapshot", "jobName", "snapshotName", "-t", target);
        testTargetsCommand("-t", target, "save-snapshot", "jobName", "snapshotName");

        testTargetsCommand("submit", "-t", target, testJobJarFile.toString());
        testTargetsCommand("-t", target, "submit", testJobJarFile.toString());

        testTargetsCommand("suspend", "jobName", "-t", target);
        testTargetsCommand("-t", target, "suspend", "jobName");
    }

    @Test
    public void testTargetsAfterCommandTakesPrecedence() {
        String target = "foobar@127.0.0.1:5701,127.0.0.1:5702";
        testTargetsCommand("-t", "ignore@127.0.0.1:1234", "submit", "-t", target, testJobJarFile.toString());
    }

    @Test
    public void testTargetsInvalidValue() {
        run("submit", "-t", "@", testJobJarFile.toString());
        String actual = captureErr();
        assertContains(actual, "Invalid value for option '--targets':");
    }

    @Test
    public void testTargetsClusterNameOptional() {
        String target = "127.0.0.1:5701,127.0.0.1:5702";

        testTargetsCommandCluster("dev", "list-jobs", "-t", target);
        testTargetsCommandCluster("dev", "-t", target, "list-jobs");
    }

    private void testTargetsCommand(String... args) {
        testTargetsCommandCluster("foobar", args);
    }

    private ClientConfig testTargetsCommandCluster(String expectedClusterName, String... args) {
        AtomicReference<ClientConfig> atomicConfig = new AtomicReference<>();
        Function<ClientConfig, HazelcastInstance> fnRunCommand = (config) -> {
            atomicConfig.set(config);
            return this.client;
        };

        try {
            run(fnRunCommand, args);
        } catch (Exception ignore) {
            // ignore
        }

        ClientConfig config = atomicConfig.get();
        assertEquals(expectedClusterName, config.getClusterName());
        assertEquals("[127.0.0.1:5701, 127.0.0.1:5702]", config.getNetworkConfig().getAddresses().toString());

        return config;
    }

    @Test
    public void test_yaml_configuration() {
        test_custom_configuration(yamlConfiguration.toString());
    }

    @Test
    public void test_xml_configuration() {
        test_custom_configuration(xmlConfiguration.toString());
    }

    @Test
    public void when_targets_and_configuration_together_then_targets_is_applied() {
        ClientConfig config = testTargetsCommandCluster("my-cluster",
                "--targets", "my-cluster@127.0.0.1:5701,127.0.0.1:5702",
                "-f", yamlConfiguration.toString(),
                "list-jobs");

        assertThat(config.getLabels()).contains("yaml-label");
    }

    @Test
    public void test_targets_and_xml_configuration_together() {
        ClientConfig config = testTargetsCommandCluster("my-cluster",
                "--targets", "my-cluster@127.0.0.1:5701,127.0.0.1:5702",
                "-f", xmlConfiguration.toString(),
                "list-jobs");

        assertThat(config.getLabels()).contains("xml-label");
    }

    @Test
    public void test_targets_after_command_and_configuration_together() {
        ClientConfig config = testTargetsCommandCluster("my-cluster",
                "-f", yamlConfiguration.toString(),
                "list-jobs",
                "--targets", "my-cluster@127.0.0.1:5701,127.0.0.1:5702"
        );

        assertThat(config.getLabels()).contains("yaml-label");
    }

    @Test
    public void test_targets_after_command_and_configuration_from_default_config_together() throws IOException {
        Path sourceLocation = Paths.get("src/test/resources/com/hazelcast/client/console/hazelcast-client-template.yaml");
        Path cpConfigLocation = Paths.get("target/test-classes/hazelcast-client.yaml");

        try {
            Files.copy(
                    sourceLocation,
                    cpConfigLocation,
                    StandardCopyOption.REPLACE_EXISTING);

            ClientConfig config = testTargetsCommandCluster("my-cluster",
                    "list-jobs",
                    "--targets", "my-cluster@127.0.0.1:5701,127.0.0.1:5702"
            );

            assertThat(config.getLabels()).contains("yaml-label");
        } finally {
            Files.delete(cpConfigLocation);
        }
    }

    private void test_custom_configuration(String configFile) {
        run(cfg -> createHazelcastClient(cfg), "-f", configFile, "cluster");

        String actual = captureOut();
        assertContains(actual, hz.getCluster().getLocalMember().getUuid().toString());
        assertContains(actual, "ACTIVE");
    }

    private void testVerbosity(String... args) {
        System.out.println("Testing verbosity with parameters " + Arrays.toString(args));
        try {
            resetOut();
            run(args);
        } catch (Exception ignored) {
        }
        assertContains(captureOut(), "Verbose mode is on");
    }

    private Job newJob() {
        return newJob("job-infinite-pipeline");
    }

    private Job newJob(String jobName) {
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.mapJournal(SOURCE_NAME, START_FROM_OLDEST))
                .withoutTimestamps()
                .writeTo(Sinks.list(SINK_NAME));
        Job job = hz.getJet().newJob(p, new JobConfig().setName(jobName));
        assertJobStatusEventually(job, JobStatus.RUNNING);
        return job;
    }

    private void run(String... args) {
        runCommandLine(cfg -> client, out, err, false, args);
    }

    private void run(Function<ClientConfig, HazelcastInstance> clientFn, String... args) {
        runCommandLine(clientFn, out, err, false, args);
    }

    private void resetOut() {
        baosOut = new ByteArrayOutputStream();
        baosErr = new ByteArrayOutputStream();
        out = new PrintStream(baosOut);
        err = new PrintStream(baosErr);
    }

    private String captureOut() {
        out.flush();
        return new String(baosOut.toByteArray());
    }

    private String captureErr() {
        err.flush();
        return new String(baosErr.toByteArray());
    }
}
