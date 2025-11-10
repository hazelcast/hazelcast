/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.submitjob.clientside.upload;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.properties.ClientProperty;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.instance.impl.HazelcastBootstrap;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.JobAlreadyExistsException;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.impl.JetClientInstanceImpl;
import com.hazelcast.jet.impl.SubmitJobParameters;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.URL;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Stream;

import static com.hazelcast.jet.core.JobAssertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doAnswer;

/**
 * Tests to upload jar to member
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class JobUploadClientFailureTest extends JetTestSupport {

    /**
     * The newJob() is called from main thread
     */
    private static final String SIMPLE_JAR = "simplejob-1.0.0.jar";

    /**
     * The newJob() is called from another thread
     */
    private static final String PARALLEL_JAR = "paralleljob-1.0.0.jar";
    private static final String JOINING_JAR = "joiningjob-1.0.0.jar";
    private static final String NO_MANIFEST_SIMPLE_JAR = "nomanifestsimplejob-1.0.0.jar";

    @After
    public void resetSingleton() {
        // Reset the singleton after the test
        HazelcastBootstrap.resetRemembered();
    }

    @Test
    public void testNullJarPath() {
        createCluster();
        JetClientInstanceImpl jetService = getClientJetService();

        SubmitJobParameters submitJobParameters = SubmitJobParameters.withJarOnClient();

        assertThatThrownBy(() -> jetService.submitJobFromJar(submitJobParameters))
                .isInstanceOf(JetException.class)
                .hasMessageContaining("jarPath can not be null");
    }

    @Test
    public void testJarDoesNotExist() {
        createCluster();
        JetClientInstanceImpl jetService = getClientJetService();

        String jarPath = "thisdoesnotexist.jar";
        SubmitJobParameters submitJobParameters = SubmitJobParameters.withJarOnClient()
                .setJarPath(Paths.get(jarPath));

        assertThatThrownBy(() -> jetService.submitJobFromJar(submitJobParameters))
                .isInstanceOf(NoSuchFileException.class)
                .hasMessageContaining(jarPath);
    }

    @Test
    public void testNullJobParameters() {
        createCluster();
        JetClientInstanceImpl jetService = getClientJetService();

        SubmitJobParameters submitJobParameters = SubmitJobParameters.withJarOnClient()
                .setJarPath(getJarPath())
                .setJobParameters(null);

        assertThatThrownBy(() -> jetService.submitJobFromJar(submitJobParameters))
                .isInstanceOf(JetException.class)
                .hasMessageContaining("jobParameters can not be null");
    }

    @Test
    public void testNullMainClass() {
        createCluster();
        JetClientInstanceImpl jetService = getClientJetService();

        SubmitJobParameters submitJobParameters = SubmitJobParameters.withJarOnClient()
                .setJarPath(getNoManifestJarPath());

        assertThatThrownBy(() -> jetService.submitJobFromJar(submitJobParameters))
                .isInstanceOf(JetException.class)
                .hasStackTraceContaining("No Main-Class found in the manifest");
    }

    @Test
    public void testTooLongFileName() throws IOException, NoSuchAlgorithmException {
        createCluster();
        JetClientInstanceImpl jetService = getClientJetService();
        JetClientInstanceImpl spyJetService = Mockito.spy(jetService);

        Path jarPath = getJarPath();

        // Create a repeated filename
        char[] charArray = new char[300];
        Arrays.fill(charArray, 'a');
        String longFileName = new String(charArray);

        doAnswer(invocation -> {
            JobUploadCall jobUploadCall = (JobUploadCall) invocation.callRealMethod();
            jobUploadCall.setFileNameWithoutExtension(longFileName);
            return jobUploadCall;
        }).when(spyJetService).initializeJobUploadCall(jarPath);


        SubmitJobParameters submitJobParameters = SubmitJobParameters.withJarOnClient()
                .setJarPath(jarPath);

        assertThatThrownBy(() -> spyJetService.submitJobFromJar(submitJobParameters))
                .isInstanceOf(JetException.class)
                .hasCauseInstanceOf(FileSystemException.class);
    }

    @Test
    public void test_jarUpload_whenResourceUploadIsNotEnabled() {
        createHazelcastInstance();
        JetClientInstanceImpl jetService = getClientJetService();

        SubmitJobParameters submitJobParameters = SubmitJobParameters.withJarOnClient()
                .setJarPath(getJarPath());

        assertThrows(JetException.class, () ->
                jetService.submitJobFromJar(submitJobParameters)
        );

        assertEqualsEventually(() -> jetService.getJobs().size(), 0);
    }

    @Test
    public void test_jarUpload_withWrongMainClassname() {
        createCluster();
        JetClientInstanceImpl jetService = getClientJetService();

        SubmitJobParameters submitJobParameters = SubmitJobParameters.withJarOnClient()
                .setJarPath(getJarPath())
                .setMainClass("org.example.Main1");

        assertThrows(JetException.class, () -> jetService.submitJobFromJar(submitJobParameters));
    }


    @Test
    public void test_jarUpload_with_parallel_jobs() {
        createCluster();

        JetClientInstanceImpl jetService = getClientJetService();

        // upload the jar to member and call and within the jar start the job from another thread
        // It fails because member uses ThreadLocal to find ExecuteJobParameters
        SubmitJobParameters submitJobParameters = SubmitJobParameters.withJarOnClient()
                .setJarPath(getParalleJarPath())
                .setJobName("parallel_job");

        jetService.submitJobFromJar(submitJobParameters);

        assertJobIsNotRunning(jetService);
    }

    @Test
    public void test_jar_isDeleted() throws IOException {
        Path newPath = null;
        try {
            // Delete left over files before the test
            String newSimpleJob = "newsimplejob.jar";
            deleteLeftOverFilesIfAny(newSimpleJob);

            createCluster();
            JetClientInstanceImpl jetService = getClientJetService();

            // Copy as new jar to make it unique
            newPath = copyJar(newSimpleJob);

            SubmitJobParameters submitJobParameters = SubmitJobParameters.withJarOnClient()
                    .setJarPath(newPath)
                    .setMainClass("org.example.Main1");

            assertThrows(JetException.class, () -> jetService.submitJobFromJar(submitJobParameters));

            boolean fileDoesNotExist = fileDoesNotExist(newSimpleJob);
            assertThat(fileDoesNotExist)
                    .isTrue();
        } finally {
            if (newPath != null) {
                // Delete local new jar
                Files.delete(newPath);
            }
        }
    }

    @Test
    public void test_jarUpload_withIncorrectChecksum() throws IOException, NoSuchAlgorithmException {
        createCluster();

        // Mock the JetClientInstanceImpl to return an incorrect checksum
        JetClientInstanceImpl jetService = getClientJetService();
        JetClientInstanceImpl spyJetService = Mockito.spy(jetService);

        Path jarPath = getJarPath();
        doAnswer(invocation -> {
            JobUploadCall jobUploadCall = (JobUploadCall) invocation.callRealMethod();
            jobUploadCall.setSha256HexOfJar("1");
            return jobUploadCall;
        }).when(spyJetService).initializeJobUploadCall(jarPath);


        SubmitJobParameters submitJobParameters = SubmitJobParameters.withJarOnClient()
                .setJarPath(getJarPath());

        assertThrows(JetException.class, () -> spyJetService.submitJobFromJar(submitJobParameters));

        assertEqualsEventually(() -> jetService.getJobs().size(), 0);
    }

    @Test
    public void test_jobAlreadyExists() {
        createCluster();
        JetClientInstanceImpl jetService = getClientJetService();

        String job1 = "job1";
        SubmitJobParameters submitJobParameters1 = SubmitJobParameters.withJarOnClient()
                .setJarPath(getJarPath())
                .setJobName(job1);

        jetService.submitJobFromJar(submitJobParameters1);


        String job2 = "job1";
        SubmitJobParameters submitJobParameters2 = SubmitJobParameters.withJarOnClient()
                .setJarPath(getJarPath())
                .setJobName(job2);

        assertThatThrownBy(() -> jetService.submitJobFromJar(submitJobParameters2))
                .isInstanceOf(JetException.class)
                .hasRootCauseInstanceOf(JobAlreadyExistsException.class);

        assertTrueEventually(() -> {
            List<Job> jobs = jetService.getJobs();
            assertEquals(1, jobs.size());
            assertTrue(containsName(jobs, job1));
        });
    }

    @Test
    public void test_jarUpload_whenMemberShutsDown() throws IOException, NoSuchAlgorithmException {
        HazelcastInstance[] cluster = createMultiNodeCluster();

        // Speed up the test by waiting less on invocation timeout
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(ClientProperty.INVOCATION_TIMEOUT_SECONDS.getName(), "10");
        HazelcastInstance client = createHazelcastClient(clientConfig);

        JetClientInstanceImpl jetService = (JetClientInstanceImpl) client.getJet();
        JetClientInstanceImpl spyJetService = Mockito.spy(jetService);

        assertClusterSizeEventually(2, client);

        Path jarPath = getJarPath();
        doAnswer(invocation -> {
            JobUploadCall jobUploadCall = (JobUploadCall) invocation.callRealMethod();
            UUID memberUuid = jobUploadCall.getMemberUuid();
            // Shutdown the target member
            for (HazelcastInstance hazelcastInstance : cluster) {
                if (hazelcastInstance.getCluster().getLocalMember().getUuid().equals(memberUuid)) {
                    hazelcastInstance.shutdown();
                    break;
                }
            }
            assertClusterSizeEventually(1, client);
            return jobUploadCall;
        }).when(spyJetService).initializeJobUploadCall(jarPath);

        SubmitJobParameters submitJobParameters = SubmitJobParameters.withJarOnClient()
                .setJarPath(getJarPath());

        // Should throw OperationTimeoutException because target instance was shut down
        assertThrows(OperationTimeoutException.class, () -> spyJetService.submitJobFromJar(submitJobParameters));
    }

    @Test
    public void test_jarUpload_invalid_tempdir_whenResourceUploadIsEnabled() {
        createClusterWithUploadDirectoryPath("directorynotexists");
        JetClientInstanceImpl jetService = getClientJetService();

        SubmitJobParameters submitJobParameters = SubmitJobParameters.withJarOnClient()
                .setJarPath(getJarPath());

        assertThatThrownBy(() -> jetService.submitJobFromJar(submitJobParameters))
                .isInstanceOf(JetException.class)
                .hasMessage("The upload directory path does not exist: directorynotexists");
    }


    @Test
    public void test_jarUpload_whenJobIsJoining() {
        createCluster();
        JetClientInstanceImpl jetService = getClientJetService();

        String jobName = "joiningJob";
        SubmitJobParameters submitJobParameters = SubmitJobParameters.withJarOnClient()
                .setJarPath(getJoiningJarPath())
                .setJobName(jobName);

        assertThatThrownBy(() -> jetService.submitJobFromJar(submitJobParameters))
                .isInstanceOf(JetException.class)
                .hasStackTraceContaining("The job has started successfully. However the job should not call the join() method.\n" +
                                         "Please remove the join() call");

        assertTrueEventually(() -> {
            List<Job> jobs = jetService.getJobs();
            assertEquals(1, jobs.size());
            assertTrue(containsName(jobs, jobName));
        });
    }

    private void createCluster() {
        Config config = smallInstanceConfig();
        JetConfig jetConfig = config.getJetConfig();
        jetConfig.setResourceUploadEnabled(true);

        createHazelcastInstance(config);
    }

    public void createClusterWithUploadDirectoryPath(String uploadDirectoryPath) {
        Config config = smallInstanceConfig();
        config.setProperty(ClusterProperty.JAR_UPLOAD_DIR_PATH.getName(), uploadDirectoryPath);
        JetConfig jetConfig = config.getJetConfig();
        jetConfig.setResourceUploadEnabled(true);

        createHazelcastInstance(config);
    }

    private HazelcastInstance[] createMultiNodeCluster() {
        Config config = smallInstanceConfig();
        JetConfig jetConfig = config.getJetConfig();
        jetConfig.setResourceUploadEnabled(true);

        return createHazelcastInstances(config, 2);
    }

    private JetClientInstanceImpl getClientJetService() {
        HazelcastInstance client = createHazelcastClient();
        return (JetClientInstanceImpl) client.getJet();
    }

    // this jar is only as below
    // Source is https://github.com/OrcunColak/simplejob.git
    /*
    public static void main(String[] args) {

        String jobName = null;
        if (args != null && args.length > 0) {
            jobName = args[0];
        }

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.itemStream(10))
                .withoutTimestamps()
                .filter(event -> event.sequence() % 2 == 0)
                .setName("filter out odd numbers")
                .writeTo(Sinks.logger());

        HazelcastInstance hz = Hazelcast.bootstrappedInstance();

        if (jobName != null) {
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(jobName);
            hz.getJet().newJob(pipeline, jobConfig);
        } else {
            hz.getJet().newJob(pipeline);
        }
    }
    */
    public static Path getJarPath() {
        return getPath(SIMPLE_JAR);
    }

    public static Path getParalleJarPath() {
        return getPath(PARALLEL_JAR);
    }

    public static Path getJoiningJarPath() {
        return getPath(JOINING_JAR);
    }
    static Path copyJar(String newJarPath) throws IOException {
        // Copy as new jar
        Path jarPath = getJarPath();
        Path newPath = jarPath.resolveSibling(newJarPath);
        return Files.copy(jarPath, newPath, StandardCopyOption.REPLACE_EXISTING);
    }

    public static boolean jarDoesNotExistInTempDirectory() throws IOException {
        return fileDoesNotExist(SIMPLE_JAR);
    }

    public static Path getNoManifestJarPath() {
        return getPath(NO_MANIFEST_SIMPLE_JAR);
    }

    private static Path getPath(String jarName) {
        ClassLoader classLoader = JobUploadClientFailureTest.class.getClassLoader();
        URL resource = classLoader.getResource(jarName);
        Path result = null;
        try {
            assert resource != null;
            result = Paths.get(resource.toURI());
        } catch (Exception exception) {
            fail("Unable to get jar path from :" + jarName);
        }
        return result;
    }

    private void deleteLeftOverFilesIfAny(String newJarName) throws IOException {
        Path tempDirectory = Paths.get(System.getProperty("java.io.tmpdir"));
        try (Stream<Path> stream = Files.list(tempDirectory)) {
            stream.forEach(fullPath -> {
                String fileName = fullPath.getFileName().toString();
                if (fileName.contains(newJarName)) {
                    try {
                        Files.delete(fullPath);
                    } catch (IOException ignored) {
                    }
                }
            });
        }
    }

    private static boolean fileDoesNotExist(String newJarName) throws IOException {
        // Get default temp directory
        Path tempDirectory = Paths.get(System.getProperty("java.io.tmpdir"));

        try (Stream<Path> stream = Files.list(tempDirectory)) {
            return stream.noneMatch(fullPath -> {
                Path fileName = fullPath.getFileName();
                return fileName.startsWith(newJarName);
            });
        }
    }

    public static boolean containsName(List<Job> list, String name) {
        return list.stream().anyMatch(job -> Objects.equals(job.getName(), name));
    }

    private static void assertJobIsNotRunning(JetService jetService) {
        // Assert job size
        assertEqualsEventually(() -> jetService.getJobs().size(), 0);
    }

    public static void assertJobIsRunning(JetService jetService) throws IOException {
        // Assert job size
        assertEqualsEventually(() -> jetService.getJobs().size(), 1);

        // Assert job status
        Job job = jetService.getJobs().get(0);
        assertThat(job).eventuallyHasStatus(JobStatus.RUNNING);

        // Assert job jar does is deleted
        jarDoesNotExistInTempDirectory();
    }
}
