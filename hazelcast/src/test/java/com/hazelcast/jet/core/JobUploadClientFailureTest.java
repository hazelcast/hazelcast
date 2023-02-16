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

package com.hazelcast.jet.core;

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
import com.hazelcast.jet.SubmitJobParameters;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.impl.JetClientInstanceImpl;
import com.hazelcast.jet.impl.JobUploadCall;
import com.hazelcast.jet.test.SerialTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.URL;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Stream;

import static junit.framework.TestCase.assertEquals;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doAnswer;

@Category({SerialTest.class})
public class JobUploadClientFailureTest extends JetTestSupport {

    private static final String SIMPLE_JAR = "simplejob-1.0.0.jar";
    private static final String NO_MANIFEST_SIMPLE_JAR = "nomanifestsimplejob-1.0.0.jar";

    @After
    public void resetSingleton() {
        // Reset the singleton after the test
        HazelcastBootstrap.resetSupplier();
    }

    @Test
    public void testNullJarPath() {
        createHazelcastInstance();
        HazelcastInstance client = createHazelcastClient();
        JetService jetService = client.getJet();

        SubmitJobParameters submitJobParameters = new SubmitJobParameters();

        assertThatThrownBy(() -> jetService.submitJobFromJar(submitJobParameters))
                .isInstanceOf(JetException.class)
                .hasMessageContaining("jarPath can not be null");
    }

    @Test
    public void testNullJobParameters() {
        createHazelcastInstance();
        HazelcastInstance client = createHazelcastClient();
        JetService jetService = client.getJet();

        SubmitJobParameters submitJobParameters = new SubmitJobParameters();
        submitJobParameters.setJarPath(getJarPath());
        submitJobParameters.setJobParameters(null);

        assertThatThrownBy(() -> jetService.submitJobFromJar(submitJobParameters))
                .isInstanceOf(JetException.class)
                .hasMessageContaining("jobParameters can not be null");
    }

    @Test
    public void testNullMainClass() {
        HazelcastInstance client = createCluster();
        JetService jetService = client.getJet();

        SubmitJobParameters submitJobParameters = new SubmitJobParameters()
                .setJarPath(getNoManifestJarPath());

        assertThatThrownBy(() -> jetService.submitJobFromJar(submitJobParameters))
                .isInstanceOf(JetException.class)
                .hasStackTraceContaining("No Main-Class found in the manifest");
    }

    @Test
    public void testTooLongFileName() throws IOException, NoSuchAlgorithmException {
        HazelcastInstance client = createCluster();
        JetService jetService = client.getJet();
        JetClientInstanceImpl spyJetService = (JetClientInstanceImpl) Mockito.spy(jetService);

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


        SubmitJobParameters submitJobParameters = new SubmitJobParameters()
                .setJarPath(jarPath);

        assertThatThrownBy(() -> spyJetService.submitJobFromJar(submitJobParameters))
                .isInstanceOf(JetException.class)
                .hasCauseInstanceOf(FileSystemException.class);
    }

    @Test
    public void test_jarUpload_whenResourceUploadIsNotEnabled() {
        createHazelcastInstance();
        HazelcastInstance client = createHazelcastClient();
        JetService jetService = client.getJet();

        SubmitJobParameters submitJobParameters = new SubmitJobParameters()
                .setJarPath(getJarPath());

        assertThrows(JetException.class, () ->
                jetService.submitJobFromJar(submitJobParameters)
        );

        assertEqualsEventually(() -> jetService.getJobs().size(), 0);
    }

    @Test
    public void test_jarUpload_withWrongMainClassname() {
        HazelcastInstance client = createCluster();
        JetService jetService = client.getJet();

        SubmitJobParameters submitJobParameters = new SubmitJobParameters()
                .setJarPath(getJarPath())
                .setMainClass("org.example.Main1");

        assertThrows(JetException.class, () -> jetService.submitJobFromJar(submitJobParameters));
    }


    @Test
    public void test_jar_isDeleted() throws IOException {
        Path newPath = null;
        try {
            // Delete left over files before the test
            String newSimpleJob = "newsimplejob.jar";
            deleteLeftOverFilesIfAny(newSimpleJob);

            HazelcastInstance client = createCluster();
            JetService jetService = client.getJet();

            // Copy as new jar to make it unique
            newPath = copyJar(newSimpleJob);

            SubmitJobParameters submitJobParameters = new SubmitJobParameters()
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
        HazelcastInstance client = createCluster();

        // Mock the JetClientInstanceImpl to return an incorrect checksum
        JetClientInstanceImpl jetService = (JetClientInstanceImpl) client.getJet();
        JetClientInstanceImpl spyJetService = Mockito.spy(jetService);

        Path jarPath = getJarPath();
        doAnswer(invocation -> {
            JobUploadCall jobUploadCall = (JobUploadCall) invocation.callRealMethod();
            jobUploadCall.setSha256HexOfJar("1");
            return jobUploadCall;
        }).when(spyJetService).initializeJobUploadCall(jarPath);


        SubmitJobParameters submitJobParameters = new SubmitJobParameters()
                .setJarPath(getJarPath());

        assertThrows(JetException.class, () -> spyJetService.submitJobFromJar(submitJobParameters));

        assertEqualsEventually(() -> jetService.getJobs().size(), 0);
    }

    @Test
    public void test_jobAlreadyExists() {
        createCluster();

        HazelcastInstance client = createHazelcastClient();
        JetService jetService = client.getJet();

        String job1 = "job1";
        SubmitJobParameters submitJobParameters1 = new SubmitJobParameters()
                .setJarPath(getJarPath())
                .setJobName(job1);

        jetService.submitJobFromJar(submitJobParameters1);


        String job2 = "job1";
        SubmitJobParameters submitJobParameters2 = new SubmitJobParameters()
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
        HazelcastInstance[] cluster = createCluster(2);

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
            for (HazelcastInstance hazelcastInstance : cluster) {
                if (hazelcastInstance.getCluster().getLocalMember().getUuid().equals(memberUuid)) {
                    hazelcastInstance.shutdown();
                    break;
                }
            }
            assertClusterSizeEventually(1, client);
            return jobUploadCall;
        }).when(spyJetService).initializeJobUploadCall(jarPath);

        SubmitJobParameters submitJobParameters = new SubmitJobParameters()
                .setJarPath(getJarPath());

        // Should throw OperationTimeoutException because target instance was shut down
        assertThrows(OperationTimeoutException.class, () -> spyJetService.submitJobFromJar(submitJobParameters));
    }

    private HazelcastInstance createCluster() {
        Config config = smallInstanceConfig();
        JetConfig jetConfig = config.getJetConfig();
        jetConfig.setResourceUploadEnabled(true);

        createHazelcastInstance(config);
        return createHazelcastClient();
    }

    private HazelcastInstance[] createCluster(int nodeCount) {
        Config config = smallInstanceConfig();
        JetConfig jetConfig = config.getJetConfig();
        jetConfig.setResourceUploadEnabled(true);

        return createHazelcastInstances(config, nodeCount);
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

    static Path copyJar(String newJarPath) throws IOException {
        // Copy as new jar
        Path jarPath = getJarPath();
        Path newPath = jarPath.resolveSibling(newJarPath);
        return Files.copy(jarPath, newPath, StandardCopyOption.REPLACE_EXISTING);
    }

    static boolean jarDoesNotExist() throws IOException {
        return fileDoesNotExist(SIMPLE_JAR);
    }

    private Path getNoManifestJarPath() {
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

    static boolean containsName(List<Job> list, String name) {
        return list.stream().anyMatch(job -> Objects.equals(job.getName(), name));
    }
}
