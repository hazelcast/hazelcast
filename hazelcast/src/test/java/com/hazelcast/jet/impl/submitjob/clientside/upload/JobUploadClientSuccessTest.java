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
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.config.RoutingMode;
import com.hazelcast.client.properties.ClientProperty;
import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastBootstrap;
import com.hazelcast.internal.util.Sha256Util;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.impl.JetClientInstanceImpl;
import com.hazelcast.jet.impl.SubmitJobParameters;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static com.hazelcast.jet.impl.submitjob.clientside.upload.JobUploadClientFailureTest.assertJobIsRunning;
import static com.hazelcast.jet.impl.submitjob.clientside.upload.JobUploadClientFailureTest.containsName;
import static com.hazelcast.jet.impl.submitjob.clientside.upload.JobUploadClientFailureTest.getJarPath;
import static org.junit.Assert.assertEquals;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

// Tests to upload jar to member
@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class JobUploadClientSuccessTest extends JetTestSupport {

    @After
    public void resetSingleton() {
        // Reset the singleton after the test
        HazelcastBootstrap.resetRemembered();
    }

    @Test
    public void sha256() throws IOException, NoSuchAlgorithmException {
        Path jarPath = getJarPath();
        String sha256Hex = Sha256Util.calculateSha256Hex(jarPath);
        assertEquals("b1c93019597f7cb6d17d98b720837b3b0b7187b231844c4213f6b372308b118f", sha256Hex);
    }

    @Test
    public void test_jarUpload_whenResourceUploadIsEnabled() throws IOException {
        createCluster();
        JetClientInstanceImpl jetService = getClientJetService();

        SubmitJobParameters submitJobParameters = SubmitJobParameters.withJarOnClient()
                .setJarPath(getJarPath());

        jetService.submitJobFromJar(submitJobParameters);

        assertJobIsRunning(jetService);
    }

    @Test
    public void test_jarUpload_withJobParameters() throws IOException {
        createCluster();
        JetClientInstanceImpl jetService = getClientJetService();

        // Pass the job argument that will be used as job name
        String jobName = "myjetjob";
        SubmitJobParameters submitJobParameters = SubmitJobParameters.withJarOnClient()
                .setJarPath(getJarPath())
                .setJobParameters(Collections.singletonList(jobName));

        jetService.submitJobFromJar(submitJobParameters);

        assertJobIsRunning(jetService);

        assertEqualsEventually(() -> {
            Job job = jetService.getJobs().get(0);
            return job.getName();
        }, jobName);
    }

    @Test
    public void test_jarUploadBySingleMemberClient_whenResourceUploadIsEnabled() throws IOException {
        HazelcastInstance[] hazelcastInstances = createMultiNodeCluster();

        // Get address of the member that is not Job Coordinator
        HazelcastInstance targetInstance = hazelcastInstances[1];
        Address targetAddress = targetInstance.getCluster().getLocalMember().getAddress();

        // Create a SINGLE_MEMBER routing client
        ClientConfig clientConfig = new ClientConfig();
        ClientNetworkConfig clientNetworkConfig = clientConfig.getNetworkConfig();
        clientNetworkConfig.getClusterRoutingConfig().setRoutingMode(RoutingMode.SINGLE_MEMBER);

        // Set the target address for SINGLE_MEMBER routing client
        List<String> addresses = clientNetworkConfig.getAddresses();
        addresses.add(targetAddress.getHost() + ":" + targetAddress.getPort());

        // Create client and submit job
        HazelcastInstance client = createHazelcastClient(clientConfig);
        JetClientInstanceImpl jetService = (JetClientInstanceImpl) client.getJet();

        SubmitJobParameters submitJobParameters = SubmitJobParameters.withJarOnClient()
                .setJarPath(getJarPath());

        jetService.submitJobFromJar(submitJobParameters);

        assertJobIsRunning(jetService);
    }

    @Test
    public void test_jarUpload_withMainClassname() throws IOException {
        createCluster();
        // Create client and submit job
        JetClientInstanceImpl jetService = getClientJetService();

        SubmitJobParameters submitJobParameters = SubmitJobParameters.withJarOnClient()
                .setJarPath(getJarPath())
                .setMainClass("org.example.Main");

        jetService.submitJobFromJar(submitJobParameters);

        assertJobIsRunning(jetService);
    }

    @Test
    public void test_jarUpload_whenResourceUploadIsEnabled_withSmallBuffer() throws IOException {
        createCluster();

        // Change the part buffer size
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(ClientProperty.JOB_UPLOAD_PART_SIZE.getName(), "100");
        HazelcastInstance client = createHazelcastClient(clientConfig);

        JetClientInstanceImpl jetService = (JetClientInstanceImpl) client.getJet();

        SubmitJobParameters submitJobParameters = SubmitJobParameters.withJarOnClient()
                .setJarPath(getJarPath());

        jetService.submitJobFromJar(submitJobParameters);

        assertJobIsRunning(jetService);
    }

    // This test is slow because it is trying to upload a lot of jobs
    @Category({SlowTest.class})
    @Test
    public void test_stress_jarUpload_whenResourceUploadIsEnabled() {
        createCluster();

        ExecutorService executorService = Executors.newFixedThreadPool(10);

        // Shared client for ExecutorService threads
        HazelcastInstance client = createHazelcastClient();
        JetClientInstanceImpl jetService = (JetClientInstanceImpl) client.getJet();

        ConcurrentSkipListSet<String> submittedJobNames = new ConcurrentSkipListSet<>();
        int jobLimit = 50;
        for (int index = 0; index < jobLimit; index++) {
            int value = index;
            executorService.submit(() -> {
                String jobName = "job-" + value;
                SubmitJobParameters submitJobParameters = SubmitJobParameters.withJarOnClient()
                        .setJarPath(getJarPath())
                        .setJobName(jobName);

                jetService.submitJobFromJar(submitJobParameters);
                submittedJobNames.add(jobName);
            });
        }
        assertEqualsEventually(() -> jetService.getJobs().size(), jobLimit);

        assertTrueEventually(() -> {
            TreeSet<String> jobNames = jetService.getJobs().stream()
                    .map(Job::getName)
                    .collect(Collectors.toCollection(TreeSet::new));

            assertThat(jobNames).containsAll(submittedJobNames);
        });

        executorService.shutdownNow();
    }

    @Test
    public void test_multipleJarUploads_whenResourceUploadIsEnabled() {
        createCluster();
        JetClientInstanceImpl jetService = getClientJetService();

        String job1 = "job1";
        SubmitJobParameters submitJobParameters1 = SubmitJobParameters.withJarOnClient()
                .setJarPath(getJarPath())
                .setJobName(job1);

        jetService.submitJobFromJar(submitJobParameters1);

        String job2 = "job2";
        SubmitJobParameters submitJobParameters2 = SubmitJobParameters.withJarOnClient()
                .setJarPath(getJarPath())
                .setJobName(job2);
        jetService.submitJobFromJar(submitJobParameters2);

        assertTrueEventually(() -> {
            List<Job> jobs = jetService.getJobs();
            assertEquals(2, jobs.size());
            assertTrue(containsName(jobs, job1));
            assertTrue(containsName(jobs, job2));
        });
    }

    @Test
    public void test_jarUpload_tempdir_whenResourceUploadIsEnabled() throws IOException {
        Path path = Paths.get("target/jardirectory");
        try {
            // Make sure the temp directory exists
            Files.createDirectories(path);
            String directoryPath = path.toString();

            // Start cluster with the temp directory path
            createClusterWithUploadDirectoryPath(directoryPath);

            JetClientInstanceImpl jetService = getClientJetService();

            SubmitJobParameters submitJobParameters = SubmitJobParameters.withJarOnClient()
                    .setJarPath(getJarPath());

            jetService.submitJobFromJar(submitJobParameters);

            assertJobIsRunning(jetService);

            // After a job is executed the jar is deleted. Test that no file exists in the temp dir
            File directory = new File(directoryPath);
            File[] jarFiles = directory.listFiles((dir, name) -> name.endsWith(".jar"));
            assertThat(jarFiles).isNotNull().isEmpty();
        } finally {
            // Cleanup
            Files.deleteIfExists(path);
        }
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

}
