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
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.properties.ClientProperty;
import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastBootstrap;
import com.hazelcast.internal.util.Sha256Util;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.SubmitJobParameters;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.test.SerialTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.hazelcast.jet.core.JobUploadClientFailureTest.getJarPath;
import static com.hazelcast.jet.core.JobUploadClientFailureTest.jarDoesNotExist;
import static java.util.Collections.emptyList;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

@Category({SerialTest.class})
public class JobUploadClientSuccessTest extends JetTestSupport {

    @After
    public void resetSingleton() {
        // Reset the singleton after the test
        HazelcastBootstrap.resetSupplier();
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

        JetService jetService = getClientJetService();

        SubmitJobParameters submitJobParameters = new SubmitJobParameters()
                .setJarPath(getJarPath());

        jetService.submitJobFromJar(submitJobParameters);

        assertJobIsRunning(jetService);
    }

    @Test
    public void test_jarUpload_withJobParameters() throws IOException {
        createCluster();
        JetService jetService = getClientJetService();

        // Pass the job argument that will be used as job name
        String jobName = "myjetjob";
        SubmitJobParameters submitJobParameters = new SubmitJobParameters()
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
    public void test_jarUploadByNonSmartClient_whenResourceUploadIsEnabled() throws IOException {
        HazelcastInstance[] hazelcastInstances = createCluster(2);

        // Get address of the member that is not Job Coordinator
        HazelcastInstance targetInstance = hazelcastInstances[1];
        Address targetAddress = targetInstance.getCluster().getLocalMember().getAddress();

        // Create a non-smart client
        ClientConfig clientConfig = new ClientConfig();
        ClientNetworkConfig clientNetworkConfig = clientConfig.getNetworkConfig();
        clientNetworkConfig.setSmartRouting(false);

        // Set the target address for non-smart client
        List<String> addresses = clientNetworkConfig.getAddresses();
        addresses.add(targetAddress.getHost() + ":" + targetAddress.getPort());

        // Create client and submit job
        HazelcastInstance client = createHazelcastClient(clientConfig);
        JetService jetService = client.getJet();

        SubmitJobParameters submitJobParameters = new SubmitJobParameters()
                .setJarPath(getJarPath());

        jetService.submitJobFromJar(submitJobParameters);

        assertJobIsRunning(jetService);
    }

    @Test
    public void test_jarUpload_withMainClassname() throws IOException {
        createCluster();

        // Create client and submit job
        JetService jetService = getClientJetService();
        List<String> jobParameters = emptyList();

        SubmitJobParameters submitJobParameters = new SubmitJobParameters()
                .setJarPath(getJarPath())
                .setMainClass("org.example.Main")
                .setJobParameters(jobParameters);

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

        JetService jetService = client.getJet();

        SubmitJobParameters submitJobParameters = new SubmitJobParameters()
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

        int jobLimit = 50;
        for (int index = 0; index < jobLimit; index++) {
            executorService.submit(() -> {
                HazelcastInstance client = createHazelcastClient();
                JetService jetService = client.getJet();

                SubmitJobParameters submitJobParameters = new SubmitJobParameters()
                        .setJarPath(getJarPath());

                jetService.submitJobFromJar(submitJobParameters);
                client.shutdown();
            });
        }

        HazelcastInstance client = createHazelcastClient();
        JetService jetService = client.getJet();
        assertEqualsEventually(() -> jetService.getJobs().size(), jobLimit);
    }

    @Test
    public void test_multipleJarUploads_whenResourceUploadIsEnabled() {
        createCluster();

        HazelcastInstance client = createHazelcastClient();
        JetService jetService = client.getJet();

        String job1 = "job1";
        SubmitJobParameters submitJobParameters1 = new SubmitJobParameters()
                .setJarPath(getJarPath())
                .setJobName(job1);

        jetService.submitJobFromJar(submitJobParameters1);

        String job2 = "job2";
        SubmitJobParameters submitJobParameters2 = new SubmitJobParameters()
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

    private void createCluster() {
        Config config = smallInstanceConfig();
        JetConfig jetConfig = config.getJetConfig();
        jetConfig.setResourceUploadEnabled(true);

        createHazelcastInstance(config);
    }

    private HazelcastInstance[] createCluster(int nodeCount) {
        Config config = smallInstanceConfig();
        JetConfig jetConfig = config.getJetConfig();
        jetConfig.setResourceUploadEnabled(true);

        return createHazelcastInstances(config, nodeCount);
    }

    private JetService getClientJetService() {
        HazelcastInstance client = createHazelcastClient();
        return client.getJet();
    }

    static void assertJobIsRunning(JetService jetService) throws IOException {
        // Assert job size
        assertEqualsEventually(() -> jetService.getJobs().size(), 1);

        // Assert job status
        Job job = jetService.getJobs().get(0);
        assertJobStatusEventually(job, JobStatus.RUNNING);

        // Assert job jar does is deleted
        jarDoesNotExist();
    }

    private boolean containsName(List<Job> list, String name) {
        return list.stream().anyMatch(job -> Objects.equals(job.getName(), name));
    }
}
