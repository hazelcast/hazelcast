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

package com.hazelcast.jet.impl.submitjob.clientside.execute;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.config.RoutingMode;
import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastBootstrap;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.impl.JetClientInstanceImpl;
import com.hazelcast.jet.impl.SubmitJobParameters;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.hazelcast.jet.core.JobAssertions.assertThat;
import static com.hazelcast.jet.impl.submitjob.clientside.upload.JobUploadClientFailureTest.containsName;
import static com.hazelcast.jet.impl.submitjob.clientside.upload.JobUploadClientFailureTest.getJarPath;
import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

// Tests to execute existing jar on the member
@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class JobExecuteClientSuccessTest extends JetTestSupport {

    @After
    public void resetSingleton() {
        // Reset the singleton after the test
        HazelcastBootstrap.resetRemembered();
    }

    @Test
    public void test_jarExecute_whenResourceUploadIsEnabled() {
        createCluster();
        JetClientInstanceImpl jetService = getClientJetService();

        Path jarPath = getJarPath();
        SubmitJobParameters submitJobParameters = SubmitJobParameters.withJarOnMember()
                .setJarPath(jarPath);

        jetService.submitJobFromJar(submitJobParameters);

        assertJobIsRunning(jetService, jarPath);
    }

    @Test
    public void test_jarExecute_withJobParameters() {
        createCluster();
        JetClientInstanceImpl jetService = getClientJetService();

        // Pass the job argument that will be used as job name
        String jobName = "myjetjob";
        Path jarPath = getJarPath();
        SubmitJobParameters submitJobParameters = SubmitJobParameters.withJarOnMember()
                .setJarPath(jarPath)
                .setJobParameters(Collections.singletonList(jobName));

        jetService.submitJobFromJar(submitJobParameters);

        assertJobIsRunning(jetService, jarPath);

        assertEqualsEventually(() -> {
            Job job = jetService.getJobs().get(0);
            return job.getName();
        }, jobName);
    }

    @Test
    public void test_jarExecuteBySingleMemberClient_whenResourceUploadIsEnabled() {
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

        Path jarPath = getJarPath();
        SubmitJobParameters submitJobParameters = SubmitJobParameters.withJarOnMember()
                .setJarPath(jarPath);

        jetService.submitJobFromJar(submitJobParameters);

        assertJobIsRunning(jetService, jarPath);
    }

    @Test
    public void test_jarExecute_withMainClassname() {
        createCluster();
        // Create client and submit job
        JetClientInstanceImpl jetService = getClientJetService();
        List<String> jobParameters = emptyList();

        Path jarPath = getJarPath();
        SubmitJobParameters submitJobParameters = SubmitJobParameters.withJarOnMember()
                .setJarPath(jarPath)
                .setMainClass("org.example.Main")
                .setJobParameters(jobParameters);

        jetService.submitJobFromJar(submitJobParameters);

        assertJobIsRunning(jetService, jarPath);
    }


    // This test is slow because it is trying to upload a lot of jobs
    @Category({SlowTest.class})
    @Test
    public void test_stress_jarExecute_whenResourceUploadIsEnabled() {
        createCluster();

        ExecutorService executorService = Executors.newFixedThreadPool(10);

        int jobLimit = 50;
        for (int index = 0; index < jobLimit; index++) {
            executorService.submit(() -> {
                HazelcastInstance client = createHazelcastClient();
                JetClientInstanceImpl jetService = (JetClientInstanceImpl) client.getJet();

                SubmitJobParameters submitJobParameters = SubmitJobParameters.withJarOnMember()
                        .setJarPath(getJarPath());

                jetService.submitJobFromJar(submitJobParameters);
                client.shutdown();
            });
        }

        HazelcastInstance client = createHazelcastClient();
        JetService jetService = client.getJet();
        try {
            assertEqualsEventually(() -> jetService.getJobs().size(), jobLimit);
        } finally {
            executorService.shutdownNow();
        }
    }

    @Test
    public void test_multipleJarExecutes_whenResourceUploadIsEnabled() {
        createCluster();
        JetClientInstanceImpl jetService = getClientJetService();

        String job1 = "job1";
        SubmitJobParameters submitJobParameters1 = SubmitJobParameters.withJarOnMember()
                .setJarPath(getJarPath())
                .setJobName(job1);

        jetService.submitJobFromJar(submitJobParameters1);

        String job2 = "job2";
        SubmitJobParameters submitJobParameters2 = SubmitJobParameters.withJarOnMember()
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

    static void assertJobIsRunning(JetService jetService, Path jarPath) {
        // Assert job size
        assertEqualsEventually(() -> jetService.getJobs().size(), 1);

        // Assert job status
        Job job = jetService.getJobs().get(0);
        assertThat(job).eventuallyHasStatus(JobStatus.RUNNING);

        // Assert job jar is not deleted
        assertTrue(Files.exists(jarPath));
    }
}
