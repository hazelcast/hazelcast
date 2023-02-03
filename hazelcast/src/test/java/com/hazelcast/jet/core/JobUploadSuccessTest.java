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
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.util.Collections.emptyList;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category({SerialTest.class})
public class JobUploadSuccessTest extends JetTestSupport {

    @After
    public void resetSingleton() {
        // Reset the singleton after the test
        HazelcastBootstrap.resetSupplier();
    }

    @Test
    public void sha256() throws IOException, NoSuchAlgorithmException {
        Path jarPath = getJarPath();
        String sha256Hex = Sha256Util.calculateSha256Hex(jarPath);
        assertEquals("bba07be19c71bfe5fd51dc681f807ab212efd4d6e7c3f6380dbfcbdcb5deb073", sha256Hex);
    }

    @Test
    public void test_jarUploadByClient_whenResourceUploadIsEnabled() {
        Config config = smallInstanceConfig();
        JetConfig jetConfig = config.getJetConfig();
        jetConfig.setResourceUploadEnabled(true);

        createHazelcastInstance(config);
        HazelcastInstance client = createHazelcastClient();
        JetService jetService = client.getJet();

        SubmitJobParameters submitJobParameters = new SubmitJobParameters()
                .setJarPath(getJarPath());

        jetService.submitJobFromJar(submitJobParameters);

        assertEqualsEventually(() -> jetService.getJobs().size(), 1);
    }

    @Test
    public void test_jarUploadByNonSmartClient_whenResourceUploadIsEnabled() {
        Config config = smallInstanceConfig();
        JetConfig jetConfig = config.getJetConfig();
        jetConfig.setResourceUploadEnabled(true);

        createHazelcastInstances(config, 2);

        ClientConfig clientConfig = new ClientConfig();
        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
        networkConfig.setSmartRouting(false);
        HazelcastInstance client = createHazelcastClient(clientConfig);
        JetService jetService = client.getJet();

        SubmitJobParameters submitJobParameters = new SubmitJobParameters()
                .setJarPath(getJarPath());

        jetService.submitJobFromJar(submitJobParameters);

        assertEqualsEventually(() -> jetService.getJobs().size(), 1);
    }

    @Test
    public void test_jarUploadByClient_withMainClassname() {
        Config config = smallInstanceConfig();
        JetConfig jetConfig = config.getJetConfig();
        jetConfig.setResourceUploadEnabled(true);

        createHazelcastInstance(config);
        HazelcastInstance client = createHazelcastClient();
        JetService jetService = client.getJet();
        List<String> jobParameters = emptyList();

        SubmitJobParameters submitJobParameters = new SubmitJobParameters()
                .setJarPath(getJarPath())
                .setMainClass("org.example.Main")
                .setJobParameters(jobParameters);

        jetService.submitJobFromJar(submitJobParameters);

        assertEqualsEventually(() -> jetService.getJobs().size(), 1);
    }


    @Test
    public void test_jarUploadByMember_whenResourceUploadIsEnabled() {
        Config config = smallInstanceConfig();
        JetConfig jetConfig = config.getJetConfig();
        jetConfig.setResourceUploadEnabled(true);

        HazelcastInstance hazelcastInstance = createHazelcastInstance(config);
        JetService jetService = hazelcastInstance.getJet();

        SubmitJobParameters submitJobParameters = new SubmitJobParameters()
                .setJarPath(getJarPath());

        jetService.submitJobFromJar(submitJobParameters);

        assertEqualsEventually(() -> jetService.getJobs().size(), 1);
    }

    @Test
    public void test_jarUploadByMember_withMainClassname() {
        Config config = smallInstanceConfig();
        JetConfig jetConfig = config.getJetConfig();
        jetConfig.setResourceUploadEnabled(true);

        HazelcastInstance hazelcastInstance = createHazelcastInstance(config);
        JetService jetService = hazelcastInstance.getJet();

        SubmitJobParameters submitJobParameters = new SubmitJobParameters()
                .setJarPath(getJarPath())
                .setMainClass("org.example.Main");

        jetService.submitJobFromJar(submitJobParameters);

        assertEqualsEventually(() -> jetService.getJobs().size(), 1);
    }

    @Test
    public void test_jarUpload_whenResourceUploadIsEnabled_withSmallBuffer() {
        Config config = smallInstanceConfig();
        JetConfig jetConfig = config.getJetConfig();
        jetConfig.setResourceUploadEnabled(true);

        createHazelcastInstance(config);

        // Change the part buffer size
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(ClientProperty.JOB_UPLOAD_PART_SIZE.getName(), "100");
        HazelcastInstance client = createHazelcastClient(clientConfig);

        JetService jetService = client.getJet();

        SubmitJobParameters submitJobParameters = new SubmitJobParameters()
                .setJarPath(getJarPath());

        jetService.submitJobFromJar(submitJobParameters);

        assertEqualsEventually(() -> jetService.getJobs().size(), 1);
    }

    // This test is slow because it is trying to upload a lot of jobs
    @Category({SlowTest.class})
    @Test
    public void test_stress_jarUpload_whenResourceUploadIsEnabled() {
        Config config = smallInstanceConfig();
        JetConfig jetConfig = config.getJetConfig();
        jetConfig.setResourceUploadEnabled(true);

        createHazelcastInstance(config);
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
        Config config = smallInstanceConfig();
        JetConfig jetConfig = config.getJetConfig();
        jetConfig.setResourceUploadEnabled(true);

        createHazelcastInstance(config);
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

        // Let the jobs run
        sleepAtLeastSeconds(5);
    }

    private boolean containsName(List<Job> list, String name) {
        return list.stream().anyMatch(job -> Objects.equals(job.getName(), name));
    }

    // this jar is only as below
    // Source is https://github.com/OrcunColak/simplejob.git
    /*
     public class Main {
       public static void main(String[] args) {

         Pipeline pipeline = Pipeline.create();
         pipeline.readFrom(TestSources.itemStream(10))
           .withoutTimestamps()
           .filter(event -> event.sequence() % 2 == 0)
           .setName("filter out odd numbers")
           .writeTo(Sinks.logger());

         HazelcastInstance hz = Hazelcast.bootstrappedInstance();
         hz.getJet().newJob(pipeline);
       }
     }*/
    private Path getJarPath() {
        ClassLoader classLoader = getClass().getClassLoader();
        URL resource = classLoader.getResource("simplejob-1.0.0.jar");
        Path result = null;
        try {
            assert resource != null;
            result = Paths.get(resource.toURI());
        } catch (Exception exception) {
            fail("Unable to get jar path from :" + resource);
        }
        return result;
    }
}
