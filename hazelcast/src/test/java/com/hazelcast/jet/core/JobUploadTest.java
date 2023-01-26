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
import com.hazelcast.instance.impl.HazelcastBootstrap;
import com.hazelcast.internal.util.Sha256Util;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.SubmitJobParameters;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.impl.JetClientInstanceImpl;
import com.hazelcast.jet.test.SerialTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

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
import static org.mockito.Mockito.when;

@Category({SerialTest.class})
public class JobUploadTest extends JetTestSupport {

    @Test
    public void sha256() throws IOException, NoSuchAlgorithmException {
        Path jarPath = getJarPath();
        String sha256Hex = Sha256Util.calculateSha256Hex(jarPath);
        assertEquals("bba07be19c71bfe5fd51dc681f807ab212efd4d6e7c3f6380dbfcbdcb5deb073", sha256Hex);
    }

    @Test
    public void test_client_jarUpload_whenResourceUploadIsNotEnabled() {
        HazelcastInstance hazelcastInstance = createHazelcastInstance();
        HazelcastInstance client = createHazelcastClient();
        JetService jetService = client.getJet();
        List<String> jobParameters = emptyList();

        SubmitJobParameters submitJobParameters = new SubmitJobParameters();
        submitJobParameters.setJarPath(getJarPath());
        submitJobParameters.setJobParameters(jobParameters);

        assertThrows(JetException.class, () ->
                jetService.submitJobFromJar(submitJobParameters)
        );

        assertEqualsEventually(() -> jetService.getJobs().size(), 0);
        hazelcastInstance.shutdown();
    }

    @Test
    public void test_member_jarUpload_whenResourceUploadIsNotEnabled() {
        HazelcastInstance hazelcastInstance = createHazelcastInstance();
        JetService jetService = hazelcastInstance.getJet();
        List<String> jobParameters = emptyList();

        SubmitJobParameters submitJobParameters = new SubmitJobParameters();
        submitJobParameters.setJarPath(getJarPath());
        submitJobParameters.setJobParameters(jobParameters);

        assertThrows(JetException.class, () ->
                jetService.submitJobFromJar(submitJobParameters)
        );

        assertEqualsEventually(() -> jetService.getJobs().size(), 0);
        hazelcastInstance.shutdown();
    }

    @Test
    public void test_jarUploadByClient_whenResourceUploadIsEnabled() {
        // Reset the singleton because a new HazelcastInstance will be created for the test
        HazelcastBootstrap.resetSupplier();

        Config config = smallInstanceConfig();
        JetConfig jetConfig = config.getJetConfig();
        jetConfig.setResourceUploadEnabled(true);

        HazelcastInstance hazelcastInstance = createHazelcastInstance(config);
        HazelcastInstance client = createHazelcastClient();
        JetService jetService = client.getJet();
        List<String> jobParameters = emptyList();

        SubmitJobParameters submitJobParameters = new SubmitJobParameters();
        submitJobParameters.setJarPath(getJarPath());
        submitJobParameters.setJobParameters(jobParameters);

        jetService.submitJobFromJar(submitJobParameters);

        assertEqualsEventually(() -> jetService.getJobs().size(), 1);
        hazelcastInstance.shutdown();
    }

    @Test
    public void test_jarUploadByClient_withMainClassname() {
        // Reset the singleton because a new HazelcastInstance will be created for the test
        HazelcastBootstrap.resetSupplier();

        Config config = smallInstanceConfig();
        JetConfig jetConfig = config.getJetConfig();
        jetConfig.setResourceUploadEnabled(true);

        HazelcastInstance hazelcastInstance = createHazelcastInstance(config);
        HazelcastInstance client = createHazelcastClient();
        JetService jetService = client.getJet();
        List<String> jobParameters = emptyList();

        SubmitJobParameters submitJobParameters = new SubmitJobParameters();
        submitJobParameters.setJarPath(getJarPath());
        submitJobParameters.setMainClass("org.example.Main");
        submitJobParameters.setJobParameters(jobParameters);

        jetService.submitJobFromJar(submitJobParameters);

        assertEqualsEventually(() -> jetService.getJobs().size(), 1);
        hazelcastInstance.shutdown();
    }

    @Test
    public void test_jarUploadByClient_withWrongMainClassname() {

        // Reset the singleton because a new HazelcastInstance will be created for the test
        HazelcastBootstrap.resetSupplier();

        Config config = smallInstanceConfig();
        JetConfig jetConfig = config.getJetConfig();
        jetConfig.setResourceUploadEnabled(true);

        HazelcastInstance hazelcastInstance = createHazelcastInstance(config);
        HazelcastInstance client = createHazelcastClient();
        JetService jetService = client.getJet();
        List<String> jobParameters = emptyList();

        SubmitJobParameters submitJobParameters = new SubmitJobParameters();
        submitJobParameters.setJarPath(getJarPath());
        submitJobParameters.setMainClass("org.example.Main1");
        submitJobParameters.setJobParameters(jobParameters);

        assertThrows(JetException.class, () -> jetService.submitJobFromJar(submitJobParameters));
        hazelcastInstance.shutdown();

    }

    @Test
    public void test_jarUploadByMember_whenResourceUploadIsEnabled() {
        // Reset the singleton because a new HazelcastInstance will be created for the test
        HazelcastBootstrap.resetSupplier();

        Config config = smallInstanceConfig();
        JetConfig jetConfig = config.getJetConfig();
        jetConfig.setResourceUploadEnabled(true);

        HazelcastInstance hazelcastInstance = createHazelcastInstance(config);
        JetService jetService = hazelcastInstance.getJet();
        List<String> jobParameters = emptyList();

        SubmitJobParameters submitJobParameters = new SubmitJobParameters();
        submitJobParameters.setJarPath(getJarPath());
        submitJobParameters.setJobParameters(jobParameters);

        jetService.submitJobFromJar(submitJobParameters);

        assertEqualsEventually(() -> jetService.getJobs().size(), 1);
        hazelcastInstance.shutdown();
    }

    @Test
    public void test_jarUploadByMember_withMainClassname() {
        // Reset the singleton because a new HazelcastInstance will be created for the test
        HazelcastBootstrap.resetSupplier();

        Config config = smallInstanceConfig();
        JetConfig jetConfig = config.getJetConfig();
        jetConfig.setResourceUploadEnabled(true);

        HazelcastInstance hazelcastInstance = createHazelcastInstance(config);
        JetService jetService = hazelcastInstance.getJet();
        List<String> jobParameters = emptyList();

        SubmitJobParameters submitJobParameters = new SubmitJobParameters();
        submitJobParameters.setJarPath(getJarPath());
        submitJobParameters.setMainClass("org.example.Main");
        submitJobParameters.setJobParameters(jobParameters);

        jetService.submitJobFromJar(submitJobParameters);

        assertEqualsEventually(() -> jetService.getJobs().size(), 1);
        hazelcastInstance.shutdown();
    }

    @Test
    public void test_jarUploadByMember_withWrongMainClassname() {
        // Reset the singleton because a new HazelcastInstance will be created for the test
        HazelcastBootstrap.resetSupplier();

        Config config = smallInstanceConfig();
        JetConfig jetConfig = config.getJetConfig();
        jetConfig.setResourceUploadEnabled(true);

        HazelcastInstance hazelcastInstance = createHazelcastInstance(config);
        JetService jetService = hazelcastInstance.getJet();
        List<String> jobParameters = emptyList();

        SubmitJobParameters submitJobParameters = new SubmitJobParameters();
        submitJobParameters.setJarPath(getJarPath());
        submitJobParameters.setMainClass("org.example.Main1");
        submitJobParameters.setJobParameters(jobParameters);

        assertThrows(ClassNotFoundException.class, () -> jetService.submitJobFromJar(submitJobParameters));

        hazelcastInstance.shutdown();
    }

    @Test
    public void test_jarUpload_WithIncorrectChecksum() throws IOException, NoSuchAlgorithmException {
        // Reset the singleton because a new HazelcastInstance will be created for the test
        HazelcastBootstrap.resetSupplier();

        Config config = smallInstanceConfig();
        JetConfig jetConfig = config.getJetConfig();
        jetConfig.setResourceUploadEnabled(true);

        HazelcastInstance hazelcastInstance = createHazelcastInstance(config);
        HazelcastInstance client = createHazelcastClient();

        // Mock the JetClientInstanceImpl to return an incorrect checksum
        JetClientInstanceImpl jetService = (JetClientInstanceImpl) client.getJet();
        JetClientInstanceImpl spyJetService = Mockito.spy(jetService);

        Path jarPath = getJarPath();
        when(spyJetService.calculateSha256Hex(jarPath)).thenReturn("1");
        List<String> jobParameters = emptyList();

        SubmitJobParameters submitJobParameters = new SubmitJobParameters();
        submitJobParameters.setJarPath(getJarPath());
        submitJobParameters.setJobParameters(jobParameters);

        assertThrows(JetException.class, () -> spyJetService.submitJobFromJar(submitJobParameters));

        assertEqualsEventually(() -> jetService.getJobs().size(), 0);
        hazelcastInstance.shutdown();

    }

    @Test
    public void test_jarUpload_whenResourceUploadIsEnabled_withSmallBuffer() {

        // Reset the singleton because a new HazelcastInstance will be created for the test
        HazelcastBootstrap.resetSupplier();

        Config config = smallInstanceConfig();
        JetConfig jetConfig = config.getJetConfig();
        jetConfig.setResourceUploadEnabled(true);

        HazelcastInstance hazelcastInstance = createHazelcastInstance(config);

        // Change the part buffer size
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(ClientProperty.JOB_UPLOAD_PART_SIZE.getName(), "100");
        HazelcastInstance client = createHazelcastClient(clientConfig);

        JetService jetService = client.getJet();
        List<String> jobParameters = emptyList();

        SubmitJobParameters submitJobParameters = new SubmitJobParameters();
        submitJobParameters.setJarPath(getJarPath());
        submitJobParameters.setJobParameters(jobParameters);

        jetService.submitJobFromJar(submitJobParameters);

        assertEqualsEventually(() -> jetService.getJobs().size(), 1);
        hazelcastInstance.shutdown();
    }

    // This test is slow because it is trying to upload a lot of jobs
    @Category({SlowTest.class})
    @Test
    public void test_stress_jarUpload_whenResourceUploadIsEnabled() {

        // Reset the singleton because a new HazelcastInstance will be created for the test
        HazelcastBootstrap.resetSupplier();

        Config config = smallInstanceConfig();
        JetConfig jetConfig = config.getJetConfig();
        jetConfig.setResourceUploadEnabled(true);

        HazelcastInstance hazelcastInstance = createHazelcastInstance(config);
        ExecutorService executorService = Executors.newFixedThreadPool(10);

        int jobLimit = 50;
        for (int index = 0; index < jobLimit; index++) {
            executorService.submit(() -> {
                HazelcastInstance client = createHazelcastClient();
                JetService jetService = client.getJet();
                List<String> jobParameters = emptyList();

                SubmitJobParameters submitJobParameters = new SubmitJobParameters();
                submitJobParameters.setJarPath(getJarPath());
                submitJobParameters.setJobParameters(jobParameters);

                jetService.submitJobFromJar(submitJobParameters);
                client.shutdown();
            });
        }

        HazelcastInstance client = createHazelcastClient();
        JetService jetService = client.getJet();
        assertEqualsEventually(() -> jetService.getJobs().size(), jobLimit);
        hazelcastInstance.shutdown();
    }

    @Test
    public void test_multipleJarUploads_whenResourceUploadIsEnabled() {

        // Reset the singleton because a new HazelcastInstance will be created for the test
        HazelcastBootstrap.resetSupplier();

        Config config = smallInstanceConfig();
        JetConfig jetConfig = config.getJetConfig();
        jetConfig.setResourceUploadEnabled(true);

        HazelcastInstance hazelcastInstance = createHazelcastInstance(config);
        HazelcastInstance client = createHazelcastClient();
        JetService jetService = client.getJet();
        List<String> jobParameters = emptyList();

        String job1 = "job1";
        SubmitJobParameters submitJobParameters1 = new SubmitJobParameters();
        submitJobParameters1.setJarPath(getJarPath());
        submitJobParameters1.setJobName(job1);
        submitJobParameters1.setJobParameters(jobParameters);

        jetService.submitJobFromJar(submitJobParameters1);

        String job2 = "job2";
        SubmitJobParameters submitJobParameters2 = new SubmitJobParameters();
        submitJobParameters2.setJarPath(getJarPath());
        submitJobParameters2.setJobName(job2);
        submitJobParameters2.setJobParameters(jobParameters);
        jetService.submitJobFromJar(submitJobParameters2);

        assertTrueEventually(() -> {
            List<Job> jobs = jetService.getJobs();
            assertEquals(2, jobs.size());
            assertTrue(containsName(jobs, job1));
            assertTrue(containsName(jobs, job2));
        });

        // Let the jobs run
        sleepAtLeastSeconds(5);

        hazelcastInstance.shutdown();
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
