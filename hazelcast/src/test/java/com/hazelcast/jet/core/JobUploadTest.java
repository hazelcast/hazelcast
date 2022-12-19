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

package com.hazelcast.jet.core;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastBootstrap;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.impl.JetClientInstanceImpl;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.util.Collections.emptyList;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category({QuickTest.class})
public class JobUploadTest extends JetTestSupport {

    @Test
    public void test_client_jarUpload_whenResourceUploadIsNotEnabled() {
        createHazelcastInstance();
        HazelcastInstance client = createHazelcastClient();
        JetService jetService = client.getJet();
        List<String> jobParameters = emptyList();
        assertThrows(JetException.class, () ->
                jetService.uploadJob(getJarPath(),
                        null,
                        null,
                        null,
                        jobParameters)
        );
    }

    @Test
    public void test_member_jarUpload_whenResourceUploadIsNotEnabled() {
        HazelcastInstance hazelcastInstance = createHazelcastInstance();
        JetService jetService = hazelcastInstance.getJet();
        List<String> jobParameters = emptyList();
        assertThrows(JetException.class, () ->
                jetService.uploadJob(getJarPath(),
                        null,
                        null,
                        null,
                        jobParameters)
        );
    }

    @Test
    public void test_jarUpload_whenResourceUploadIsEnabled() {
        // Reset the singleton for the test
        HazelcastBootstrap.resetSupplier();

        Config config = smallInstanceConfig();
        JetConfig jetConfig = config.getJetConfig();
        jetConfig.setResourceUploadEnabled(true);

        HazelcastInstance hazelcastInstance = createHazelcastInstance(config);
        HazelcastInstance client = createHazelcastClient();
        JetService jetService = client.getJet();
        List<String> jobParameters = emptyList();

        jetService.uploadJob(getJarPath(),
                null,
                null,
                null,
                jobParameters);

        assertEqualsEventually(() -> jetService.getJobs().size(), 1);
        hazelcastInstance.shutdown();
    }

    @Test
    public void test_jarUpload_whenResourceUploadIsEnabled_withSmallBuffer() {

        // Change the part buffer size
        System.setProperty(JetClientInstanceImpl.PART_SIZE, "100");

        // Reset the singleton for the test
        HazelcastBootstrap.resetSupplier();

        Config config = smallInstanceConfig();
        JetConfig jetConfig = config.getJetConfig();
        jetConfig.setResourceUploadEnabled(true);

        HazelcastInstance hazelcastInstance = createHazelcastInstance(config);
        HazelcastInstance client = createHazelcastClient();
        JetService jetService = client.getJet();
        List<String> jobParameters = emptyList();

        jetService.uploadJob(getJarPath(),
                null,
                null,
                null,
                jobParameters);

        assertEqualsEventually(() -> jetService.getJobs().size(), 1);
        hazelcastInstance.shutdown();
    }

    @Test
    public void test_stress_jarUpload_whenResourceUploadIsEnabled() throws InterruptedException {

        // Reset the singleton for the test
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

                jetService.uploadJob(getJarPath(),
                        null,
                        null,
                        null,
                        jobParameters);
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

        // Reset the singleton for the test
        HazelcastBootstrap.resetSupplier();

        Config config = smallInstanceConfig();
        JetConfig jetConfig = config.getJetConfig();
        jetConfig.setResourceUploadEnabled(true);

        HazelcastInstance hazelcastInstance = createHazelcastInstance(config);
        HazelcastInstance client = createHazelcastClient();
        JetService jetService = client.getJet();
        List<String> jobParameters = emptyList();

        String job1 = "job1";
        jetService.uploadJob(getJarPath(),
                null,
                job1,
                null,
                jobParameters);

        String job2 = "job2";
        jetService.uploadJob(getJarPath(),
                null,
                job2,
                null,
                jobParameters);

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

    private Path getJarPath() {
        ClassLoader classLoader = getClass().getClassLoader();
        URL resource = classLoader.getResource("simplejob.jar");
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
