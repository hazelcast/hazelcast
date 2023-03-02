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

package com.hazelcast.jet.core.submitjob.clientside.execute;

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
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.impl.JetClientInstanceImpl;
import com.hazelcast.jet.impl.submitjob.clientside.execute.JobExecuteCall;
import com.hazelcast.jet.test.SerialTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.UUID;

import static com.hazelcast.jet.core.submitjob.clientside.upload.JobUploadClientFailureTest.containsName;
import static com.hazelcast.jet.core.submitjob.clientside.upload.JobUploadClientFailureTest.getJarPath;
import static com.hazelcast.jet.core.submitjob.clientside.upload.JobUploadClientFailureTest.getNoManifestJarPath;
import static junit.framework.TestCase.assertEquals;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doAnswer;

@Category({SerialTest.class})
public class JobExecuteClientFailureTest extends JetTestSupport {

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

        SubmitJobParameters submitJobParameters = SubmitJobParameters.forDirectJobExecution();

        assertThatThrownBy(() -> jetService.submitJobFromJar(submitJobParameters))
                .isInstanceOf(JetException.class)
                .hasMessageContaining("jarPath can not be null");
    }

    @Test
    public void testNullJobParameters() {
        createHazelcastInstance();
        HazelcastInstance client = createHazelcastClient();
        JetService jetService = client.getJet();

        SubmitJobParameters submitJobParameters = SubmitJobParameters.forDirectJobExecution()
                .setJarPath(getJarPath())
                .setJobParameters(null);

        assertThatThrownBy(() -> jetService.submitJobFromJar(submitJobParameters))
                .isInstanceOf(JetException.class)
                .hasMessageContaining("jobParameters can not be null");
    }

    @Test
    public void testNullMainClass() {
        HazelcastInstance client = createCluster();
        JetService jetService = client.getJet();

        SubmitJobParameters submitJobParameters = SubmitJobParameters.forDirectJobExecution()
                .setJarPath(getNoManifestJarPath());

        assertThatThrownBy(() -> jetService.submitJobFromJar(submitJobParameters))
                .isInstanceOf(JetException.class)
                .hasStackTraceContaining("No Main-Class found in the manifest");
    }

    @Test
    public void test_jarExecute_whenResourceUploadIsNotEnabled() {
        createHazelcastInstance();
        HazelcastInstance client = createHazelcastClient();
        JetService jetService = client.getJet();

        SubmitJobParameters submitJobParameters =SubmitJobParameters.forDirectJobExecution()
                .setJarPath(getJarPath());

        assertThrows(JetException.class, () ->
                jetService.submitJobFromJar(submitJobParameters)
        );

        assertEqualsEventually(() -> jetService.getJobs().size(), 0);
    }

    @Test
    public void test_jarExecute_withWrongMainClassname() {
        HazelcastInstance client = createCluster();
        JetService jetService = client.getJet();

        SubmitJobParameters submitJobParameters = SubmitJobParameters.forDirectJobExecution()
                .setJarPath(getJarPath())
                .setMainClass("org.example.Main1");

        assertThrows(JetException.class, () -> jetService.submitJobFromJar(submitJobParameters));
    }


    @Test
    public void test_jar_isNotDeleted() {
        HazelcastInstance client = createCluster();
        JetService jetService = client.getJet();

        SubmitJobParameters submitJobParameters = SubmitJobParameters.forDirectJobExecution()
                .setJarPath(getJarPath())
                .setMainClass("org.example.Main1");

        assertThrows(JetException.class, () -> jetService.submitJobFromJar(submitJobParameters));

        boolean fileExists = Files.exists(getJarPath());
        assertThat(fileExists)
                .isTrue();

    }


    @Test
    public void test_jobAlreadyExists() {
        createCluster();

        HazelcastInstance client = createHazelcastClient();
        JetService jetService = client.getJet();

        String job1 = "job1";
        SubmitJobParameters submitJobParameters1 = SubmitJobParameters.forDirectJobExecution()
                .setJarPath(getJarPath())
                .setJobName(job1);

        jetService.submitJobFromJar(submitJobParameters1);


        String job2 = "job1";
        SubmitJobParameters submitJobParameters2 = SubmitJobParameters.forDirectJobExecution()
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
    public void test_jarExecute_whenMemberShutsDown() throws IOException, NoSuchAlgorithmException {
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
            JobExecuteCall jobExecuteCall = (JobExecuteCall) invocation.callRealMethod();
            UUID memberUuid = jobExecuteCall.getMemberUuid();
            // Shutdown the target member
            for (HazelcastInstance hazelcastInstance : cluster) {
                if (hazelcastInstance.getCluster().getLocalMember().getUuid().equals(memberUuid)) {
                    hazelcastInstance.shutdown();
                    break;
                }
            }
            assertClusterSizeEventually(1, client);
            return jobExecuteCall;
        }).when(spyJetService).initializeJobExecuteCall(jarPath);

        SubmitJobParameters submitJobParameters = SubmitJobParameters.forDirectJobExecution()
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
}
