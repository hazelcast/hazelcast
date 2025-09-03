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
import com.hazelcast.client.properties.ClientProperty;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.instance.impl.HazelcastBootstrap;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.JobAlreadyExistsException;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.impl.JetClientInstanceImpl;
import com.hazelcast.jet.impl.SubmitJobParameters;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

import static com.hazelcast.jet.impl.submitjob.clientside.upload.JobUploadClientFailureTest.containsName;
import static com.hazelcast.jet.impl.submitjob.clientside.upload.JobUploadClientFailureTest.getJarPath;
import static com.hazelcast.jet.impl.submitjob.clientside.upload.JobUploadClientFailureTest.getNoManifestJarPath;
import static org.junit.Assert.assertEquals;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doAnswer;

// Tests to execute existing jar on the member
@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class JobExecuteClientFailureTest extends JetTestSupport {

    @After
    public void resetSingleton() {
        // Reset the singleton after the test
        HazelcastBootstrap.resetRemembered();
    }

    @Test
    public void testNullJarPath() {
        createCluster();
        JetClientInstanceImpl jetService = getClientJetService();

        SubmitJobParameters submitJobParameters = SubmitJobParameters.withJarOnMember();

        assertThatThrownBy(() -> jetService.submitJobFromJar(submitJobParameters))
                .isInstanceOf(JetException.class)
                .hasMessageContaining("jarPath can not be null");
    }

    @Test
    public void testNullJobParameters() {
        createCluster();
        JetClientInstanceImpl jetService = getClientJetService();

        SubmitJobParameters submitJobParameters = SubmitJobParameters.withJarOnMember()
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

        SubmitJobParameters submitJobParameters = SubmitJobParameters.withJarOnMember()
                .setJarPath(getNoManifestJarPath());

        assertThatThrownBy(() -> jetService.submitJobFromJar(submitJobParameters))
                .isInstanceOf(JetException.class)
                .hasStackTraceContaining("No Main-Class found in the manifest");
    }

    @Test
    public void test_jarExecute_whenResourceUploadIsNotEnabled() {
        createHazelcastInstance();
        HazelcastInstance client = createHazelcastClient();
        JetClientInstanceImpl jetService = (JetClientInstanceImpl) client.getJet();

        SubmitJobParameters submitJobParameters = SubmitJobParameters.withJarOnMember()
                .setJarPath(getJarPath());

        assertThrows(JetException.class, () ->
                jetService.submitJobFromJar(submitJobParameters)
        );

        assertEqualsEventually(() -> jetService.getJobs().size(), 0);
    }

    @Test
    public void test_jarExecute_withWrongMainClassname() {
        createCluster();
        JetClientInstanceImpl jetService = getClientJetService();

        SubmitJobParameters submitJobParameters = SubmitJobParameters.withJarOnMember()
                .setJarPath(getJarPath())
                .setMainClass("org.example.Main1");

        assertThrows(JetException.class, () -> jetService.submitJobFromJar(submitJobParameters));
    }


    @Test
    public void test_jar_isNotDeleted() {
        createCluster();
        JetClientInstanceImpl jetService = getClientJetService();

        SubmitJobParameters submitJobParameters = SubmitJobParameters.withJarOnMember()
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
        JetClientInstanceImpl jetService = getClientJetService();

        String job1 = "job1";
        SubmitJobParameters submitJobParameters1 = SubmitJobParameters.withJarOnMember()
                .setJarPath(getJarPath())
                .setJobName(job1);

        jetService.submitJobFromJar(submitJobParameters1);


        String job2 = "job1";
        SubmitJobParameters submitJobParameters2 = SubmitJobParameters.withJarOnMember()
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
    public void test_jarExecute_whenMemberShutsDown() {
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

        SubmitJobParameters submitJobParameters = SubmitJobParameters.withJarOnMember()
                .setJarPath(getJarPath());

        // Should throw OperationTimeoutException because target instance was shut down
        assertThrows(OperationTimeoutException.class, () -> spyJetService.submitJobFromJar(submitJobParameters));
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
}
