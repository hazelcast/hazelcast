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

package com.hazelcast.jet.impl.submitjob.commandline.upload;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastBootstrap;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;

import static com.hazelcast.jet.impl.submitjob.clientside.upload.JobUploadClientFailureTest.getParalleJarPath;
import static com.hazelcast.jet.impl.submitjob.clientside.upload.JobUploadClientFailureTest.jarDoesNotExistInTempDirectory;
import static org.junit.Assert.assertFalse;

// Test for HazelcastCommandLine
@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class JobExecuteCommandLineSuccessTest extends JetTestSupport {

    @After
    public void resetSingleton() {
        // Reset the singleton after the test
        HazelcastBootstrap.resetRemembered();
    }

    @Test
    public void test_jarUpload_whenResourceUploadIsEnabled()
            throws IOException, ClassNotFoundException, InvocationTargetException, IllegalAccessException {
        HazelcastInstance member = createCluster();

        HazelcastInstance hazelcastClient = createHazelcastClient();

        // newJob() is called from another thread, but it succeeds
        // because CLI does not use ThreadLocal to store ExecuteJobParameters
        HazelcastBootstrap.executeJarOnCLI(() -> hazelcastClient,
                getParalleJarPath().toString(),
                null,
                null,
                null,
                Collections.emptyList());

        assertFalse(hazelcastClient.getLifecycleService().isRunning());

        JetService jetService = member.getJet();
        assertJobIsRunning(jetService);
    }

    private HazelcastInstance createCluster() {
        Config config = smallInstanceConfig();
        JetConfig jetConfig = config.getJetConfig();
        jetConfig.setResourceUploadEnabled(true);

        return createHazelcastInstance(config);
    }

    public static void assertJobIsRunning(JetService jetService) throws IOException {
        // Assert job size
        assertEqualsEventually(() -> jetService.getJobs().size(), 1);

        // Assert job status
        Job job = jetService.getJobs().get(0);
        assertJobStatusEventually(job, JobStatus.RUNNING);

        // Assert job jar does is deleted
        jarDoesNotExistInTempDirectory();
    }
}
