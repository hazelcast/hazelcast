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

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastBootstrap;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.SubmitJobParameters;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.test.SerialTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.nio.file.Path;

import static com.hazelcast.jet.core.JobUploadClientFailureTest.copyJar;
import static com.hazelcast.jet.core.JobUploadClientSuccessTest.assertJobIsRunning;


@Category({SerialTest.class})
public class JobUploadMemberSuccessTest extends JetTestSupport {

    @After
    public void resetSingleton() {
        // Reset the singleton after the test
        HazelcastBootstrap.resetSupplier();
    }

    @Test
    public void test_jarUpload_whenResourceUploadIsEnabled() throws IOException {

        // Copy the jar because it will be deleted
        String memberSimpleJob = "member2.jar";
        Path newPath = copyJar(memberSimpleJob);

        Config config = smallInstanceConfig();
        JetConfig jetConfig = config.getJetConfig();
        jetConfig.setResourceUploadEnabled(true);

        HazelcastInstance hazelcastInstance = createHazelcastInstance(config);
        JetService jetService = hazelcastInstance.getJet();

        SubmitJobParameters submitJobParameters = new SubmitJobParameters()
                .setJarPath(newPath);

        jetService.submitJobFromJar(submitJobParameters);

        assertJobIsRunning(jetService);
    }

    @Test
    public void test_jarUpload_withMainClassname() throws IOException {

        // Copy the jar because it will be deleted
        String memberSimpleJob = "member3.jar";
        Path newPath = copyJar(memberSimpleJob);

        Config config = smallInstanceConfig();
        JetConfig jetConfig = config.getJetConfig();
        jetConfig.setResourceUploadEnabled(true);

        HazelcastInstance hazelcastInstance = createHazelcastInstance(config);
        JetService jetService = hazelcastInstance.getJet();

        SubmitJobParameters submitJobParameters = new SubmitJobParameters()
                .setJarPath(newPath)
                .setMainClass("org.example.Main");

        jetService.submitJobFromJar(submitJobParameters);

        assertJobIsRunning(jetService);
    }
}
