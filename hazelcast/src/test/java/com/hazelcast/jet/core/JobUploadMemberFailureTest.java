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
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.SubmitJobParameters;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.test.SerialTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.fail;

@Category({SerialTest.class})
public class JobUploadMemberFailureTest extends JetTestSupport {

    @After
    public void resetSingleton() {
        // Reset the singleton after the test
        HazelcastBootstrap.resetSupplier();
    }

    @Test
    public void test_jarUpload_whenResourceUploadIsNotEnabled() {
        // Create with default configuration
        HazelcastInstance hazelcastInstance = createHazelcastInstance();
        JetService jetService = hazelcastInstance.getJet();

        SubmitJobParameters submitJobParameters = new SubmitJobParameters()
                .setJarPath(getJarPath());

        assertThrows(JetException.class, () ->
                jetService.submitJobFromJar(submitJobParameters)
        );

        assertEqualsEventually(() -> jetService.getJobs().size(), 0);
    }

    @Test
    public void test_jarUploadBy_withWrongMainClassname() {
        // Create with special configuration
        Config config = smallInstanceConfig();
        JetConfig jetConfig = config.getJetConfig();
        jetConfig.setResourceUploadEnabled(true);

        HazelcastInstance hazelcastInstance = createHazelcastInstance(config);
        JetService jetService = hazelcastInstance.getJet();

        SubmitJobParameters submitJobParameters = new SubmitJobParameters()
                .setJarPath(getJarPath())
                .setMainClass("org.example.Main1");

        assertThrows(ClassNotFoundException.class, () -> jetService.submitJobFromJar(submitJobParameters));
    }

    private Path getJarPath() {
        return getPath("simplejob-1.0.0.jar");
    }

    private Path getPath(String jarName) {
        ClassLoader classLoader = getClass().getClassLoader();
        URL resource = classLoader.getResource(jarName);
        Path result = null;
        try {
            assert resource != null;
            result = Paths.get(resource.toURI());
        } catch (Exception exception) {
            fail("Unable to get jar path from :" + jarName);
        }
        return result;
    }
}
