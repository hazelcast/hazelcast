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

package com.hazelcast.client.usercodedeployment;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientUserCodeDeploymentConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.internal.util.FilteringClassLoader;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import usercodedeployment.IncrementingEntryProcessor;

import java.io.FileNotFoundException;

import static java.util.Collections.singletonList;
import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientUserCodeDeploymentExceptionTest extends HazelcastTestSupport {

    private TestHazelcastFactory factory = new TestHazelcastFactory();

    @After
    public void tearDown() throws Exception {
        factory.terminateAll();
    }

    @Test
    public void testUserCodeDeploymentIsDisabledByDefaultOnClient() {
        // this test also validate the EP is filtered locally and has to be loaded from the other member
        ClientConfig clientConfig = new ClientConfig();
        Config config = createNodeConfig();
        config.getUserCodeDeploymentConfig().setEnabled(true);

        IncrementingEntryProcessor incrementingEntryProcessor = new IncrementingEntryProcessor();
        factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient(clientConfig);

        IMap<Integer, Integer> map = client.getMap(randomName());
        try {
            map.executeOnEntries(incrementingEntryProcessor);
            fail();
        } catch (HazelcastSerializationException e) {
            assertEquals(ClassNotFoundException.class, e.getCause().getClass());
        }
    }

    private Config createNodeConfig() {
        Config i2Config = new Config();
        FilteringClassLoader filteringCL = new FilteringClassLoader(singletonList("usercodedeployment"), null);
        i2Config.setClassLoader(filteringCL);
        return i2Config;
    }


    private ClientConfig createClientConfig() {
        ClientConfig config = new ClientConfig();
        ClientUserCodeDeploymentConfig clientUserCodeDeploymentConfig = new ClientUserCodeDeploymentConfig();
        clientUserCodeDeploymentConfig.addClass("usercodedeployment.IncrementingEntryProcessor");
        config.setUserCodeDeploymentConfig(clientUserCodeDeploymentConfig.setEnabled(true));
        return config;
    }

    @Test(expected = IllegalStateException.class)
    public void testUserCodeDeployment_serverIsNotEnabled() {
        ClientConfig clientConfig = createClientConfig();
        clientConfig.getUserCodeDeploymentConfig().setEnabled(true);
        Config config = createNodeConfig();

        factory.newHazelcastInstance(config);
        factory.newHazelcastClient(clientConfig);
    }

    /**
     * The two JARs {@code IncrementingEntryProcessor.jar} and {@code IncrementingEntryProcessorConflicting.jar}
     * contain the same class {@link IncrementingEntryProcessor} with different implementations
     */
    @Test(expected = IllegalStateException.class)
    public void testClientsWithConflictingClassRepresentations() {
        Config config = createNodeConfig();
        config.getUserCodeDeploymentConfig().setEnabled(true);
        factory.newHazelcastInstance(config);

        ClientUserCodeDeploymentConfig clientUserCodeDeploymentConfig1 = new ClientUserCodeDeploymentConfig()
                .addJar("IncrementingEntryProcessor.jar").setEnabled(true);
        ClientConfig clientConfig1 = new ClientConfig()
                .setUserCodeDeploymentConfig(clientUserCodeDeploymentConfig1);
        factory.newHazelcastClient(clientConfig1);

        ClientUserCodeDeploymentConfig clientUserCodeDeploymentConfig2 = new ClientUserCodeDeploymentConfig()
                .addJar("IncrementingEntryProcessorConflicting.jar").setEnabled(true);
        ClientConfig clientConfig2 = new ClientConfig()
                .setUserCodeDeploymentConfig(clientUserCodeDeploymentConfig2);
        factory.newHazelcastClient(clientConfig2);
    }

    @Test(expected = ClassNotFoundException.class)
    public void testClientsWith_wrongClassName() throws Throwable {
        Config config = createNodeConfig();
        config.getUserCodeDeploymentConfig().setEnabled(true);

        factory.newHazelcastInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        ClientUserCodeDeploymentConfig clientUserCodeDeploymentConfig = new ClientUserCodeDeploymentConfig();
        clientUserCodeDeploymentConfig.addClass("NonExisting.class").setEnabled(true);
        clientConfig.setUserCodeDeploymentConfig(clientUserCodeDeploymentConfig);
        try {
            factory.newHazelcastClient(clientConfig);
        } catch (HazelcastException e) {
            throw e.getCause();
        }
    }

    @Test(expected = FileNotFoundException.class)
    public void testClientsWith_wrongJarPath() throws Throwable {
        Config config = createNodeConfig();
        config.getUserCodeDeploymentConfig().setEnabled(true);

        factory.newHazelcastInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        ClientUserCodeDeploymentConfig clientUserCodeDeploymentConfig = new ClientUserCodeDeploymentConfig();
        clientUserCodeDeploymentConfig.addJar("NonExisting.jar").setEnabled(true);
        clientConfig.setUserCodeDeploymentConfig(clientUserCodeDeploymentConfig);
        try {
            factory.newHazelcastClient(clientConfig);
        } catch (HazelcastException e) {
            throw e.getCause();
        }
    }
}
