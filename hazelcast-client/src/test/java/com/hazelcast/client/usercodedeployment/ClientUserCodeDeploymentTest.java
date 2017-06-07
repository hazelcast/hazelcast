/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.UserCodeDeploymentConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.FilteringClassLoader;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;
import usercodedeployment.EntryProcessorWithAnonymousAndInner;
import usercodedeployment.IncrementingEntryProcessor;

import java.util.Collection;
import java.util.LinkedList;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class})
public class ClientUserCodeDeploymentTest extends HazelcastTestSupport {

    private TestHazelcastFactory factory = new TestHazelcastFactory();

    @After
    public void tearDown() throws Exception {
        factory.terminateAll();
    }

    @Parameterized.Parameter(0)
    public UserCodeDeploymentConfig.ClassCacheMode classCacheMode;
    @Parameterized.Parameter(1)
    public UserCodeDeploymentConfig.ProviderMode providerMode;


    @Parameters(name = "ClassCacheMode:{0}, ProviderMode:{1}")
    public static Collection<Object[]> parameters() {
        LinkedList<Object[]> parameters = new LinkedList<Object[]>();
                parameters.add(new Object[]{UserCodeDeploymentConfig.ClassCacheMode.OFF, UserCodeDeploymentConfig.ProviderMode.OFF});
        return parameters;
    }

    private Config createNodeConfig() {
        Config config = new Config();
        FilteringClassLoader filteringCL = new FilteringClassLoader(singletonList("usercodedeployment"), null);
        config.setClassLoader(filteringCL);
        config.getUserCodeDeploymentConfig()
                .setEnabled(true)
                .setClassCacheMode(classCacheMode)
                .setProviderMode(providerMode);
        return config;
    }

    private ClientConfig createClientConfig() {
        ClientConfig config = new ClientConfig();
        ClientUserCodeDeploymentConfig clientUserCodeDeploymentConfig = new ClientUserCodeDeploymentConfig();
        clientUserCodeDeploymentConfig.addClass("usercodedeployment.IncrementingEntryProcessor");
        config.setUserCodeDeploymentConfig(clientUserCodeDeploymentConfig.setEnabled(true));
        return config;
    }

    @Test
    public void testSingleMember() {
        ClientConfig config = createClientConfig();
        Config clientConfig = createNodeConfig();

        factory.newHazelcastInstance(clientConfig);
        HazelcastInstance client = factory.newHazelcastClient(config);

        assertCodeDeploymentWorking(client);
    }

    @Test
    public void testWithMultipleMembers() {
        ClientConfig config = createClientConfig();
        Config clientConfig = createNodeConfig();

        factory.newHazelcastInstance(clientConfig);
        HazelcastInstance client = factory.newHazelcastClient(config);
        factory.newHazelcastInstance(clientConfig);

        assertCodeDeploymentWorking(client);
    }

    @Test
    public void testWithMultipleNodes_clientReconnectsToNewNode() {
        ClientConfig config = createClientConfig();
        Config clientConfig = createNodeConfig();

        HazelcastInstance firstInstance = factory.newHazelcastInstance(clientConfig);
        HazelcastInstance client = factory.newHazelcastClient(config);
        factory.newHazelcastInstance(clientConfig);
        firstInstance.getLifecycleService().shutdown();

        assertCodeDeploymentWorking(client);
    }

    private void assertCodeDeploymentWorking(HazelcastInstance client) {
        IncrementingEntryProcessor incrementingEntryProcessor = new IncrementingEntryProcessor();
        int keyCount = 100;
        IMap<Integer, Integer> map = client.getMap(randomName());

        for (int i = 0; i < keyCount; i++) {
            map.put(i, 0);
        }
        map.executeOnEntries(incrementingEntryProcessor);

        for (int i = 0; i < keyCount; i++) {
            assertEquals(1, (int) map.get(i));
        }
    }

    @Test
    public void testWithInnerAndAnonymousClass() {
        Config config = createNodeConfig();

        factory.newHazelcastInstance(config);
        ClientConfig clientConfig = new ClientConfig();
        ClientUserCodeDeploymentConfig clientUserCodeDeploymentConfig = new ClientUserCodeDeploymentConfig();
        clientUserCodeDeploymentConfig.addJar("EntryProcessorWithAnonymousAndInner.jar").setEnabled(true);
        clientConfig.setUserCodeDeploymentConfig(clientUserCodeDeploymentConfig);

        HazelcastInstance client = factory.newHazelcastClient(clientConfig);


        EntryProcessorWithAnonymousAndInner incrementingEntryProcessor = new EntryProcessorWithAnonymousAndInner();
        int keyCount = 100;
        IMap<Integer, Integer> map = client.getMap(randomName());

        for (int i = 0; i < keyCount; i++) {
            map.put(i, 0);
        }
        map.executeOnEntries(incrementingEntryProcessor);

        for (int i = 0; i < keyCount; i++) {
            assertEquals(1, (int) map.get(i));
        }
    }
}