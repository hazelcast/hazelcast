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

package com.hazelcast.client.config;

import com.hazelcast.client.LoadBalancer;
import com.hazelcast.client.test.Employee;
import com.hazelcast.client.test.PortableFactory;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.client.util.RandomLB;
import com.hazelcast.config.Config;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientConfigTest {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void testCopyConstructor_withFullyConfiguredClientConfig() throws IOException {
        URL schemaResource = ClientConfigTest.class.getClassLoader().getResource("hazelcast-client-full.xml");
        ClientConfig expected = new XmlClientConfigBuilder(schemaResource).build();
        ClientConfig actual = new ClientConfig(expected);
        assertEquals(expected, actual);
    }

    @Test
    public void testCopyConstructor_withDefaultClientConfig() {
        ClientConfig expected = new ClientConfig();
        ClientConfig actual = new ClientConfig(expected);
        assertEquals(expected, actual);
    }

    @Test
    public void testAccessDefaultCluster() {
        hazelcastFactory.newHazelcastInstance();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient();

        assertNotNull(client.getConfig());
    }

    @Test
    public void testAccessClusterNameOverClientInstance() {
        Config config = new Config();
        String clusterName = "aClusterName";
        config.setClusterName(clusterName);
        hazelcastFactory.newHazelcastInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName(clusterName);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        assertEquals(clusterName, client.getConfig().getClusterName());
    }

    @Test
    public void testAccessSerializationConfigOverClientInstance() {
        hazelcastFactory.newHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getSerializationConfig().addPortableFactory(PortableFactory.FACTORY_ID, new PortableFactory());
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        SerializationConfig serializationConfig = client.getConfig().getSerializationConfig();
        Map<Integer, com.hazelcast.nio.serialization.PortableFactory> factories = serializationConfig.getPortableFactories();
        assertEquals(1, factories.size());
        assertEquals(factories.get(PortableFactory.FACTORY_ID).create(Employee.CLASS_ID).getClassId(), Employee.CLASS_ID);
    }

    @Test
    public void testUserContext_passContext() {
        hazelcastFactory.newHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();
        ConcurrentMap<String, Object> context = new ConcurrentHashMap<String, Object>();
        context.put("key1", "value1");
        Object value2 = new Object();
        context.put("key2", value2);

        // check set setter returns ClientConfig instance
        clientConfig = clientConfig.setUserContext(context);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        ConcurrentMap<String, Object> returnedContext = client.getUserContext();
        assertEquals(context, returnedContext);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUserContext_throwExceptionWhenContextNull() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setUserContext(null);
    }

    @Test
    public void testReliableTopic() {
        ClientConfig clientConfig = new ClientConfig();
        ClientReliableTopicConfig defaultReliableTopicConfig = new ClientReliableTopicConfig("default");
        defaultReliableTopicConfig.setReadBatchSize(100);
        clientConfig.addReliableTopicConfig(defaultReliableTopicConfig);
        ClientReliableTopicConfig newConfig = clientConfig.getReliableTopicConfig("newConfig");

        assertEquals(100, newConfig.getReadBatchSize());
    }

    @Test
    public void testSettingLoaderBalancerShouldClearLoadBalancerClassName() {
        LoadBalancer loadBalancer = new RandomLB();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setLoadBalancerClassName("com.hazelcast.client.test.CustomLoadBalancer");
        clientConfig.setLoadBalancer(loadBalancer);

        assertNull(clientConfig.getLoadBalancerClassName());
        assertSame(loadBalancer, clientConfig.getLoadBalancer());
    }

    @Test
    public void testSettingLoadBalancerClassNameShouldClearLoadBalancer() {
        LoadBalancer loadBalancer = new RandomLB();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setLoadBalancer(loadBalancer);
        clientConfig.setLoadBalancerClassName("com.hazelcast.client.test.CustomLoadBalancer");

        assertEquals("com.hazelcast.client.test.CustomLoadBalancer", clientConfig.getLoadBalancerClassName());
        assertNull(clientConfig.getLoadBalancer());
    }
}
