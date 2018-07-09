/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.test.Employee;
import com.hazelcast.client.test.PortableFactory;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientConfigTest {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void testAccessGroupNameOverClientInstance() {
        Config config = new Config();
        String groupName = "aGroupName";
        config.getGroupConfig().setName(groupName);
        hazelcastFactory.newHazelcastInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getGroupConfig().setName(groupName);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        assertEquals(groupName, client.getConfig().getGroupConfig().getName());
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
}
