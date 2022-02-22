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

package com.hazelcast.client;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class HazelcastClientTest extends HazelcastTestSupport {

    private static final String CLIENT_CONFIG_PROP_NAME = "hazelcast.client.config";

    @Before
    public void setUp() {
        cleanup();
        Hazelcast.newHazelcastInstance(smallInstanceConfig());
    }

    @After
    public void tearDown() {
        cleanup();
    }

    private void cleanup() {
        HazelcastClient.shutdownAll();
        assertEquals(0, HazelcastClient.getAllHazelcastClients().size());
        Hazelcast.shutdownAll();
        assertEquals(0, Hazelcast.getAllHazelcastInstances().size());
    }

    // null instance name
    @Test(expected = IllegalArgumentException.class)
    public void testGetOrCreateHazelcastClient_withNullConfig() {
        HazelcastClient.getOrCreateHazelcastClient(null);
    }

    @Test
    public void testGetOrCreateHazelcastClient_returnsSame_withNoConfig() {
        String hzConfigProperty = System.getProperty(CLIENT_CONFIG_PROP_NAME);
        try {
            System.setProperty(CLIENT_CONFIG_PROP_NAME, "classpath:hazelcast-client-test.xml");
            HazelcastInstance client1 = HazelcastClient.getOrCreateHazelcastClient();
            HazelcastInstance client2 = HazelcastClient.getOrCreateHazelcastClient();
            assertEquals("Calling two times getOrCreateHazelcastClient should return same client", client1, client2);
        } finally {
            if (hzConfigProperty == null) {
                System.clearProperty(CLIENT_CONFIG_PROP_NAME);
            } else {
                System.setProperty(CLIENT_CONFIG_PROP_NAME, hzConfigProperty);
            }
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetOrCreateHazelcastClient_withNullName() {
        ClientConfig config = new ClientConfig();
        HazelcastClient.getOrCreateHazelcastClient(config);
    }

    @Test
    public void testGetOrCreateHazelcastClient_whenClientDoesNotExist() {
        String instanceName = randomString();
        ClientConfig config = new ClientConfig();
        config.setInstanceName(instanceName);

        HazelcastInstance client = HazelcastClient.getOrCreateHazelcastClient(config);
        assertEquals(client, HazelcastClient.getHazelcastClientByName(instanceName));
    }

    @Test
    public void testGetOrCreateHazelcastClient_whenClientExists() {
        String instanceName = randomString();
        ClientConfig config = new ClientConfig();
        config.setInstanceName(instanceName);

        HazelcastInstance client = HazelcastClient.newHazelcastClient(config);
        assertEquals(client, HazelcastClient.getOrCreateHazelcastClient(config));
    }

    @Test
    public void testGetOrCreateHazelcastClientConcurrently() throws ExecutionException, InterruptedException {
        String instanceName = randomString();
        ClientConfig config = new ClientConfig();
        config.setInstanceName(instanceName);

        int clientCount = 10;
        List<HazelcastInstance> clients = Collections.synchronizedList(new ArrayList<>(clientCount));
        List<Future> futures = new ArrayList<>(clientCount);
        for (int i = 0; i < clientCount; i++) {
            futures.add(spawn(() -> {
                clients.add(HazelcastClient.getOrCreateHazelcastClient(config));
            }));
        }
        for (int i = 0; i < clientCount; i++) {
            futures.get(i).get();
        }
        assertEquals(clientCount, clients.size());
        for (int i = 1; i < clientCount; i++) {
            assertEquals(clients.get(0), clients.get(i));
        }
    }

    @Test
    public void testGetHazelcastClientByName() {
        String instanceName = randomString();
        ClientConfig config = new ClientConfig();
        config.setInstanceName(instanceName);

        HazelcastInstance client1 = HazelcastClient.newHazelcastClient(config);
        assertEquals(client1, HazelcastClient.getHazelcastClientByName(instanceName));
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testNewHazelcastClient_withSameConfig() {
        String instanceName = randomString();
        ClientConfig config = new ClientConfig();
        config.setInstanceName(instanceName);

        HazelcastClient.newHazelcastClient(config);
        HazelcastClient.newHazelcastClient(config);
    }

    @Test
    public void testGetAllHazelcastClients() {
        HazelcastInstance client1 = HazelcastClient.newHazelcastClient(new ClientConfig()
                .setInstanceName(randomString()));
        HazelcastInstance client2 = HazelcastClient.getOrCreateHazelcastClient(new ClientConfig()
                .setInstanceName(randomString()));

        Collection<HazelcastInstance> clients = HazelcastClient.getAllHazelcastClients();
        assertEquals(2, clients.size());
        assertContains(clients, client1);
        assertContains(clients, client2);
    }

    @Test
    public void testShutdownAll() {
        HazelcastClient.newHazelcastClient(new ClientConfig()
                .setInstanceName(randomString()));
        HazelcastClient.getOrCreateHazelcastClient(new ClientConfig()
                .setInstanceName(randomString()));

        HazelcastClient.shutdownAll();
        assertEquals(0, HazelcastClient.getAllHazelcastClients().size());
    }

    @Test
    public void testShutdown_withInstance() {
        HazelcastInstance client = HazelcastClient.newHazelcastClient(new ClientConfig()
                .setInstanceName(randomString()));

        HazelcastClient.shutdown(client);

        assertEquals(0, HazelcastClient.getAllHazelcastClients().size());
        assertFalse(client.getLifecycleService().isRunning());
    }

    public void testShutdown_withName() {
        ClientConfig config = new ClientConfig();
        String instanceName = randomString();
        config.setInstanceName(instanceName);

        HazelcastInstance client = HazelcastClient.newHazelcastClient(config);
        HazelcastClient.shutdown(instanceName);

        assertEquals(0, HazelcastClient.getAllHazelcastClients().size());
        assertFalse(client.getLifecycleService().isRunning());
    }
}
