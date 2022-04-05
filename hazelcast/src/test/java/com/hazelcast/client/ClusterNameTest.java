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
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.test.HazelcastTestSupport.assertOpenEventually;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClusterNameTest {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void cleanup() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void test_clusterName_overClientInstance() {
        hazelcastFactory.newHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();
        String name = "aClient";
        clientConfig.setInstanceName(name);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        assertEquals(name, client.getName());
    }

    @Test
    public void test_clusterName_overGetConnectedClients() {
        HazelcastInstance instance = hazelcastFactory.newHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();
        String name = "aClient";
        clientConfig.setInstanceName(name);
        hazelcastFactory.newHazelcastClient(clientConfig);

        Collection<Client> connectedClients = instance.getClientService().getConnectedClients();
        Client client = connectedClients.iterator().next();
        assertEquals(name, client.getName());
    }

    @Test
    public void test_clusterName_overClientConnectedEvent() {
        HazelcastInstance instance = hazelcastFactory.newHazelcastInstance();
        final CountDownLatch clientConnected = new CountDownLatch(1);

        final AtomicReference<String> clusterName = new AtomicReference<String>();
        instance.getClientService().addClientListener(new ClientListener() {
            @Override
            public void clientConnected(Client client) {
                clusterName.set(client.getName());
                clientConnected.countDown();
            }

            @Override
            public void clientDisconnected(Client client) {

            }
        });

        ClientConfig clientConfig = new ClientConfig();
        String name = "aClient";
        clientConfig.setInstanceName(name);
        hazelcastFactory.newHazelcastClient(clientConfig);

        assertOpenEventually(clientConnected);
        assertEquals(name, clusterName.get());
    }

    @Test
    public void test_clusterName_overClientDisconnectedEvent() {
        HazelcastInstance instance = hazelcastFactory.newHazelcastInstance();
        final CountDownLatch clientDisconnected = new CountDownLatch(1);

        final AtomicReference<String> clusterName = new AtomicReference<String>();
        instance.getClientService().addClientListener(new ClientListener() {
            @Override
            public void clientConnected(Client client) {

            }

            @Override
            public void clientDisconnected(Client client) {
                clusterName.set(client.getName());
                clientDisconnected.countDown();
            }
        });

        ClientConfig clientConfig = new ClientConfig();
        String name = "aClient";
        clientConfig.setInstanceName(name);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);
        client.shutdown();

        assertOpenEventually(clientDisconnected);
        assertEquals(name, clusterName.get());
    }
}
