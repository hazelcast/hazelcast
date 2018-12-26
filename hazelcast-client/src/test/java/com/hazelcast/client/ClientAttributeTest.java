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

package com.hazelcast.client;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.Client;
import com.hazelcast.core.ClientListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
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
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientAttributeTest {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();
    private final String key = "attributeKey";
    private final String value = "attributeValue";
    private final String nonExistingKey = "nonExistingKey";

    @After
    public void cleanup() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void test_clientAttribute_overClientConfig() {
        ClientConfig clientConfig = new ClientConfig();

        clientConfig.setAttribute(key, value);
        assertEquals(value, clientConfig.getAttributes().get(key));
        assertEquals(1, clientConfig.getAttributes().size());
        assertNull(clientConfig.getAttributes().get(nonExistingKey));
    }

    @Test
    public void test_clientAttribute_overGetConnectedClients() {
        HazelcastInstance instance = hazelcastFactory.newHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setAttribute(key, value);
        hazelcastFactory.newHazelcastClient(clientConfig);

        Collection<Client> connectedClients = instance.getClientService().getConnectedClients();
        Client client = connectedClients.iterator().next();

        assertEquals(1, client.getAttributes().size());
        assertEquals(value, client.getAttributes().get(key));
        assertNull(value, client.getAttributes().get(nonExistingKey));
    }

    @Test
    public void test_clientAttribute_overClientConnectedEvent() {
        HazelcastInstance instance = hazelcastFactory.newHazelcastInstance();
        final CountDownLatch clientConnected = new CountDownLatch(1);

        final AtomicReference<Client> clientRef = new AtomicReference<Client>();
        instance.getClientService().addClientListener(new ClientListener() {
            @Override
            public void clientConnected(Client client) {
                clientRef.set(client);
                clientConnected.countDown();
            }

            @Override
            public void clientDisconnected(Client client) {

            }
        });

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setAttribute(key, value);
        hazelcastFactory.newHazelcastClient(clientConfig);

        assertOpenEventually(clientConnected);
        Client client = clientRef.get();

        assertEquals(1, client.getAttributes().size());
        assertEquals(value, client.getAttributes().get(key));
        assertNull(value, client.getAttributes().get(nonExistingKey));
    }

    @Test
    public void test_clientAttribute_overClientDisconnectedEvent() {
        HazelcastInstance instance = hazelcastFactory.newHazelcastInstance();
        final CountDownLatch clientDisconnected = new CountDownLatch(1);

        final AtomicReference<Client> clientRef = new AtomicReference<Client>();
        instance.getClientService().addClientListener(new ClientListener() {
            @Override
            public void clientConnected(Client client) {

            }

            @Override
            public void clientDisconnected(Client client) {
                clientRef.set(client);
                clientDisconnected.countDown();
            }
        });

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setAttribute(key, value);
        HazelcastInstance clientInstance = hazelcastFactory.newHazelcastClient(clientConfig);
        clientInstance.shutdown();

        assertOpenEventually(clientDisconnected);
        Client client = clientRef.get();

        assertEquals(1, client.getAttributes().size());
        assertEquals(value, client.getAttributes().get(key));
        assertNull(value, client.getAttributes().get(nonExistingKey));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void test_modifyClientAttributes_overGetConnectedClients() {
        HazelcastInstance instance = hazelcastFactory.newHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setAttribute(key, value);
        hazelcastFactory.newHazelcastClient(clientConfig);

        Collection<Client> connectedClients = instance.getClientService().getConnectedClients();
        Client client = connectedClients.iterator().next();
        client.getAttributes().put(key, value);
    }
}
