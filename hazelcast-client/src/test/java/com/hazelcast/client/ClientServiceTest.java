/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.Client;
import com.hazelcast.core.ClientListener;
import com.hazelcast.core.ClientService;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientServiceTest extends HazelcastTestSupport {

    @After
    public void teardown() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testConnectedClients() {
        HazelcastInstance server = Hazelcast.newHazelcastInstance();

        HazelcastInstance client1 = HazelcastClient.newHazelcastClient();
        HazelcastInstance client2 = HazelcastClient.newHazelcastClient();

        ClientService clientService = server.getClientService();
        Collection<Client> connectedClients = clientService.getConnectedClients();
        assertEquals(2, connectedClients.size());

        String uuid1 = client1.getLocalEndpoint().getUuid();
        String uuid2 = client2.getLocalEndpoint().getUuid();
        for (Client connectedClient : connectedClients) {
            String uuid = connectedClient.getUuid();
            assertTrue(uuid.equals(uuid1) || uuid.equals(uuid2));
        }
    }

    @Test
    public void testClientListener() throws InterruptedException {
        HazelcastInstance server = Hazelcast.newHazelcastInstance();

        final AtomicInteger totalAdd = new AtomicInteger(0);
        final CountDownLatch latchAdd = new CountDownLatch(2);
        final CountDownLatch latchRemove = new CountDownLatch(2);
        ClientListener clientListener = new ClientListener() {
            @Override
            public void clientConnected(Client client) {
                totalAdd.incrementAndGet();
                latchAdd.countDown();
            }

            @Override
            public void clientDisconnected(Client client) {
                latchRemove.countDown();
            }
        };

        final ClientService clientService = server.getClientService();
        String id = clientService.addClientListener(clientListener);

        HazelcastInstance client1 = HazelcastClient.newHazelcastClient();
        HazelcastInstance client2 = HazelcastClient.newHazelcastClient();

        client1.getLifecycleService().shutdown();
        client2.getLifecycleService().shutdown();

        assertTrue(latchAdd.await(6, TimeUnit.SECONDS));
        assertTrue(latchRemove.await(6, TimeUnit.SECONDS));

        assertTrue(clientService.removeClientListener(id));

        assertFalse(clientService.removeClientListener("foo"));

        assertEquals(0, clientService.getConnectedClients().size());

        HazelcastClient.newHazelcastClient();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(1, clientService.getConnectedClients().size());
            }
        }, 4);

        assertEquals(2, totalAdd.get());
    }

    @Test
    public void testConnectedClientsWithReAuth() throws InterruptedException {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setConnectionAttemptPeriod(1000 * 10);

        final CountDownLatch countDownLatch = new CountDownLatch(2);
        clientConfig.addListenerConfig(new ListenerConfig(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                if (event.getState() == LifecycleEvent.LifecycleState.CLIENT_CONNECTED) {
                    countDownLatch.countDown();
                }
            }
        }));
        HazelcastInstance server = Hazelcast.newHazelcastInstance();
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        // Restart the node
        server.shutdown();
        Thread.sleep(1000);
        final HazelcastInstance restartedInstance = Hazelcast.newHazelcastInstance();

        client.getMap(randomMapName()).size(); // Do any operation

        assertOpenEventually(countDownLatch); // Wait for clients to reconnect & reAuth
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(1, restartedInstance.getClientService().getConnectedClients().size());
            }
        });
    }

    @Test
    public void testClientListenerForBothNodes() {
        HazelcastInstance server1 = Hazelcast.newHazelcastInstance();
        HazelcastInstance server2 = Hazelcast.newHazelcastInstance();

        ClientListenerLatch clientListenerLatch = new ClientListenerLatch(2);

        ClientService clientService1 = server1.getClientService();
        clientService1.addClientListener(clientListenerLatch);

        ClientService clientService2 = server2.getClientService();
        clientService2.addClientListener(clientListenerLatch);

        HazelcastInstance client = HazelcastClient.newHazelcastClient();
        String instance1Key = generateKeyOwnedBy(server1);
        String instance2Key = generateKeyOwnedBy(server2);

        IMap<Object, Object> map = client.getMap("map");
        map.put(instance1Key, 0);
        map.put(instance2Key, 0);

        assertClientConnected(clientService1, clientService2);
        assertOpenEventually(clientListenerLatch, 5);
    }

    private void assertClientConnected(ClientService... services) {
        for (final ClientService service : services) {
            assertTrueEventually(new AssertTask() {
                @Override
                public void run() throws Exception {
                    assertEquals(1, service.getConnectedClients().size());
                }
            }, 5);
        }
    }

    private static class ClientListenerLatch extends CountDownLatch implements ClientListener {
        public ClientListenerLatch(int count) {
            super(count);
        }

        @Override
        public void clientConnected(Client client) {
            countDown();
        }

        @Override
        public void clientDisconnected(Client client) {
        }
    }
}