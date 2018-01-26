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

package com.hazelcast.client;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.impl.ClientTestUtil;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.spi.impl.ClientExecutionServiceImpl;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.Client;
import com.hazelcast.core.ClientListener;
import com.hazelcast.core.ClientService;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.UnknownHostException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientServiceTest extends ClientTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void cleanup() {
        hazelcastFactory.terminateAll();
    }

    @Test(expected = NullPointerException.class)
    public void testAddClientListener_whenListenerIsNull() {
        HazelcastInstance instance = hazelcastFactory.newHazelcastInstance();

        ClientService clientService = instance.getClientService();
        clientService.addClientListener(null);
    }

    @Test(expected = NullPointerException.class)
    public void testRemoveClientListener_whenIdIsNull() {
        HazelcastInstance instance = hazelcastFactory.newHazelcastInstance();

        ClientService clientService = instance.getClientService();
        clientService.removeClientListener(null);
    }

    @Test(timeout = 120000)
    public void testRemoveClientListener_whenListenerAlreadyRemoved() {
        HazelcastInstance instance = hazelcastFactory.newHazelcastInstance();

        ClientService clientService = instance.getClientService();
        ClientListener clientListener = mock(ClientListener.class);
        String id = clientService.addClientListener(clientListener);

        // first time remove
        assertTrue(clientService.removeClientListener(id));

        // second time remove
        assertFalse(clientService.removeClientListener(id));
    }

    @Test(timeout = 120000)
    public void testRemoveClientListener_whenNonExistingId() {
        HazelcastInstance instance = hazelcastFactory.newHazelcastInstance();

        ClientService clientService = instance.getClientService();

        assertFalse(clientService.removeClientListener("foobar"));
    }

    @Test(timeout = 120000)
    public void testNumberOfClients_afterUnAuthenticatedClient() {
        final HazelcastInstance instance = hazelcastFactory.newHazelcastInstance();
        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.getGroupConfig().setPassword("wrongPassword");

        try {
            hazelcastFactory.newHazelcastClient(clientConfig);
        } catch (IllegalStateException ignored) {

        }

        assertEquals(0, instance.getClientService().getConnectedClients().size());
    }

    @Test(timeout = 120000)
    public void testNumberOfClients_afterUnAuthenticatedClient_withTwoNode() {
        final HazelcastInstance instance1 = hazelcastFactory.newHazelcastInstance();
        final HazelcastInstance instance2 = hazelcastFactory.newHazelcastInstance();
        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.getGroupConfig().setPassword("wrongPassword");

        try {
            hazelcastFactory.newHazelcastClient(clientConfig);
        } catch (IllegalStateException ignored) {

        }

        assertEquals(0, instance1.getClientService().getConnectedClients().size());
        assertEquals(0, instance2.getClientService().getConnectedClients().size());
    }


    @Test(timeout = 120000)
    @Category(NightlyTest.class)
    public void testNumberOfClients_afterUnAuthenticatedClient_withTwoNode_twoClient() {
        final HazelcastInstance instance1 = hazelcastFactory.newHazelcastInstance();
        final HazelcastInstance instance2 = hazelcastFactory.newHazelcastInstance();
        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.getGroupConfig().setPassword("wrongPassword");

        try {
            hazelcastFactory.newHazelcastClient(clientConfig);
        } catch (IllegalStateException ignored) {

        }
        try {
            hazelcastFactory.newHazelcastClient(clientConfig);
        } catch (IllegalStateException ignored) {

        }

        assertEquals(0, instance1.getClientService().getConnectedClients().size());
        assertEquals(0, instance2.getClientService().getConnectedClients().size());
    }


    @Test(timeout = 120000)
    public void testConnectedClients() {
        final HazelcastInstance instance = hazelcastFactory.newHazelcastInstance();

        final HazelcastInstance client1 = hazelcastFactory.newHazelcastClient();
        final HazelcastInstance client2 = hazelcastFactory.newHazelcastClient();

        final ClientService clientService = instance.getClientService();
        final Collection<Client> connectedClients = clientService.getConnectedClients();
        assertEquals(2, connectedClients.size());

        final String uuid1 = client1.getLocalEndpoint().getUuid();
        final String uuid2 = client2.getLocalEndpoint().getUuid();
        for (Client connectedClient : connectedClients) {
            final String uuid = connectedClient.getUuid();
            assertTrue(uuid.equals(uuid1) || uuid.equals(uuid2));
        }

    }

    @Test(timeout = 120000)
    public void testClientListener() throws InterruptedException {
        final HazelcastInstance instance = hazelcastFactory.newHazelcastInstance();
        final ClientService clientService = instance.getClientService();
        final CountDownLatch latchAdd = new CountDownLatch(2);
        final CountDownLatch latchRemove = new CountDownLatch(2);
        final AtomicInteger totalAdd = new AtomicInteger(0);

        final ClientListener clientListener = new ClientListener() {
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
        final String id = clientService.addClientListener(clientListener);

        final HazelcastInstance client1 = hazelcastFactory.newHazelcastClient();
        final HazelcastInstance client2 = hazelcastFactory.newHazelcastClient();

        client1.getLifecycleService().shutdown();
        client2.getLifecycleService().shutdown();

        assertOpenEventually(latchAdd);
        assertOpenEventually(latchRemove);

        assertTrue(clientService.removeClientListener(id));

        assertFalse(clientService.removeClientListener("foo"));

        assertEquals(0, clientService.getConnectedClients().size());

        hazelcastFactory.newHazelcastClient();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(1, clientService.getConnectedClients().size());
            }
        });

        assertEquals(2, totalAdd.get());
    }

    @Test(timeout = 120000)
    public void testConnectedClientsWithReAuth() throws InterruptedException {
        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setConnectionAttemptPeriod(1000 * 5);
        clientConfig.getNetworkConfig().setConnectionAttemptLimit(Integer.MAX_VALUE);

        final CountDownLatch countDownLatch = new CountDownLatch(2);

        clientConfig.addListenerConfig(new ListenerConfig(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                if (event.getState() == LifecycleEvent.LifecycleState.CLIENT_CONNECTED) {
                    countDownLatch.countDown();
                }


            }
        }));
        HazelcastInstance instance = hazelcastFactory.newHazelcastInstance();
        final HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        //restart the node
        instance.shutdown();
        final HazelcastInstance restartedInstance = hazelcastFactory.newHazelcastInstance();

        client.getMap(randomMapName()).size(); // do any operation

        assertOpenEventually(countDownLatch); //wait for clients to reconnect & reAuth
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(1, restartedInstance.getClientService().getConnectedClients().size());
            }
        });
    }

    @Test(timeout = 120000)
    public void testClientListenerForBothNodes() {
        final HazelcastInstance instance1 = hazelcastFactory.newHazelcastInstance();
        final HazelcastInstance instance2 = hazelcastFactory.newHazelcastInstance();
        final ClientConnectedListenerLatch clientListenerLatch = new ClientConnectedListenerLatch(2);

        final ClientService clientService1 = instance1.getClientService();
        clientService1.addClientListener(clientListenerLatch);
        final ClientService clientService2 = instance2.getClientService();
        clientService2.addClientListener(clientListenerLatch);

        final HazelcastInstance client = hazelcastFactory.newHazelcastClient();
        final String instance1Key = generateKeyOwnedBy(instance1);
        final String instance2Key = generateKeyOwnedBy(instance2);

        final IMap<Object, Object> map = client.getMap("map");
        map.put(instance1Key, 0);
        map.put(instance2Key, 0);

        assertClientConnected(clientService1, clientService2);
        assertOpenEventually(clientListenerLatch);
    }

    @Test(timeout = 120000)
    public void testClientListenerDisconnected() throws InterruptedException {
        Config config = new Config();
        config.setProperty(GroupProperty.IO_THREAD_COUNT.getName(), "1");

        final HazelcastInstance hz = hazelcastFactory.newHazelcastInstance(config);
        final HazelcastInstance hz2 = hazelcastFactory.newHazelcastInstance(config);

        int clientCount = 10;
        ClientDisconnectedListenerLatch listenerLatch = new ClientDisconnectedListenerLatch(2 * clientCount);
        hz.getClientService().addClientListener(listenerLatch);
        hz2.getClientService().addClientListener(listenerLatch);

        Collection<HazelcastInstance> clients = new LinkedList<HazelcastInstance>();
        for (int i = 0; i < clientCount; i++) {
            HazelcastInstance client = hazelcastFactory.newHazelcastClient();
            IMap<Object, Object> map = client.getMap(randomMapName());

            map.addEntryListener(new EntryAdapter<Object, Object>(), true);
            map.put(generateKeyOwnedBy(hz), "value");
            map.put(generateKeyOwnedBy(hz2), "value");

            clients.add(client);
        }

        ExecutorService ex = Executors.newFixedThreadPool(4);
        try {
            for (final HazelcastInstance client : clients) {
                ex.execute(new Runnable() {
                    @Override
                    public void run() {
                        client.shutdown();
                    }
                });
            }

            assertOpenEventually(listenerLatch);

            assertTrueEventually(new AssertTask() {
                @Override
                public void run() throws Exception {
                    assertEquals(0, hz.getClientService().getConnectedClients().size());
                }
            });
            assertTrueEventually(new AssertTask() {
                @Override
                public void run() throws Exception {
                    assertEquals(0, hz2.getClientService().getConnectedClients().size());
                }
            });
        } finally {
            ex.shutdown();
        }
    }

    @Test(timeout = 120000)
    public void testPendingEventPacketsWithEvents() throws InterruptedException, UnknownHostException {
        HazelcastInstance hazelcastInstance = hazelcastFactory.newHazelcastInstance();

        HazelcastInstance client = hazelcastFactory.newHazelcastClient();
        IMap map = client.getMap(randomName());
        map.addEntryListener(new EntryAdapter(), false);
        for (int i = 0; i < 10; i++) {
            map.put(randomString(), randomString());
        }
        HazelcastClientInstanceImpl clientInstanceImpl = ClientTestUtil.getHazelcastClientInstanceImpl(client);
        Address address = hazelcastInstance.getCluster().getLocalMember().getAddress();
        ClientConnectionManager connectionManager = clientInstanceImpl.getConnectionManager();
        final ClientConnection connection = (ClientConnection) connectionManager.getActiveConnection(address);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(0, connection.getPendingPacketCount());
            }
        });
    }

    private void assertClientConnected(ClientService... services) {
        for (final ClientService service : services) {
            assertTrueEventually(new AssertTask() {
                @Override
                public void run() throws Exception {
                    assertEquals(1, service.getConnectedClients().size());
                }
            });
        }
    }

    static class ClientConnectedListenerLatch extends CountDownLatch implements ClientListener {

        public ClientConnectedListenerLatch(int count) {
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

    static class ClientDisconnectedListenerLatch extends CountDownLatch implements ClientListener {

        public ClientDisconnectedListenerLatch(int count) {
            super(count);
        }

        @Override
        public void clientConnected(Client client) {
        }

        @Override
        public void clientDisconnected(Client client) {
            countDown();
        }
    }


    @Test
    public void testClientShutdownDuration_whenServersDown() {
        HazelcastInstance hazelcastInstance = hazelcastFactory.newHazelcastInstance();
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        ClientConfig config = new ClientConfig();
        config.getNetworkConfig().setConnectionAttemptPeriod(0).setConnectionAttemptLimit(1);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(config);
        client.getLifecycleService().addLifecycleListener(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                if (event.getState() == LifecycleEvent.LifecycleState.SHUTDOWN) {
                    countDownLatch.countDown();
                }
            }
        });

        hazelcastInstance.shutdown();
        assertOpenEventually(countDownLatch, ClientExecutionServiceImpl.TERMINATE_TIMEOUT_SECONDS);
    }

    @Test
    public void testClientListener_fromConfig() {
        Config config = new Config();
        final CountDownLatch connectedLatch = new CountDownLatch(1);
        final CountDownLatch disconnectedLatch = new CountDownLatch(1);
        ListenerConfig listenerConfig = new ListenerConfig(new ClientListener() {
            @Override
            public void clientConnected(Client client) {
                connectedLatch.countDown();
            }

            @Override
            public void clientDisconnected(Client client) {
                disconnectedLatch.countDown();
            }
        });

        config.addListenerConfig(listenerConfig);

        HazelcastInstance instance = hazelcastFactory.newHazelcastInstance(config);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient();
        client.shutdown();
        assertOpenEventually(connectedLatch);
        assertOpenEventually(disconnectedLatch);
        instance.shutdown();
    }

    @Test
    public void testClientListener_withDummyClient() {
        Config config = new Config();
        final CountDownLatch latch = new CountDownLatch(2);
        final AtomicInteger eventCount = new AtomicInteger();
        ListenerConfig listenerConfig = new ListenerConfig(new ClientListener() {
            @Override
            public void clientConnected(Client client) {
                eventCount.incrementAndGet();
                latch.countDown();
            }

            @Override
            public void clientDisconnected(Client client) {
                eventCount.incrementAndGet();
                latch.countDown();
            }
        });

        config.addListenerConfig(listenerConfig);

        hazelcastFactory.newHazelcastInstance(config);
        hazelcastFactory.newHazelcastInstance(config);

        HazelcastInstance client = hazelcastFactory.newHazelcastClient();
        client.shutdown();
        assertOpenEventually(latch);
        //client events will only be fired from one of the nodes.
        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(2, eventCount.get());
            }
        }, 4);
    }

    @Test
    public void testClientListener_withShuttingDownOwnerMember() throws InterruptedException {
        Config config = new Config();
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicInteger atomicInteger = new AtomicInteger();
        ListenerConfig listenerConfig = new ListenerConfig(new ClientListener() {
            @Override
            public void clientConnected(Client client) {
                atomicInteger.incrementAndGet();
                latch.countDown();
            }

            @Override
            public void clientDisconnected(Client client) {
                atomicInteger.incrementAndGet();
            }
        });

        config.addListenerConfig(listenerConfig);
        HazelcastInstance instance = hazelcastFactory.newHazelcastInstance();
        //first member is owner connection
        hazelcastFactory.newHazelcastClient();

        config.setProperty(GroupProperty.CLIENT_ENDPOINT_REMOVE_DELAY_SECONDS.getName(), String.valueOf(Integer.MAX_VALUE));
        hazelcastFactory.newHazelcastInstance(config);
        //make sure connected to second one before proceeding
        assertOpenEventually(latch);

        //when first node is dead, client selects second one as owner
        instance.shutdown();
        //Testing that shutting down first member does not cause any client connected/disconnected event
        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(1, atomicInteger.get());
            }
        }, 4);
    }

    @Test
    public void testAddingListenersOpenConnectionsToAllCluster() {
        int memberCount = 4;
        for (int i = 0; i < memberCount; i++) {
            hazelcastFactory.newHazelcastInstance();
        }

        HazelcastClientInstanceImpl client = getHazelcastClientInstanceImpl(hazelcastFactory.newHazelcastClient());
        ClientConnectionManager connectionManager = client.getConnectionManager();
        IMap<Object, Object> map = client.getMap("test");
        map.addEntryListener(mock(EntryListener.class), false);

        assertEquals(memberCount, connectionManager.getActiveConnections().size());

    }
}
