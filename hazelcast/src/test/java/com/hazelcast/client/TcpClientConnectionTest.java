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
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.impl.clientside.ClientTestUtil;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.connection.ClientConnectionManager;
import com.hazelcast.client.properties.ClientProperty;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.ConnectionListener;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;

import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class TcpClientConnectionTest extends ClientTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void cleanup() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void testWithIllegalAddress() {
        String illegalAddress = randomString();

        hazelcastFactory.newHazelcastInstance();
        ClientConfig config = new ClientConfig();
        config.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(2000);
        config.getNetworkConfig().addAddress(illegalAddress);
        assertThrows(IllegalStateException.class, () -> HazelcastClient.newHazelcastClient(config));
    }

    @Test
    public void testEmptyStringAsAddress() {
        ClientNetworkConfig networkConfig = new ClientConfig().getNetworkConfig();
        assertThrows(IllegalArgumentException.class, () -> networkConfig.addAddress(""));
    }

    @Test
    public void testNullAsAddress() {
        ClientNetworkConfig networkConfig = new ClientConfig().getNetworkConfig();
        assertThrows(IllegalArgumentException.class, () -> networkConfig.addAddress(null));
    }

    @Test
    public void testNullAsAddresses() {
        ClientNetworkConfig networkConfig = new ClientConfig().getNetworkConfig();
        assertThrows(IllegalArgumentException.class, () -> networkConfig.addAddress(null, null));
    }

    @Test
    public void testWithLegalAndIllegalAddressTogether() {
        String illegalAddress = randomString();

        HazelcastInstance server = hazelcastFactory.newHazelcastInstance();
        Address serverAddress = server.getCluster().getLocalMember().getAddress();
        ClientConfig config = new ClientConfig();
        config.setProperty(ClientProperty.SHUFFLE_MEMBER_LIST.getName(), "false");
        config.getNetworkConfig()
                .addAddress(illegalAddress)
                .addAddress(serverAddress.getHost() + ":" + serverAddress.getPort());
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(config);

        Collection<Client> connectedClients = server.getClientService().getConnectedClients();
        assertEquals(connectedClients.size(), 1);

        Client serverSideClientInfo = connectedClients.iterator().next();
        assertEquals(serverSideClientInfo.getUuid(), client.getLocalEndpoint().getUuid());
    }

    @Test
    public void testMemberConnectionOrder() {
        HazelcastInstance server1 = hazelcastFactory.newHazelcastInstance();
        HazelcastInstance server2 = hazelcastFactory.newHazelcastInstance();

        ClientConfig config = new ClientConfig();
        config.setProperty(ClientProperty.SHUFFLE_MEMBER_LIST.getName(), "false");
        config.getNetworkConfig().setSmartRouting(false);

        Address address1 = server1.getCluster().getLocalMember().getAddress();
        Address address2 = server2.getCluster().getLocalMember().getAddress();

        config.getNetworkConfig().
                addAddress(address1.getHost() + ":" + address1.getPort()).
                addAddress(address2.getHost() + ":" + address2.getPort());

        hazelcastFactory.newHazelcastClient(config);

        Collection<Client> connectedClients1 = server1.getClientService().getConnectedClients();
        assertEquals(connectedClients1.size(), 1);

        Collection<Client> connectedClients2 = server2.getClientService().getConnectedClients();
        assertEquals(connectedClients2.size(), 0);
    }

    @Test
    public void destroyConnection_whenDestroyedMultipleTimes_thenListenerRemoveCalledOnce() {
        HazelcastInstance server = hazelcastFactory.newHazelcastInstance();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient();
        HazelcastClientInstanceImpl clientImpl = ClientTestUtil.getHazelcastClientInstanceImpl(client);
        ClientConnectionManager connectionManager = clientImpl.getConnectionManager();

        final CountingConnectionListener listener = new CountingConnectionListener();

        connectionManager.addConnectionListener(listener);

        UUID serverUuid = server.getCluster().getLocalMember().getUuid();
        final Connection connectionToServer = connectionManager.getConnection(serverUuid);

        ReconnectListener reconnectListener = new ReconnectListener();
        clientImpl.getLifecycleService().addLifecycleListener(reconnectListener);

        connectionToServer.close(null, null);
        assertOpenEventually(reconnectListener.reconnectedLatch);

        connectionToServer.close(null, null);
        assertEqualsEventually(() -> listener.connectionRemovedCount.get(), 1);
        sleepMillis(100);
        assertEquals("connection removed should be called only once", 1, listener.connectionRemovedCount.get());
    }

    private class CountingConnectionListener implements ConnectionListener {

        final AtomicInteger connectionRemovedCount = new AtomicInteger();
        final AtomicInteger connectionAddedCount = new AtomicInteger();

        @Override
        public void connectionAdded(Connection connection) {
            connectionAddedCount.incrementAndGet();
        }

        @Override
        public void connectionRemoved(Connection connection) {
            connectionRemovedCount.incrementAndGet();
        }
    }

    @Test
    public void testAsyncConnectionCreationInAsyncMethods() throws ExecutionException, InterruptedException {
        hazelcastFactory.newHazelcastInstance();
        ClientConfig config = new ClientConfig();
        final HazelcastInstance client = hazelcastFactory.newHazelcastClient(config);
        final IExecutorService executorService = client.getExecutorService(randomString());

        final HazelcastInstance secondInstance = hazelcastFactory.newHazelcastInstance();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(2, client.getCluster().getMembers().size());
            }
        });

        final AtomicReference<Future> atomicReference = new AtomicReference<Future>();
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                Member secondMember = secondInstance.getCluster().getLocalMember();
                Future future = executorService.submitToMember(new DummySerializableCallable(), secondMember);
                atomicReference.set(future);
            }
        });
        thread.start();
        try {
            assertTrueEventually(new AssertTask() {
                @Override
                public void run() throws Exception {
                    assertNotNull(atomicReference.get());
                }
            }, 30);
        } finally {
            thread.interrupt();
            thread.join();
        }
    }

    @Test
    public void testAddingConnectionListenerTwice_shouldCauseEventDeliveredTwice() {
        hazelcastFactory.newHazelcastInstance();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient();

        HazelcastClientInstanceImpl clientImpl = ClientTestUtil.getHazelcastClientInstanceImpl(client);
        ClientConnectionManager connectionManager = clientImpl.getConnectionManager();

        final CountingConnectionListener listener = new CountingConnectionListener();

        connectionManager.addConnectionListener(listener);
        connectionManager.addConnectionListener(listener);

        hazelcastFactory.newHazelcastInstance();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(listener.connectionAddedCount.get(), 2);
            }
        });
    }

    @Test
    public void testClientOpenClusterToAllEventually() {
        int memberCount = 4;
        for (int i = 0; i < memberCount; i++) {
            hazelcastFactory.newHazelcastInstance();
        }

        HazelcastInstance client = hazelcastFactory.newHazelcastClient();
        makeSureConnectedToServers(client, memberCount);

    }

    @Test
    public void testClientOpenClusterToAllEventually_onAsyncMode() {
        int memberCount = 4;
        for (int i = 0; i < memberCount; i++) {
            hazelcastFactory.newHazelcastInstance();
        }

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().setAsyncStart(true);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);
        makeSureConnectedToServers(client, memberCount);
    }
}
