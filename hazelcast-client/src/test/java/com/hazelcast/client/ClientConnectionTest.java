/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.impl.ClientTestUtil;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.spi.properties.ClientProperty;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.Client;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.core.Member;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionListener;
import com.hazelcast.security.UsernamePasswordCredentials;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.EmptyStatement;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientConnectionTest extends HazelcastTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void cleanup() {
        hazelcastFactory.terminateAll();
    }

    @Test(expected = IllegalStateException.class)
    public void testWithIllegalAddress() {
        String illegalAddress = randomString();

        hazelcastFactory.newHazelcastInstance();
        ClientConfig config = new ClientConfig();
        config.getNetworkConfig().setConnectionAttemptPeriod(1);
        config.getNetworkConfig().addAddress(illegalAddress);
        hazelcastFactory.newHazelcastClient(config);
    }

    @Test
    public void testWithLegalAndIllegalAddressTogether() {
        String illegalAddress = randomString();

        HazelcastInstance server = hazelcastFactory.newHazelcastInstance();
        ClientConfig config = new ClientConfig();
        config.setProperty(ClientProperty.SHUFFLE_MEMBER_LIST.getName(), "false");
        config.getNetworkConfig().addAddress(illegalAddress).addAddress("localhost");
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

        InetSocketAddress socketAddress1 = server1.getCluster().getLocalMember().getSocketAddress();
        InetSocketAddress socketAddress2 = server2.getCluster().getLocalMember().getSocketAddress();

        config.getNetworkConfig().
                addAddress(socketAddress1.getHostName() + ":" + socketAddress1.getPort()).
                addAddress(socketAddress2.getHostName() + ":" + socketAddress2.getPort());

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

        final CountingConnectionRemoveListener listener = new CountingConnectionRemoveListener();

        connectionManager.addConnectionListener(listener);

        final Address serverAddress = new Address(server.getCluster().getLocalMember().getSocketAddress());
        final Connection connectionToServer = connectionManager.getConnection(serverAddress);

        final CountDownLatch isConnected = new CountDownLatch(1);
        clientImpl.getLifecycleService().addLifecycleListener(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                if (LifecycleEvent.LifecycleState.CLIENT_CONNECTED == event.getState()) {
                    isConnected.countDown();
                }
            }
        });

        connectionManager.destroyConnection(connectionToServer, null, null);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertTrue(isConnected.await(5, TimeUnit.SECONDS));
            }
        });

        connectionManager.destroyConnection(connectionToServer, null, null);

        assertEquals("connection removed should be called only once", 1, listener.count.get());
    }

    private class CountingConnectionRemoveListener implements ConnectionListener {

        final AtomicInteger count = new AtomicInteger();

        @Override
        public void connectionAdded(Connection connection) {

        }

        @Override
        public void connectionRemoved(Connection connection) {
            count.incrementAndGet();
        }
    }

    @Test
    public void testAsyncConnectionCreationInAsyncMethods() throws ExecutionException, InterruptedException {
        hazelcastFactory.newHazelcastInstance();
        CountDownLatch countDownLatch = new CountDownLatch(1);
        ClientConfig config = new ClientConfig();
        WaitingCredentials credentials = new WaitingCredentials("dev", "dev-pass", countDownLatch);
        config.setCredentials(credentials);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(config);
        final IExecutorService executorService = client.getExecutorService(randomString());

        credentials.waitFlag.set(true);

        final HazelcastInstance secondInstance = hazelcastFactory.newHazelcastInstance();
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
            countDownLatch.countDown();
        }

    }


    static class WaitingCredentials extends UsernamePasswordCredentials {

        private final CountDownLatch countDownLatch;
        AtomicBoolean waitFlag = new AtomicBoolean();

        public WaitingCredentials(String username, String password, CountDownLatch countDownLatch) {
            super(username, password);
            this.countDownLatch = countDownLatch;
        }

        @Override
        public String getUsername() {
            return super.getUsername();
        }

        @Override
        public String getPassword() {
            if (waitFlag.get()) {
                try {
                    countDownLatch.await();
                } catch (InterruptedException e) {
                    EmptyStatement.ignore(e);
                }
            }
            return super.getPassword();
        }
    }

}
