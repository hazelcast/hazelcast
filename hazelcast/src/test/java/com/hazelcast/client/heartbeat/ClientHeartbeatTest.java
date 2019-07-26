/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.heartbeat;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.connection.ClientConnectionManager;
import com.hazelcast.client.impl.connection.nio.ClientConnection;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientAddPartitionLostListenerCodec;
import com.hazelcast.client.impl.protocol.codec.ClientRemovePartitionLostListenerCodec;
import com.hazelcast.client.impl.spi.ClientListenerService;
import com.hazelcast.client.impl.spi.EventHandler;
import com.hazelcast.client.impl.spi.impl.ListenerMessageCodec;
import com.hazelcast.client.properties.ClientProperty;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.cluster.Member;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.core.LifecycleService;
import com.hazelcast.partition.Partition;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionListener;
import com.hazelcast.spi.exception.TargetDisconnectedException;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientHeartbeatTest extends ClientTestSupport {

    private static final int HEARTBEAT_TIMEOUT_MILLIS = 10000;

    private TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @After
    public void cleanup() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void testOwnerConnectionClosed_whenHeartbeatStopped() {
        HazelcastInstance instance = hazelcastFactory.newHazelcastInstance();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(getClientConfig());

        HazelcastClientInstanceImpl clientImpl = getHazelcastClientInstanceImpl(client);
        ClientConnectionManager connectionManager = clientImpl.getConnectionManager();
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        connectionManager.addConnectionListener(new ConnectionListener() {
            @Override
            public void connectionAdded(Connection connection) {

            }

            @Override
            public void connectionRemoved(Connection connection) {
                ClientConnection clientConnection = (ClientConnection) connection;
                if (clientConnection.isAuthenticatedAsOwner()) {
                    countDownLatch.countDown();
                }
            }
        });

        blockMessagesFromInstance(instance, client);
        assertOpenEventually(countDownLatch);
    }

    @Test
    public void testNonOwnerConnectionClosed_whenHeartbeatStopped() {
        hazelcastFactory.newHazelcastInstance();
        final HazelcastInstance client = hazelcastFactory.newHazelcastClient(getClientConfig());
        HazelcastInstance instance = hazelcastFactory.newHazelcastInstance();

        HazelcastClientInstanceImpl clientImpl = getHazelcastClientInstanceImpl(client);
        final ClientConnectionManager connectionManager = clientImpl.getConnectionManager();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(2, connectionManager.getActiveConnections().size());
            }
        });

        final CountDownLatch countDownLatch = new CountDownLatch(1);
        connectionManager.addConnectionListener(new ConnectionListener() {
            @Override
            public void connectionAdded(Connection connection) {

            }

            @Override
            public void connectionRemoved(Connection connection) {
                ClientConnection clientConnection = (ClientConnection) connection;
                if (!clientConnection.isAuthenticatedAsOwner()) {
                    countDownLatch.countDown();
                }
            }
        });

        blockMessagesFromInstance(instance, client);
        assertOpenEventually(countDownLatch);
    }

    @Test
    public void testInvocation_whenHeartbeatStopped() throws InterruptedException {
        hazelcastFactory.newHazelcastInstance();
        final HazelcastInstance client = hazelcastFactory.newHazelcastClient(getClientConfig());
        final HazelcastInstance instance2 = hazelcastFactory.newHazelcastInstance();

        // Make sure that the partitions are updated as expected with the new member
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                Member instance2Member = instance2.getCluster().getLocalMember();
                Set<Partition> partitions = client.getPartitionService().getPartitions();
                boolean found = false;
                for (Partition p : partitions) {
                    if (p.getOwner().equals(instance2Member)) {
                        found = true;
                        break;
                    }
                }
                assertTrue(found);
            }
        });

        // make sure client is connected to instance2
        String keyOwnedByInstance2 = generateKeyOwnedBy(instance2);
        IMap<String, String> map = client.getMap(randomString());
        map.put(keyOwnedByInstance2, randomString());
        blockMessagesFromInstance(instance2, client);

        expectedException.expect(TargetDisconnectedException.class);
        expectedException.expectMessage(containsString("Heartbeat"));
        map.put(keyOwnedByInstance2, randomString());
    }

    @Test
    public void testAsyncInvocation_whenHeartbeatStopped() throws Throwable {
        hazelcastFactory.newHazelcastInstance();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(getClientConfig());
        HazelcastInstance instance2 = hazelcastFactory.newHazelcastInstance();

        // make sure client is connected to instance2
        IMap<String, String> map = client.getMap(randomString());
        String keyOwnedByInstance2 = generateKeyOwnedBy(instance2);
        map.put(keyOwnedByInstance2, randomString());

        blockMessagesFromInstance(instance2, client);

        expectedException.expect(TargetDisconnectedException.class);
        expectedException.expectMessage(containsString("Heartbeat"));
        try {
            map.putAsync(keyOwnedByInstance2, randomString()).get();
        } catch (ExecutionException e) {
            //unwrap exception
            throw e.getCause();
        }
    }

    @Test
    public void testInvocation_whenHeartbeatResumed() throws InterruptedException {
        hazelcastFactory.newHazelcastInstance();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(getClientConfig());
        HazelcastInstance instance2 = hazelcastFactory.newHazelcastInstance();

        // make sure client is connected to instance2
        String keyOwnedByInstance2 = generateKeyOwnedBy(instance2);
        IMap<String, String> map = client.getMap(randomString());
        map.put(keyOwnedByInstance2, randomString());

        blockMessagesFromInstance(instance2, client);
        sleepMillis(HEARTBEAT_TIMEOUT_MILLIS * 2);
        unblockMessagesFromInstance(instance2, client);

        map.put(keyOwnedByInstance2, randomString());
    }

    private static ClientConfig getClientConfig() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(ClientProperty.HEARTBEAT_TIMEOUT.getName(), String.valueOf(HEARTBEAT_TIMEOUT_MILLIS));
        clientConfig.setProperty(ClientProperty.HEARTBEAT_INTERVAL.getName(), "500");
        return clientConfig;
    }


    @Test
    public void testAuthentication_whenHeartbeatResumed() throws Exception {
        HazelcastInstance hazelcastInstance = hazelcastFactory.newHazelcastInstance();
        ClientConfig config = new ClientConfig();
        config.setProperty(ClientProperty.SHUFFLE_MEMBER_LIST.getName(), "false");
        final HazelcastInstance client = hazelcastFactory.newHazelcastClient(config);
        HazelcastClientInstanceImpl hazelcastClientInstanceImpl = getHazelcastClientInstanceImpl(client);
        final ClientConnectionManager clientConnectionManager = hazelcastClientInstanceImpl.getConnectionManager();

        final CountDownLatch countDownLatch = new CountDownLatch(2);
        client.getLifecycleService().addLifecycleListener(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                countDownLatch.countDown();
            }
        });

        final HazelcastInstance instance2 = hazelcastFactory.newHazelcastInstance();
        blockMessagesFromInstance(instance2, client);

        final HazelcastInstance instance3 = hazelcastFactory.newHazelcastInstance();
        hazelcastInstance.shutdown();

        //wait for disconnect from instance1 since it is shutdown  // CLIENT_DISCONNECTED event
        //and wait for connect to from instance3 // CLIENT_CONNECTED event
        assertOpenEventually(countDownLatch);

        //verify and wait for authentication to 3 is complete
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                String uuid = instance3.getLocalEndpoint().getUuid();
                assertEquals(uuid, getClientEngineImpl(instance3).getOwnerUuid(client.getLocalEndpoint().getUuid()));
                assertEquals(uuid, getClientEngineImpl(instance2).getOwnerUuid(client.getLocalEndpoint().getUuid()));
                assertEquals(uuid, clientConnectionManager.getPrincipal().getOwnerUuid());
                assertEquals(instance3.getCluster().getLocalMember().getAddress(), clientConnectionManager.getOwnerConnectionAddress());
            }
        });

        //unblock instance 2 for authentication response.
        unblockMessagesFromInstance(instance2, client);

        //late authentication response from instance2 should not be able to change state in both client and cluster
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                String uuid = instance3.getLocalEndpoint().getUuid();
                assertEquals(uuid, getClientEngineImpl(instance3).getOwnerUuid(client.getLocalEndpoint().getUuid()));
                assertEquals(uuid, getClientEngineImpl(instance2).getOwnerUuid(client.getLocalEndpoint().getUuid()));
                assertEquals(uuid, clientConnectionManager.getPrincipal().getOwnerUuid());
                assertEquals(instance3.getCluster().getLocalMember().getAddress(), clientConnectionManager.getOwnerConnectionAddress());
            }
        });
    }

    @Test
    public void testClientEndpointsDelaySeconds_whenHeartbeatResumed() throws Exception {
        int delaySeconds = 2;
        Config config = new Config();
        config.setProperty(GroupProperty.CLIENT_ENDPOINT_REMOVE_DELAY_SECONDS.getName(), String.valueOf(delaySeconds));
        HazelcastInstance hazelcastInstance = hazelcastFactory.newHazelcastInstance(config);

        HazelcastInstance client = hazelcastFactory.newHazelcastClient(getClientConfig());

        final CountDownLatch disconnectedLatch = new CountDownLatch(1);

        LifecycleService lifecycleService = client.getLifecycleService();
        lifecycleService.addLifecycleListener(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                if (LifecycleEvent.LifecycleState.CLIENT_DISCONNECTED == event.getState()) {
                    disconnectedLatch.countDown();
                }
            }
        });

        blockMessagesFromInstance(hazelcastInstance, client);
        //Wait for client to disconnect because of hearBeat problem.
        assertOpenEventually(disconnectedLatch);

        final CountDownLatch connectedLatch = new CountDownLatch(1);
        final AtomicLong stateChangeCount = new AtomicLong();
        lifecycleService.addLifecycleListener(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                stateChangeCount.incrementAndGet();
                Logger.getLogger(this.getClass()).info("state event: " + event);
                if (LifecycleEvent.LifecycleState.CLIENT_CONNECTED == event.getState()) {
                    connectedLatch.countDown();
                }
            }
        });

        unblockMessagesFromInstance(hazelcastInstance, client);
        //Wait for client to connect back after heartbeat issue is resolved
        assertOpenEventually(connectedLatch);

        //After client connected, there should not be further change in client state
        //We are specifically testing for scheduled ClientDisconnectionOperation not to take action when run
        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(1, stateChangeCount.get());
            }
        }, delaySeconds * 2);
    }

    @Test
    public void testAddingListenerToNewConnectionFailedBecauseOfHeartbeat() throws Exception {
        hazelcastFactory.newHazelcastInstance();

        final HazelcastInstance client = hazelcastFactory.newHazelcastClient(getClientConfig());

        HazelcastClientInstanceImpl clientInstanceImpl = getHazelcastClientInstanceImpl(client);
        final ClientListenerService clientListenerService = clientInstanceImpl.getListenerService();
        final CountDownLatch blockIncoming = new CountDownLatch(1);
        final CountDownLatch heartbeatStopped = new CountDownLatch(1);
        final CountDownLatch onListenerRegister = new CountDownLatch(2);
        clientInstanceImpl.getConnectionManager().addConnectionListener(new ConnectionListener() {
            @Override
            public void connectionAdded(Connection connection) {

            }

            @Override
            public void connectionRemoved(Connection connection) {
                heartbeatStopped.countDown();
            }

        });

        clientListenerService.registerListener(createPartitionLostListenerCodec(), new EventHandler() {
            AtomicInteger count = new AtomicInteger(0);

            @Override
            public void handle(Object event) {

            }

            @Override
            public void beforeListenerRegister() {
                if (count.incrementAndGet() == 2) {
                    try {
                        blockIncoming.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }

            @Override
            public void onListenerRegister() {
                onListenerRegister.countDown();
            }

        });

        HazelcastInstance hazelcastInstance2 = hazelcastFactory.newHazelcastInstance();

        assertSizeEventually(2, clientInstanceImpl.getConnectionManager().getActiveConnections());

        blockMessagesFromInstance(hazelcastInstance2, client);
        assertOpenEventually(heartbeatStopped);
        blockIncoming.countDown();

        unblockMessagesFromInstance(hazelcastInstance2, client);
        assertOpenEventually(onListenerRegister);
    }

    private ListenerMessageCodec createPartitionLostListenerCodec() {
        return new ListenerMessageCodec() {
            @Override
            public ClientMessage encodeAddRequest(boolean localOnly) {
                return ClientAddPartitionLostListenerCodec.encodeRequest(localOnly);
            }

            @Override
            public String decodeAddResponse(ClientMessage clientMessage) {
                return ClientAddPartitionLostListenerCodec.decodeResponse(clientMessage).response;
            }

            @Override
            public ClientMessage encodeRemoveRequest(String realRegistrationId) {
                return ClientRemovePartitionLostListenerCodec.encodeRequest(realRegistrationId);
            }

            @Override
            public boolean decodeRemoveResponse(ClientMessage clientMessage) {
                return ClientRemovePartitionLostListenerCodec.decodeResponse(clientMessage).response;
            }
        };
    }
}
