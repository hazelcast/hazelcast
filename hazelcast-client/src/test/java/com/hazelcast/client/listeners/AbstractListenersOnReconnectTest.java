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

package com.hazelcast.client.listeners;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.ClientTestUtil;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.spi.impl.listener.ClientEventRegistration;
import com.hazelcast.client.spi.impl.listener.ClientListenerServiceImpl;
import com.hazelcast.client.spi.properties.ClientProperty;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.core.Member;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import org.junit.After;
import org.junit.Test;

import java.util.Collection;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.client.spi.properties.ClientProperty.HEARTBEAT_TIMEOUT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class AbstractListenersOnReconnectTest extends ClientTestSupport {

    protected HazelcastInstance client;
    protected AtomicInteger eventCount;
    private String registrationId;
    private int clusterSize;

    private static final int EVENT_COUNT = 10;
    private TestHazelcastFactory factory = new TestHazelcastFactory();

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    //-------------------------- testListenersTerminateRandomNode --------------------- //
    @Test
    public void testListenersNonSmartRoutingTerminateRandomNode() {
        factory.newInstances(null, 3);
        ClientConfig clientConfig = getNonSmartClientConfig();
        client = factory.newHazelcastClient(clientConfig);
        testListenersTerminateRandomNode();
    }

    @Test
    public void testListenersSmartRoutingTerminateRandomNode() {
        factory.newInstances(null, 3);
        ClientConfig clientConfig = getSmartClientConfig();
        client = factory.newHazelcastClient(clientConfig);
        testListenersTerminateRandomNode();
    }

    private void testListenersTerminateRandomNode() {
        setupListener();

        final CountDownLatch disconnectedLatch = new CountDownLatch(1);
        final CountDownLatch connectedLatch = new CountDownLatch(1);
        client.getLifecycleService().addLifecycleListener(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                if (LifecycleEvent.LifecycleState.CLIENT_DISCONNECTED == event.getState()) {
                    disconnectedLatch.countDown();
                }
                if (LifecycleEvent.LifecycleState.CLIENT_CONNECTED == event.getState()) {
                    connectedLatch.countDown();
                }
            }
        });

        terminateRandomNode();
        assertOpenEventually(disconnectedLatch);
        factory.newHazelcastInstance();
        assertOpenEventually(connectedLatch);

        assertClusterSizeEventually(clusterSize, client);
        validateRegistrationsAndListenerFunctionality();
    }

    //-------------------------- testListenersWaitMemberDestroy --------------------- //

    @Test
    public void testListenersWaitMemberDestroySmartRouting() {
        Config config = new Config();
        int endpointDelaySeconds = 2;
        config.setProperty(GroupProperty.CLIENT_ENDPOINT_REMOVE_DELAY_SECONDS.getName(), String.valueOf(endpointDelaySeconds));
        factory.newInstances(config, 3);
        client = factory.newHazelcastClient(getSmartClientConfig());

        setupListener();

        Collection<HazelcastInstance> allHazelcastInstances = factory.getAllHazelcastInstances();

        final CountDownLatch disconnectedLatch = new CountDownLatch(1);
        final CountDownLatch connectedLatch = new CountDownLatch(1);
        client.getLifecycleService().addLifecycleListener(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                if (LifecycleEvent.LifecycleState.CLIENT_DISCONNECTED == event.getState()) {
                    disconnectedLatch.countDown();
                }
                if (LifecycleEvent.LifecycleState.CLIENT_CONNECTED == event.getState()) {
                    connectedLatch.countDown();
                }
            }
        });
        final HazelcastClientInstanceImpl clientInstanceImpl = getHazelcastClientInstanceImpl(client);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(3, clientInstanceImpl.getConnectionManager().getActiveConnections().size());
            }
        });

        HazelcastInstance ownerMember = getOwnerServer(factory, clientInstanceImpl);
        for (HazelcastInstance member : allHazelcastInstances) {
            blockMessagesFromInstance(member, client);
        }

        ownerMember.getLifecycleService().terminate();

        for (HazelcastInstance member : allHazelcastInstances) {
            unblockMessagesFromInstance(member, client);
        }

        assertOpenEventually(disconnectedLatch);
        assertOpenEventually(connectedLatch);

        sleepAtLeastMillis(endpointDelaySeconds * 1000 + 2000);
        clusterSize = clusterSize - 1;
        validateRegistrationsAndListenerFunctionality();
    }

    //--------------------------------------------------------------------------------- //

    @Test
    public void testListenersWhenClientDisconnectedOperationRuns_whenOwnerMemberRemoved() {
        Config config = new Config();
        int endpointDelaySeconds = 2;
        config.setProperty(GroupProperty.CLIENT_ENDPOINT_REMOVE_DELAY_SECONDS.getName(), String.valueOf(endpointDelaySeconds));
        HazelcastInstance ownerServer = factory.newHazelcastInstance(config);
        client = factory.newHazelcastClient(getSmartClientConfig());
        HazelcastInstance server2 = factory.newHazelcastInstance(config);

        setupListener();

        final CountDownLatch disconnectedLatch = new CountDownLatch(1);
        final CountDownLatch connectedLatch = new CountDownLatch(1);
        client.getLifecycleService().addLifecycleListener(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                if (LifecycleEvent.LifecycleState.CLIENT_DISCONNECTED == event.getState()) {
                    disconnectedLatch.countDown();
                }
                if (LifecycleEvent.LifecycleState.CLIENT_CONNECTED == event.getState()) {
                    connectedLatch.countDown();
                }
            }
        });

        blockMessagesToInstance(server2, client);

        ownerServer.shutdown();

        sleepAtLeastMillis(TimeUnit.SECONDS.toMillis(endpointDelaySeconds) * 2);

        unblockMessagesToInstance(server2, client);

        assertOpenEventually(disconnectedLatch);
        assertOpenEventually(connectedLatch);

        clusterSize = clusterSize - 1;
        validateRegistrationsAndListenerFunctionality();
    }

    @Test
    public void testListenersWhenClientDisconnectedOperationRuns_whenOwnerConnectionRemoved() {
        Config config = new Config();
        int endpointDelaySeconds = 2;
        config.setProperty(GroupProperty.CLIENT_ENDPOINT_REMOVE_DELAY_SECONDS.getName(), String.valueOf(endpointDelaySeconds));
        config.setProperty(GroupProperty.CLIENT_HEARTBEAT_TIMEOUT_SECONDS.getName(), "4");
        HazelcastInstance ownerServer = factory.newHazelcastInstance(config);
        ClientConfig smartClientConfig = getSmartClientConfig();
        smartClientConfig.setProperty(ClientProperty.HEARTBEAT_INTERVAL.getName(), "500");

        client = factory.newHazelcastClient(smartClientConfig);
        factory.newHazelcastInstance(config);
        setupListener();

        final CountDownLatch disconnectedLatch = new CountDownLatch(1);
        final CountDownLatch connectedLatch = new CountDownLatch(1);
        client.getLifecycleService().addLifecycleListener(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                if (LifecycleEvent.LifecycleState.CLIENT_DISCONNECTED == event.getState()) {
                    disconnectedLatch.countDown();
                }
                if (LifecycleEvent.LifecycleState.CLIENT_CONNECTED == event.getState()) {
                    connectedLatch.countDown();
                }
            }
        });

        blockMessagesToInstance(ownerServer, client);

        assertOpenEventually(disconnectedLatch);
        sleepAtLeastMillis(TimeUnit.SECONDS.toMillis(endpointDelaySeconds) * 2);

        unblockMessagesToInstance(ownerServer, client);

        assertOpenEventually(connectedLatch);

        validateRegistrationsAndListenerFunctionality();
    }

    //-------------------------- testListenersTemporaryNetworkBlockage --------------------- //

    @Test
    public void testTemporaryBlockedNoDisconnectionSmartRouting() {
        factory.newHazelcastInstance();

        ClientConfig clientConfig = getSmartClientConfig(4, 1);
        client = factory.newHazelcastClient(clientConfig);

        testListenersTemporaryNetworkBlockage();
    }

    @Test
    public void testTemporaryBlockedNoDisconnectionNonSmartRouting() {
        factory.newHazelcastInstance();

        ClientConfig clientConfig = getNonSmartClientConfig(4, 1);
        client = factory.newHazelcastClient(clientConfig);

        testListenersTemporaryNetworkBlockage();
    }

    @Test
    public void testTemporaryBlockedNoDisconnectionMultipleServerSmartRouting() {
        factory.newInstances(null, 3);

        ClientConfig clientConfig = getSmartClientConfig(4, 1);
        client = factory.newHazelcastClient(clientConfig);

        testListenersTemporaryNetworkBlockage();
    }

    @Test
    public void testTemporaryBlockedNoDisconnectionMultipleServerNonSmartRouting() {
        factory.newInstances(null, 3);

        ClientConfig clientConfig = getNonSmartClientConfig(4, 1);
        client = factory.newHazelcastClient(clientConfig);

        testListenersTemporaryNetworkBlockage();
    }

    private void testListenersTemporaryNetworkBlockage() {
        setupListener();

        HazelcastClientInstanceImpl clientInstanceImpl = getHazelcastClientInstanceImpl(client);
        HazelcastInstance server = getOwnerServer(factory, clientInstanceImpl);


        long timeout = clientInstanceImpl.getProperties().getMillis(HEARTBEAT_TIMEOUT);
        long heartbeatTimeout = timeout > 0 ? timeout : Integer.parseInt(HEARTBEAT_TIMEOUT.getDefaultValue());
        long waitTime = heartbeatTimeout / 2;

        long endTime = System.currentTimeMillis() + waitTime;
        blockMessagesFromInstance(server, client);
        long sleepTime = endTime - System.currentTimeMillis();

        if (sleepTime > 0) {
            sleepMillis((int) sleepTime);
        }

        unblockMessagesFromInstance(server, client);

        validateRegistrationsAndListenerFunctionality();
    }

    //-------------------------- testListenersHeartbeatTimeoutToOwner --------------------- //

    @Test
    public void testClusterReconnectDueToHeartbeatSmartRouting() {
        factory.newHazelcastInstance();

        ClientConfig clientConfig = getSmartClientConfig(4, 1);
        client = factory.newHazelcastClient(clientConfig);

        testListenersHeartbeatTimeoutToOwner();
    }

    @Test
    public void testClusterReconnectMultipleServersDueToHeartbeatSmartRouting() {
        factory.newInstances(null, 3);

        ClientConfig clientConfig = getSmartClientConfig(4, 1);
        client = factory.newHazelcastClient(clientConfig);

        testListenersHeartbeatTimeoutToOwner();
    }

    @Test
    public void testClusterReconnectDueToHeartbeatNonSmartRouting() {
        factory.newHazelcastInstance();

        ClientConfig clientConfig = getNonSmartClientConfig(4, 1);
        client = factory.newHazelcastClient(clientConfig);

        testListenersHeartbeatTimeoutToOwner();
    }

    @Test
    public void testClusterReconnectMultipleServerDueToHeartbeatNonSmartRouting() {
        factory.newInstances(null, 3);

        ClientConfig clientConfig = getNonSmartClientConfig(4, 1);
        client = factory.newHazelcastClient(clientConfig);

        testListenersHeartbeatTimeoutToOwner();
    }

    private void testListenersHeartbeatTimeoutToOwner() {
        setupListener();

        HazelcastClientInstanceImpl clientInstanceImpl = getHazelcastClientInstanceImpl(client);
        HazelcastInstance server = getOwnerServer(factory, clientInstanceImpl);


        final CountDownLatch disconnectedLatch = new CountDownLatch(1);
        final CountDownLatch connectedLatch = new CountDownLatch(1);
        client.getLifecycleService().addLifecycleListener(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                if (LifecycleEvent.LifecycleState.CLIENT_DISCONNECTED == event.getState()) {
                    disconnectedLatch.countDown();
                }
                if (LifecycleEvent.LifecycleState.CLIENT_CONNECTED == event.getState()) {
                    connectedLatch.countDown();
                }
            }
        });

        blockMessagesFromInstance(server, client);
        assertOpenEventually(disconnectedLatch);

        unblockMessagesFromInstance(server, client);
        assertOpenEventually(connectedLatch);

        validateRegistrationsAndListenerFunctionality();
    }


    //-------------------------- testListenersTerminateOwnerNode --------------------- //

    @Test
    public void testListenersSmartRoutingMultipleServer() {
        factory.newInstances(null, 3);

        ClientConfig clientConfig = getSmartClientConfig();
        client = factory.newHazelcastClient(clientConfig);
        testListenersTerminateOwnerNode();
    }

    @Test
    public void testListenersNonSmartRoutingMultipleServer() {
        factory.newInstances(null, 3);

        ClientConfig clientConfig = getNonSmartClientConfig();
        client = factory.newHazelcastClient(clientConfig);
        testListenersTerminateOwnerNode();
    }

    @Test
    public void testListenersSmartRouting() {
        factory.newHazelcastInstance();

        ClientConfig clientConfig = getSmartClientConfig();
        client = factory.newHazelcastClient(clientConfig);
        testListenersTerminateOwnerNode();
    }

    @Test
    public void testListenersNonSmartRouting() {
        factory.newHazelcastInstance();

        ClientConfig clientConfig = getNonSmartClientConfig();
        client = factory.newHazelcastClient(clientConfig);

        testListenersTerminateOwnerNode();
    }

    private void testListenersTerminateOwnerNode() {
        setupListener();

        final CountDownLatch disconnectedLatch = new CountDownLatch(1);
        final CountDownLatch connectedLatch = new CountDownLatch(1);
        client.getLifecycleService().addLifecycleListener(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                if (LifecycleEvent.LifecycleState.CLIENT_DISCONNECTED == event.getState()) {
                    disconnectedLatch.countDown();
                }
                if (LifecycleEvent.LifecycleState.CLIENT_CONNECTED == event.getState()) {
                    connectedLatch.countDown();
                }
            }
        });

        HazelcastClientInstanceImpl clientInstanceImpl = getHazelcastClientInstanceImpl(client);
        HazelcastInstance server = getOwnerServer(factory, clientInstanceImpl);
        server.getLifecycleService().terminate();

        factory.newHazelcastInstance();
        assertClusterSizeEventually(clusterSize, client);

        assertOpenEventually(disconnectedLatch);
        assertOpenEventually(connectedLatch);

        validateRegistrationsAndListenerFunctionality();
    }

    //-------------------------- utility and validation methods --------------------- //

    private void setupListener() {
        clusterSize = factory.getAllHazelcastInstances().size();
        assertClusterSizeEventually(clusterSize, client);
        eventCount = new AtomicInteger();
        registrationId = addListener();
    }

    private void validateRegistrationsAndListenerFunctionality() {
        assertClusterSizeEventually(clusterSize, client);
        validateRegistrations(clusterSize, registrationId, getHazelcastClientInstanceImpl(client));
        validateListenerFunctionality();
        assertTrue(removeListener(registrationId));
    }


    private void validateRegistrations(final int clusterSize, final String registrationId,
                                       final HazelcastClientInstanceImpl clientInstanceImpl) {
        final boolean smartRouting = clientInstanceImpl.getClientConfig().getNetworkConfig().isSmartRouting();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                int size = smartRouting ? clusterSize : 1;
                Collection<ClientEventRegistration> registrations = getClientEventRegistrations(client,
                        registrationId);
                assertEquals(size, registrations.size());
                if (smartRouting) {
                    Collection<Member> members = clientInstanceImpl.getClientClusterService().getMemberList();
                    for (ClientEventRegistration registration : registrations) {
                        Connection registeredSubscriber = registration.getSubscriber();
                        boolean contains = false;
                        for (Member member : members) {
                            contains |= registeredSubscriber.getEndPoint().equals(member.getAddress());
                        }
                        assertTrue("Registered member " + registeredSubscriber + " is not in the cluster member list " + members,
                                contains);
                    }
                } else {
                    ClientEventRegistration registration = registrations.iterator().next();
                    assertEquals(clientInstanceImpl.getClientClusterService().getOwnerConnectionAddress(),
                            registration.getSubscriber().getEndPoint());
                }
            }
        });
    }

    private void validateListenerFunctionality() {
        for (int i = 0; i < EVENT_COUNT; i++) {
            produceEvent();
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                int count = eventCount.get();
                assertTrue("Received event count is " + count + " but it is expected to be at least " + EVENT_COUNT,
                        count >= EVENT_COUNT);
            }
        });

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                int count = eventCount.get();
                assertEquals("Received event count is " + count + " but it is expected to stay at " + EVENT_COUNT, EVENT_COUNT,
                        eventCount.get());
            }
        }, 3);
    }

    private void terminateRandomNode() {
        int clusterSize = factory.getAllHazelcastInstances().size();
        HazelcastInstance[] instances = new HazelcastInstance[clusterSize];
        factory.getAllHazelcastInstances().toArray(instances);
        int randNode = new Random().nextInt(clusterSize);
        instances[randNode].getLifecycleService().terminate();
    }

    private Collection<ClientEventRegistration> getClientEventRegistrations(HazelcastInstance client, String id) {
        HazelcastClientInstanceImpl clientImpl = ClientTestUtil.getHazelcastClientInstanceImpl(client);
        ClientListenerServiceImpl listenerService = (ClientListenerServiceImpl) clientImpl.getListenerService();
        return listenerService.getActiveRegistrations(id);
    }

    private ClientConfig getNonSmartClientConfig(int heartbeatTimeoutSeconds, int heartbeatIntervalSeconds) {
        ClientConfig clientConfig = getSmartClientConfig(heartbeatTimeoutSeconds, heartbeatIntervalSeconds);
        clientConfig.getNetworkConfig().setSmartRouting(false);
        return clientConfig;
    }

    private ClientConfig getSmartClientConfig(int heartbeatTimeoutSeconds, int heartbeatIntervalSeconds) {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setConnectionAttemptLimit(Integer.MAX_VALUE);
        clientConfig.getNetworkConfig().setRedoOperation(true);
        clientConfig.setProperty(ClientProperty.HEARTBEAT_TIMEOUT.getName(), String.valueOf(TimeUnit.SECONDS.toMillis(heartbeatTimeoutSeconds)));
        clientConfig.setProperty(ClientProperty.HEARTBEAT_INTERVAL.getName(), String.valueOf(TimeUnit.SECONDS.toMillis(heartbeatIntervalSeconds)));
        return clientConfig;
    }

    private ClientConfig getSmartClientConfig() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setConnectionAttemptLimit(Integer.MAX_VALUE);
        clientConfig.getNetworkConfig().setRedoOperation(true);
        return clientConfig;
    }

    private ClientConfig getNonSmartClientConfig() {
        ClientConfig clientConfig = getSmartClientConfig();
        clientConfig.getNetworkConfig().setSmartRouting(false);
        return clientConfig;
    }

    protected abstract String addListener();

    protected abstract void produceEvent();

    protected abstract boolean removeListener(String registrationId);
}
