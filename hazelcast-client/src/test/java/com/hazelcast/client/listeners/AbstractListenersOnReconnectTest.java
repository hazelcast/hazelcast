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
import com.hazelcast.client.impl.ClientEngineImpl;
import com.hazelcast.client.impl.ClientTestUtil;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.spi.impl.listener.ClientEventRegistration;
import com.hazelcast.client.spi.impl.listener.ClientListenerServiceImpl;
import com.hazelcast.client.spi.properties.ClientProperty;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.test.AssertTask;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Collection;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
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

    private static final int ENDPOINT_REMOVE_DELAY_MILLISECONDS = ClientEngineImpl.ENDPOINT_REMOVE_DELAY_SECONDS * 1000;

    private TestHazelcastFactory factory = new TestHazelcastFactory();

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    //-------------------------- testListenersTerminateRandomNode --------------------- //
    @Test
    @Ignore
    public void testListenersNonSmartRoutingTerminateRandomNode() {
        factory.newInstances(null, 3);
        ClientConfig clientConfig = getNonSmartClientConfig();
        client = factory.newHazelcastClient(clientConfig);
        testListenersTerminateRandomNode();
    }

    @Test
    @Ignore
    public void testListenersSmartRoutingTerminateRandomNode() {
        factory.newInstances(null, 3);
        ClientConfig clientConfig = getSmartClientConfig();
        client = factory.newHazelcastClient(clientConfig);
        testListenersTerminateRandomNode();
    }

    private void testListenersTerminateRandomNode() {
        setupListener();

        terminateRandomNode();

        HazelcastClientInstanceImpl clientInstanceImpl = getHazelcastClientInstanceImpl(client);
        final CountDownLatch memberAddedLatch = new CountDownLatch(1);
        clientInstanceImpl.getClientClusterService().addMembershipListener(new MembershipListener() {
            @Override
            public void memberAdded(MembershipEvent membershipEvent) {
                memberAddedLatch.countDown();
            }

            @Override
            public void memberRemoved(MembershipEvent membershipEvent) {
            }

            @Override
            public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {
            }
        });

        factory.newHazelcastInstance();

        assertOpenEventually(memberAddedLatch);
        validateRegistrationsAndListenerFunctionality();
    }

    //-------------------------- testListenersWaitMemberDestroy --------------------- //

    @Test
    @Ignore
    public void testListenersWaitMemberDestroySmartRouting() {
        factory.newInstances(null, 3);

        ClientConfig clientConfig = getSmartClientConfig();
        clientConfig
                .setProperty(ClientProperty.HEARTBEAT_TIMEOUT.getName(), String.valueOf(2 * ENDPOINT_REMOVE_DELAY_MILLISECONDS));
        client = factory.newHazelcastClient(clientConfig);
        testListenersWaitMemberDestroy();
    }

    private void testListenersWaitMemberDestroy() {
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

        HazelcastClientInstanceImpl clientInstanceImpl = getHazelcastClientInstanceImpl(client);
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

        sleepAtLeastMillis(ENDPOINT_REMOVE_DELAY_MILLISECONDS + 2000);
        clusterSize = clusterSize - 1;
        validateRegistrationsAndListenerFunctionality();
    }

    //-------------------------- testListenersTemporaryNetworkBlockage --------------------- //

    @Test
    @Ignore
    public void testTemporaryBlockedNoDisconnectionSmartRouting() {
        factory.newHazelcastInstance();

        ClientConfig clientConfig = getSmartClientConfig();
        client = factory.newHazelcastClient(clientConfig);

        testListenersTemporaryNetworkBlockage();
    }

    @Test
    @Ignore
    public void testTemporaryBlockedNoDisconnectionNonSmartRouting() {
        factory.newHazelcastInstance();

        ClientConfig clientConfig = getNonSmartClientConfig();
        client = factory.newHazelcastClient(clientConfig);

        testListenersTemporaryNetworkBlockage();
    }

    @Test
    @Ignore
    public void testTemporaryBlockedNoDisconnectionMultipleServerSmartRouting() {
        factory.newInstances(null, 3);

        ClientConfig clientConfig = getSmartClientConfig();
        client = factory.newHazelcastClient(clientConfig);

        testListenersTemporaryNetworkBlockage();
    }

    @Test
    @Ignore
    public void testTemporaryBlockedNoDisconnectionMultipleServerNonSmartRouting() {
        factory.newInstances(null, 3);

        ClientConfig clientConfig = getNonSmartClientConfig();
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
    @Ignore
    public void testClusterReconnectDueToHeartbeatSmartRouting() {
        factory.newHazelcastInstance();

        ClientConfig clientConfig = getSmartClientConfig();
        client = factory.newHazelcastClient(clientConfig);

        testListenersHeartbeatTimeoutToOwner();
    }

    @Test
    @Ignore
    public void testClusterReconnectMultipleServersDueToHeartbeatSmartRouting() {
        factory.newInstances(null, 3);

        ClientConfig clientConfig = getSmartClientConfig();
        client = factory.newHazelcastClient(clientConfig);

        testListenersHeartbeatTimeoutToOwner();
    }

    @Test
    @Ignore
    public void testClusterReconnectDueToHeartbeatNonSmartRouting() {
        factory.newHazelcastInstance();

        ClientConfig clientConfig = getNonSmartClientConfig();
        client = factory.newHazelcastClient(clientConfig);

        testListenersHeartbeatTimeoutToOwner();
    }

    @Test
    @Ignore
    public void testClusterReconnectMultipleServerDueToHeartbeatNonSmartRouting() {
        factory.newInstances(null, 3);

        ClientConfig clientConfig = getNonSmartClientConfig();
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
    @Ignore
    public void testListenersSmartRoutingMultipleServer() {
        factory.newInstances(null, 3);

        ClientConfig clientConfig = getSmartClientConfig();
        client = factory.newHazelcastClient(clientConfig);
        testListenersTerminateOwnerNode();
    }

    @Test
    @Ignore
    public void testListenersNonSmartRoutingMultipleServer() {
        factory.newInstances(null, 3);

        ClientConfig clientConfig = getNonSmartClientConfig();
        client = factory.newHazelcastClient(clientConfig);
        testListenersTerminateOwnerNode();
    }

    @Test
    @Ignore
    public void testListenersSmartRouting() {
        factory.newHazelcastInstance();

        ClientConfig clientConfig = getSmartClientConfig();
        client = factory.newHazelcastClient(clientConfig);
        testListenersTerminateOwnerNode();
    }

    @Test
    @Ignore
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

    //-------------------------- utility anc validation methods --------------------- //

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
                        Member registeredSubscriber = registration.getSubscriber();
                        assertTrue("Registered member " + registeredSubscriber + " is not in the cluster member list " + members,
                                members.contains(registeredSubscriber));
                    }
                } else {
                    ClientEventRegistration registration = registrations.iterator().next();
                    assertEquals(clientInstanceImpl.getClientClusterService().getOwnerConnectionAddress(),
                            registration.getSubscriber().getAddress());
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

    private ClientConfig getNonSmartClientConfig() {
        ClientConfig clientConfig = getSmartClientConfig();
        clientConfig.getNetworkConfig().setSmartRouting(false);
        return clientConfig;
    }

    private ClientConfig getSmartClientConfig() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setConnectionAttemptLimit(Integer.MAX_VALUE);
        clientConfig.getNetworkConfig().setRedoOperation(true);
        clientConfig.setProperty(ClientProperty.HEARTBEAT_TIMEOUT.getName(), "4000");
        clientConfig.setProperty(ClientProperty.HEARTBEAT_INTERVAL.getName(), "1000");
        return clientConfig;
    }

    protected abstract String addListener();

    protected abstract void produceEvent();

    protected abstract boolean removeListener(String registrationId);
}
