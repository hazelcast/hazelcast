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
    private static final int EVENT_COUNT = 10;

    private static final int ENDPOINT_REMOVE_DELAY_MILLISECONDS = ClientEngineImpl.ENDPOINT_REMOVE_DELAY_SECONDS * 1000;

    private TestHazelcastFactory factory = new TestHazelcastFactory();

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    private void testListenersInternal()
            throws InterruptedException {
        final int clusterSize = factory.getAllHazelcastInstances().size();
        assertClusterSizeEventually(clusterSize, client);

        final AtomicInteger eventCount = new AtomicInteger();
        final String registrationId = addListener(eventCount);

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
        HazelcastInstance server = getOwnerServer(factory,  clientInstanceImpl);
        server.getLifecycleService().terminate();

        factory.newHazelcastInstance();
        assertClusterSizeEventually(clusterSize, client);

        assertTrue(disconnectedLatch.await(30, TimeUnit.SECONDS));
        assertTrue(connectedLatch.await(30, TimeUnit.SECONDS));

        validateRegistrations(clusterSize, registrationId, clientInstanceImpl);

        validateListenerFunctionality(eventCount);

        assertTrue(removeListener(registrationId));
    }

    private void testListenersTerminateRandomNodeInternal()
            throws InterruptedException {
        final int clusterSize = factory.getAllHazelcastInstances().size();
        assertClusterSizeEventually(clusterSize, client);

        final AtomicInteger eventCount = new AtomicInteger();
        final String registrationId = addListener(eventCount);

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

        assertTrue(memberAddedLatch.await(20, TimeUnit.SECONDS));

        assertClusterSizeEventually(clusterSize, client);

        validateRegistrations(clusterSize, registrationId, getHazelcastClientInstanceImpl(client));

        validateListenerFunctionality(eventCount);

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

    private void testListenersWaitMemberDestroy()
            throws InterruptedException {
        Collection<HazelcastInstance> allHazelcastInstances = factory.getAllHazelcastInstances();
        final int clusterSize = allHazelcastInstances.size();
        assertClusterSizeEventually(clusterSize, client);

        final AtomicInteger eventCount = new AtomicInteger();
        final String registrationId = addListener(eventCount);

        final CountDownLatch disconnectedLatch = new CountDownLatch(1);
        final CountDownLatch connectedLatch = new CountDownLatch(1);
        client.getLifecycleService().addLifecycleListener(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                if (LifecycleEvent.LifecycleState.CLIENT_DISCONNECTED == event.getState()) {
                    disconnectedLatch.countDown();
                }
                if (LifecycleEvent.LifecycleState.CLIENT_CONNECTED== event.getState()) {
                    connectedLatch.countDown();
                }
            }
        });

        HazelcastClientInstanceImpl clientInstanceImpl = getHazelcastClientInstanceImpl(client);
        HazelcastInstance ownerMember = getOwnerServer(factory,  clientInstanceImpl);
        for (HazelcastInstance member : allHazelcastInstances) {
            blockMessagesFromInstance(member, client);
        }

        ownerMember.getLifecycleService().terminate();

        for (HazelcastInstance member : allHazelcastInstances) {
            unblockMessagesFromInstance(member, client);
        }

        assertTrue(disconnectedLatch.await(30, TimeUnit.SECONDS));
        assertTrue(connectedLatch.await(30, TimeUnit.SECONDS));

        sleepAtLeastMillis(ENDPOINT_REMOVE_DELAY_MILLISECONDS + 2000);

        validateRegistrations(clusterSize - 1, registrationId, clientInstanceImpl);

        validateListenerFunctionality(eventCount);

        assertTrue(removeListener(registrationId));
    }

    private void testListenersForHeartbeat()
            throws InterruptedException {
        HazelcastClientInstanceImpl clientInstanceImpl = getHazelcastClientInstanceImpl(client);

        HazelcastInstance server = getOwnerServer(factory,  clientInstanceImpl);

        final int clusterSize = factory.getAllHazelcastInstances().size();
        assertClusterSizeEventually(clusterSize, client);

        final AtomicInteger eventCount = new AtomicInteger();
        final String registrationId = addListener(eventCount);

        final CountDownLatch disconnectedLatch = new CountDownLatch(1);
        final CountDownLatch connectedLatch = new CountDownLatch(1);
        client.getLifecycleService().addLifecycleListener(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                if (LifecycleEvent.LifecycleState.CLIENT_DISCONNECTED == event.getState()) {
                    disconnectedLatch.countDown();
                }
                if (LifecycleEvent.LifecycleState.CLIENT_CONNECTED== event.getState()) {
                    connectedLatch.countDown();
                }
            }
        });

        long timeout = clientInstanceImpl.getProperties().getMillis(HEARTBEAT_TIMEOUT);
        long heartbeatTimeout = timeout > 0 ? timeout : Integer.parseInt(HEARTBEAT_TIMEOUT.getDefaultValue());
        long waitTime = heartbeatTimeout + 1000;

        blockMessagesFromInstance(server, client);

        disconnectedLatch.await(waitTime, TimeUnit.MILLISECONDS);

        unblockMessagesFromInstance(server, client);

        assertTrue(connectedLatch.await(30, TimeUnit.SECONDS));

        validateRegistrations(clusterSize, registrationId, clientInstanceImpl);

        validateListenerFunctionality(eventCount);

        assertTrue(removeListener(registrationId));
    }

    private void terminateRandomNode() {
        int clusterSize = factory.getAllHazelcastInstances().size();
        HazelcastInstance[] instances = new HazelcastInstance[clusterSize];
        factory.getAllHazelcastInstances().toArray(instances);
        int randNode = new Random().nextInt(clusterSize);
        instances[randNode].getLifecycleService().terminate();
    }

    @Test
    public void testListenersNonSmartRouting()
            throws InterruptedException {
        factory.newHazelcastInstance();

        ClientConfig clientConfig = createClientConfig();
        clientConfig.getNetworkConfig().setSmartRouting(false);
        clientConfig.getNetworkConfig().setConnectionAttemptLimit(Integer.MAX_VALUE);
        client = factory.newHazelcastClient(clientConfig);

        testListenersInternal();
    }

    @Test
    public void testListenersSmartRouting()
            throws InterruptedException {
        factory.newHazelcastInstance();

        ClientConfig clientConfig = createClientConfig();
        client = factory.newHazelcastClient(clientConfig);
        testListenersInternal();
    }

    @Test
    public void testListenersNonSmartRoutingTerminateRandomNode()
            throws InterruptedException {
        factory.newHazelcastInstance();

        ClientConfig clientConfig = createClientConfig();
        clientConfig.getNetworkConfig().setSmartRouting(false);
        clientConfig.getNetworkConfig().setConnectionAttemptLimit(Integer.MAX_VALUE);
        client = factory.newHazelcastClient(clientConfig);

        testListenersTerminateRandomNodeInternal();
    }

    @Test
    public void testListenersSmartRoutingTerminateRandomNode()
            throws InterruptedException {
        factory.newHazelcastInstance();

        ClientConfig clientConfig = createClientConfig();
        client = factory.newHazelcastClient(clientConfig);
        testListenersTerminateRandomNodeInternal();
    }

    @Test
    public void testListenersMemberDestroyEndpointTaskSmartRouting()
            throws InterruptedException {
        factory.newInstances(null, 3);

        ClientConfig clientConfig = createClientConfig();
        clientConfig
                .setProperty(ClientProperty.HEARTBEAT_TIMEOUT.getName(), String.valueOf(2 * ENDPOINT_REMOVE_DELAY_MILLISECONDS));
        client = factory.newHazelcastClient(clientConfig);
        testListenersWaitMemberDestroy();
    }

    @Test
    public void testClusterReconnectDueToHeartbeatSmartRouting()
            throws InterruptedException {
        factory.newHazelcastInstance();

        ClientConfig clientConfig = getSmartClientConfig();
        client = factory.newHazelcastClient(clientConfig);

        testListenersForHeartbeat();
    }

    @Test
    public void testTemporaryBlockedNoDisconnectionSmartRouting()
            throws InterruptedException {
        factory.newHazelcastInstance();

        ClientConfig clientConfig = getSmartClientConfig();
        client = factory.newHazelcastClient(clientConfig);

        testListenersTemporaryNetworkBlockage();
    }

    @Test
    public void testTemporaryBlockedNoDisconnectionNonSmartRouting()
            throws InterruptedException {
        factory.newHazelcastInstance();

        ClientConfig clientConfig = getNonSmartClientConfig();
        client = factory.newHazelcastClient(clientConfig);

        testListenersTemporaryNetworkBlockage();
    }

    @Test
    public void testTemporaryBlockedNoDisconnectionMultipleServerSmartRouting()
            throws InterruptedException {
        factory.newInstances(null, 3);

        ClientConfig clientConfig = getSmartClientConfig();
        client = factory.newHazelcastClient(clientConfig);

        testListenersTemporaryNetworkBlockage();
    }

    @Test
    public void testTemporaryBlockedNoDisconnectionMultipleServerNonSmartRouting()
            throws InterruptedException {
        factory.newInstances(null, 3);

        ClientConfig clientConfig = getNonSmartClientConfig();
        client = factory.newHazelcastClient(clientConfig);

        testListenersTemporaryNetworkBlockage();
    }

    private void testListenersTemporaryNetworkBlockage() {
        HazelcastClientInstanceImpl clientInstanceImpl = getHazelcastClientInstanceImpl(client);

        HazelcastInstance server = getOwnerServer(factory,  clientInstanceImpl);

        final int clusterSize = factory.getAllHazelcastInstances().size();
        assertClusterSizeEventually(clusterSize, client);

        final AtomicInteger eventCount = new AtomicInteger();
        final String registrationId = addListener(eventCount);

        final boolean smartRouting = clientInstanceImpl.getClientConfig().getNetworkConfig().isSmartRouting();
        final int expectedRegistrationsSize = smartRouting ? clusterSize : 1;

        assertEquals(expectedRegistrationsSize, getClientEventRegistrations(client, registrationId).size());

        long timeout = clientInstanceImpl.getProperties().getMillis(HEARTBEAT_TIMEOUT);
        long heartbeatTimeout = timeout > 0 ? timeout : Integer.parseInt(HEARTBEAT_TIMEOUT.getDefaultValue());
        long waitTime = heartbeatTimeout / 2;

        validateListenerFunctionality(eventCount);

        long endTime = System.currentTimeMillis() + waitTime;
        blockMessagesFromInstance(server, client);
        long sleepTime = endTime - System.currentTimeMillis();

        if (sleepTime > 0) {
            sleepMillis((int)sleepTime);
        }

        unblockMessagesFromInstance(server, client);

        assertEquals(expectedRegistrationsSize, getClientEventRegistrations(client, registrationId).size());

        validateListenerFunctionality(eventCount);

        assertTrue(removeListener(registrationId));
    }

    private void validateListenerFunctionality(final AtomicInteger eventCount) {
        eventCount.set(0);
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
        }, 5);

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

    @Test
    public void testClusterReconnectMultipleServersDueToHeartbeatSmartRouting()
            throws InterruptedException {
        factory.newInstances(null, 3);

        ClientConfig clientConfig = getSmartClientConfig();
        client = factory.newHazelcastClient(clientConfig);

        testListenersForHeartbeat();
    }

    @Test
    public void testClusterReconnectDueToHeartbeatNonSmartRouting()
            throws InterruptedException {
        factory.newHazelcastInstance();

        ClientConfig clientConfig = getNonSmartClientConfig();
        client = factory.newHazelcastClient(clientConfig);

        testListenersForHeartbeat();
    }

    private ClientConfig getNonSmartClientConfig() {
        ClientConfig clientConfig = getSmartClientConfig();
        clientConfig.getNetworkConfig().setSmartRouting(false);
        return clientConfig;
    }

    private ClientConfig getSmartClientConfig() {
        ClientConfig clientConfig = createClientConfig();
        clientConfig.getNetworkConfig().setConnectionAttemptLimit(20).setConnectionAttemptPeriod(2000);
        clientConfig.setProperty(ClientProperty.HEARTBEAT_TIMEOUT.getName(), "4000");
        clientConfig.setProperty(ClientProperty.HEARTBEAT_INTERVAL.getName(), "1000");
        return clientConfig;
    }

    @Test
    public void testClusterReconnectMultipleServerDueToHeartbeatNonSmartRouting()
            throws InterruptedException {
        factory.newInstances(null, 3);

        ClientConfig clientConfig = getNonSmartClientConfig();
        client = factory.newHazelcastClient(clientConfig);

        testListenersForHeartbeat();
    }

    @Test
    public void testListenersSmartRoutingMultipleServer()
            throws InterruptedException {
        factory.newInstances(null, 3);

        ClientConfig clientConfig = createClientConfig();
        client = factory.newHazelcastClient(clientConfig);
        testListenersInternal();
    }

    @Test
    public void testListenersNonSmartRoutingMultipleServer()
            throws InterruptedException {
        factory.newInstances(null, 3);

        ClientConfig clientConfig = createClientConfig();
        clientConfig.getNetworkConfig().setSmartRouting(false);
        client = factory.newHazelcastClient(clientConfig);
        testListenersInternal();
    }

    private ClientConfig createClientConfig() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setRedoOperation(true);
        clientConfig.getNetworkConfig().setConnectionAttemptLimit(Integer.MAX_VALUE);
        return clientConfig;
    }

    private Collection<ClientEventRegistration> getClientEventRegistrations(HazelcastInstance client, String id) {
        HazelcastClientInstanceImpl clientImpl = ClientTestUtil.getHazelcastClientInstanceImpl(client);
        ClientListenerServiceImpl listenerService = (ClientListenerServiceImpl) clientImpl.getListenerService();
        return listenerService.getActiveRegistrations(id);
    }

    protected abstract String addListener(final AtomicInteger eventCount);

    protected abstract void produceEvent();

    protected abstract boolean removeListener(String registrationId);
}
