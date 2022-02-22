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

package com.hazelcast.client.listeners;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.clientside.ClientTestUtil;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.spi.impl.listener.ClientConnectionRegistration;
import com.hazelcast.client.impl.spi.impl.listener.ClientListenerServiceImpl;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.cluster.MembershipListener;
import com.hazelcast.config.Config;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.eventservice.impl.EventServiceImpl;
import com.hazelcast.spi.impl.eventservice.impl.EventServiceSegment;
import org.junit.After;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.client.properties.ClientProperty.HEARTBEAT_INTERVAL;
import static com.hazelcast.client.properties.ClientProperty.HEARTBEAT_TIMEOUT;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public abstract class AbstractListenersOnReconnectTest extends ClientTestSupport {

    private static final int EVENT_COUNT = 10;
    private final AtomicInteger eventCount = new AtomicInteger();
    private final TestHazelcastFactory factory = new TestHazelcastFactory();
    private CountDownLatch eventsLatch = new CountDownLatch(1);
    private final Set<String> events = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
    private UUID registrationId;
    private int clusterSize;
    protected HazelcastInstance client;

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    //-------------------------- testListenersWhenClientIsGone --------------------- //

    @Test
    public void testListenersWhenClientIsGone_smart() {
        testListenersWhenClientIsGone(true);
    }

    @Test
    public void testListenersWhenClientIsGone_unisocket() {
        testListenersWhenClientIsGone(false);
    }

    private void testListenersWhenClientIsGone(boolean isSmartClient) {
        factory.newInstances(null, 2);
        ClientConfig clientConfig = createClientConfig(isSmartClient);
        client = factory.newHazelcastClient(clientConfig);

        setupListener();

        client.shutdown();

        validateRegistrationsOnMembers(factory, 0);
    }

    //-------------------------- testListenersTerminateRandomNode --------------------- //
    @Test
    public void testListenersTerminateRandomNode_smart() {
        testListenersTerminateRandomNode(true);
    }

    @Test
    public void testListenersTerminateRandomNode_nonSmart() {
        testListenersTerminateRandomNode(false);
    }

    private void testListenersTerminateRandomNode(boolean isSmartClient) {
        factory.newInstances(null, 3);
        ClientConfig clientConfig = createClientConfig(isSmartClient);
        client = factory.newHazelcastClient(clientConfig);

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

        });

        factory.newHazelcastInstance();

        assertOpenEventually(memberAddedLatch);
        validateRegistrationsAndListenerFunctionality();
    }

    //-------------------------- testListenersTemporaryNetworkBlockage --------------------- //

    @Test
    public void testListenersTemporaryNetworkBlockage_smart_singleServer() {
        testListenersTemporaryNetworkBlockage(true, 1);
    }

    @Test
    public void testListenersTemporaryNetworkBlockage_nonSmart_singleServer() {
        testListenersTemporaryNetworkBlockage(false, 1);
    }

    @Test
    public void testListenersTemporaryNetworkBlockage_smart_multipleServer() {
        testListenersTemporaryNetworkBlockage(true, 3);
    }

    @Test
    public void testListenersTemporaryNetworkBlockage_nonSmart_multipleServer() {
        testListenersTemporaryNetworkBlockage(false, 3);
    }

    private void testListenersTemporaryNetworkBlockage(boolean isSmart, int clusterSize) {
        factory.newInstances(null, clusterSize);

        ClientConfig clientConfig = createClientConfig(isSmart);
        client = factory.newHazelcastClient(clientConfig);

        setupListener();

        HazelcastClientInstanceImpl clientInstanceImpl = getHazelcastClientInstanceImpl(client);


        long timeout = clientInstanceImpl.getProperties().getMillis(HEARTBEAT_TIMEOUT);
        long waitTime = timeout / 2;

        for (HazelcastInstance instance : factory.getAllHazelcastInstances()) {
            blockMessagesFromInstance(instance, client);
        }

        sleepMillis((int) waitTime);

        for (HazelcastInstance instance : factory.getAllHazelcastInstances()) {
            unblockMessagesFromInstance(instance, client);
        }

        validateRegistrationsAndListenerFunctionality();
    }

    //-------------------------- testListenersHeartbeatTimeoutToCluster --------------------- //

    @Test
    public void testListenersHeartbeatTimeoutToCluster_smart_singleServer() {
        testListenersHeartbeatTimeoutToCluster(true, 1);
    }

    @Test
    public void testListenersHeartbeatTimeoutToCluster_nonSmart_singleServer() {
        testListenersHeartbeatTimeoutToCluster(false, 1);
    }

    @Test
    public void testListenersHeartbeatTimeoutToCluster_smart_multipleServer() {
        testListenersHeartbeatTimeoutToCluster(true, 3);
    }

    @Test
    public void testListenersHeartbeatTimeoutToCluster_nonSmart_multipleServer() {
        testListenersHeartbeatTimeoutToCluster(false, 3);
    }

    private void testListenersHeartbeatTimeoutToCluster(boolean isSmartClient, int nodeCount) {
        factory.newInstances(null, nodeCount);
        ClientConfig clientConfig = createClientConfig(isSmartClient);
        ListenerConfig listenerConfig = new ListenerConfig();
        AtomicInteger connectCount = new AtomicInteger();
        CountDownLatch disconnectedLatch = new CountDownLatch(1);
        CountDownLatch connectedLatch = new CountDownLatch(1);
        CountDownLatch reconnectedLatch = new CountDownLatch(1);
        listenerConfig.setImplementation((LifecycleListener) event -> {
            if (LifecycleEvent.LifecycleState.CLIENT_DISCONNECTED == event.getState()) {
                disconnectedLatch.countDown();
            }
            if (LifecycleEvent.LifecycleState.CLIENT_CONNECTED == event.getState()) {
                int count = connectCount.incrementAndGet();
                if (count == 1) {
                    connectedLatch.countDown();
                } else if (count == 2) {
                    reconnectedLatch.countDown();
                }
            }
        });
        clientConfig.addListenerConfig(listenerConfig);
        client = factory.newHazelcastClient(clientConfig);
        assertOpenEventually(connectedLatch);

        setupListener();

        for (HazelcastInstance instance : factory.getAllHazelcastInstances()) {
            blockMessagesFromInstance(instance, client);
        }

        assertOpenEventually(disconnectedLatch);

        for (HazelcastInstance instance : factory.getAllHazelcastInstances()) {
            unblockMessagesFromInstance(instance, client);
        }

        assertOpenEventually(reconnectedLatch);
        validateRegistrationsAndListenerFunctionality();
    }


    //-------------------------- testListenersTerminateCluster --------------------- //

    @Test
    public void testListenersTerminateCluster_smart_singleServer() {
        testListenersTerminateCluster(true, 1);
    }

    @Test
    public void testListenersTerminateCluster_nonSmart_singleServer() {
        testListenersTerminateCluster(false, 1);
    }

    @Test
    public void testListenersTerminateCluster_smart_multipleServer() {
        testListenersTerminateCluster(true, 3);
    }

    @Test
    public void testListenersTerminateCluster_nonSmart_multipleServer() {
        testListenersTerminateCluster(false, 3);
    }

    private void testListenersTerminateCluster(boolean isSmartClient, int clusterSize) {
        factory.newInstances(null, clusterSize);

        ClientConfig clientConfig = createClientConfig(isSmartClient);
        ListenerConfig listenerConfig = new ListenerConfig();
        CountDownLatch disconnectedLatch = new CountDownLatch(1);
        AtomicInteger connectCount = new AtomicInteger();
        CountDownLatch connectedLatch = new CountDownLatch(1);
        CountDownLatch reconnectedLatch = new CountDownLatch(1);
        listenerConfig.setImplementation((LifecycleListener) event -> {
            if (LifecycleEvent.LifecycleState.CLIENT_DISCONNECTED == event.getState()) {
                disconnectedLatch.countDown();
            }
            if (LifecycleEvent.LifecycleState.CLIENT_CONNECTED == event.getState()) {
                int count = connectCount.incrementAndGet();
                if (count == 1) {
                    connectedLatch.countDown();
                } else if (count == 2) {
                    reconnectedLatch.countDown();
                }
            }
        });
        clientConfig.addListenerConfig(listenerConfig);
        client = factory.newHazelcastClient(clientConfig);

        assertOpenEventually(connectedLatch);
        setupListener();

        validateRegistrationsOnMembers(factory, 1);

        for (HazelcastInstance instance : factory.getAllHazelcastInstances()) {
            instance.getLifecycleService().terminate();
        }

        factory.newInstances(new Config(), clusterSize);
        assertClusterSizeEventually(clusterSize, client);

        assertOpenEventually(disconnectedLatch);
        assertOpenEventually(reconnectedLatch);

        validateRegistrationsAndListenerFunctionality();
    }

    //-------------------------- utility and validation methods --------------------- //

    private void setupListener() {
        clusterSize = factory.getAllHazelcastInstances().size();
        assertClusterSizeEventually(clusterSize, client);
        registrationId = addListener();
    }

    private void validateRegistrationsAndListenerFunctionality() {
        assertClusterSizeEventually(clusterSize, client);
        validateRegistrationsOnMembers(factory, 1);
        validateRegistrations(clusterSize, registrationId, getHazelcastClientInstanceImpl(client));
        validateListenerFunctionality();
        assertTrue(removeListener(registrationId));
    }

    protected void validateRegistrationsOnMembers(final TestHazelcastFactory factory, int expected) {
        assertTrueEventually(() -> {
            for (HazelcastInstance instance : factory.getAllHazelcastInstances()) {
                NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(instance);
                EventServiceImpl eventService = (EventServiceImpl) nodeEngineImpl.getEventService();
                EventServiceSegment serviceSegment = eventService.getSegment(getServiceName(), false);
                Member member = instance.getCluster().getLocalMember();
                assertNotNull(member.toString(), serviceSegment);
                ConcurrentMap registrationIdMap = serviceSegment.getRegistrationIdMap();
                assertEquals(member.toString() + " Current registrations:" + registrationIdMap, expected,
                        registrationIdMap.size());
                ILogger logger = nodeEngineImpl.getLogger(AbstractListenersOnReconnectTest.class);
                logger.warning("Current registrations at member " + member.toString() + ": " + registrationIdMap);
            }
        });
    }

    abstract String getServiceName();

    private void validateRegistrations(final int clusterSize, final UUID registrationId,
                                       final HazelcastClientInstanceImpl clientInstanceImpl) {
        final boolean smartRouting = clientInstanceImpl.getClientConfig().getNetworkConfig().isSmartRouting();

        assertTrueEventually(() -> {
            int size = smartRouting ? clusterSize : 1;
            Map<Connection, ClientConnectionRegistration> registrations = getClientEventRegistrations(client,
                    registrationId);
            assertEquals(size, registrations.size());
            if (smartRouting) {
                Collection<Member> members = clientInstanceImpl.getClientClusterService().getMemberList();
                for (Connection registeredSubscriber : registrations.keySet()) {
                    boolean contains = false;
                    for (Member member : members) {
                        contains |= registeredSubscriber.getRemoteAddress().equals(member.getAddress());
                    }
                    assertTrue("Registered member " + registeredSubscriber + " is not in the cluster member list " + members,
                            contains);
                }
            } else {
                Iterator<Connection> expectedIterator = registrations.keySet().iterator();
                assertTrue(expectedIterator.hasNext());
                Iterator<Connection> iterator = clientInstanceImpl.getConnectionManager().getActiveConnections().iterator();
                assertTrue(iterator.hasNext());
                assertEquals(iterator.next(), expectedIterator.next());
            }
        });
    }

    private void validateListenerFunctionality() {
        eventCount.set(0);
        eventsLatch = new CountDownLatch(1);
        for (int i = 0; i < EVENT_COUNT; i++) {
            events.add(randomString());
        }

        for (String event : events) {
            produceEvent(event);
        }

        assertOpenEventually(eventsLatch);

        assertTrueAllTheTime(() -> {
            int count = eventCount.get();
            assertEquals("Received event count is " + count
                    + " but it is expected to stay at " + EVENT_COUNT, EVENT_COUNT, count);
        }, 3);
    }

    private void terminateRandomNode() {
        int clusterSize = factory.getAllHazelcastInstances().size();
        HazelcastInstance[] instances = new HazelcastInstance[clusterSize];
        factory.getAllHazelcastInstances().toArray(instances);
        int randNode = new Random().nextInt(clusterSize);
        instances[randNode].getLifecycleService().terminate();
    }

    private Map<Connection, ClientConnectionRegistration> getClientEventRegistrations(HazelcastInstance client, UUID id) {
        HazelcastClientInstanceImpl clientImpl = ClientTestUtil.getHazelcastClientInstanceImpl(client);
        ClientListenerServiceImpl listenerService = (ClientListenerServiceImpl) clientImpl.getListenerService();
        return listenerService.getActiveRegistrations(id);
    }

    private ClientConfig createClientConfig(boolean isSmart) {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        clientConfig.getNetworkConfig().setSmartRouting(isSmart);
        clientConfig.getNetworkConfig().setRedoOperation(true);
        clientConfig.setProperty(HEARTBEAT_TIMEOUT.getName(), String.valueOf(TimeUnit.SECONDS.toMillis(20)));
        clientConfig.setProperty(HEARTBEAT_INTERVAL.getName(), String.valueOf(TimeUnit.SECONDS.toMillis(1)));
        return clientConfig;
    }

    protected abstract UUID addListener();

    protected abstract void produceEvent(String event);

    void onEvent(String event) {
        events.remove(event);
        eventCount.incrementAndGet();
        if (events.isEmpty()) {
            eventsLatch.countDown();
        }
    }

    protected abstract boolean removeListener(UUID registrationId);
}
