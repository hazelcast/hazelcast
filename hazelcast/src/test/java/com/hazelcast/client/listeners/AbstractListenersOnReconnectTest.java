/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.impl.clientside.ClientTestUtil;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.connection.ClientConnection;
import com.hazelcast.client.impl.connection.tcp.RoutingMode;
import com.hazelcast.client.impl.spi.impl.listener.ClientConnectionRegistration;
import com.hazelcast.client.impl.spi.impl.listener.ClientListenerServiceImpl;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.client.util.ConfigRoutingUtil;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.cluster.MembershipListener;
import com.hazelcast.config.Config;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.eventservice.impl.EventServiceImpl;
import com.hazelcast.spi.impl.eventservice.impl.EventServiceSegment;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.client.properties.ClientProperty.HEARTBEAT_INTERVAL;
import static com.hazelcast.client.properties.ClientProperty.HEARTBEAT_TIMEOUT;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public abstract class AbstractListenersOnReconnectTest extends ClientTestSupport {
    @Parameterized.Parameter
    public RoutingMode routingMode;

    private static final int EVENT_COUNT = 10;
    private final AtomicInteger eventCount = new AtomicInteger();
    private final TestHazelcastFactory factory = new TestHazelcastFactory();
    private CountDownLatch eventsLatch = new CountDownLatch(1);
    private final Set<String> events = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private UUID registrationId;
    protected ClientConfig clientConfig;
    protected HazelcastInstance client;

    @Before
    public void setUp() throws Exception {
        clientConfig = createClientConfig();
    }

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    //-------------------------- testListenersWhenClientIsGone --------------------- //

    @Test
    public void testListenersWhenClientIsGone() {
        var instances = factory.newInstances(HazelcastTestSupport::smallInstanceConfigWithoutJetAndMetrics, 2);
        client = factory.newHazelcastClient(clientConfig);

        setupListener(instances.length);

        client.shutdown();

        validateRegistrationsOnMembers(factory, 0);
    }

    //-------------------------- testListenersTerminateRandomNode --------------------- //
    @Test
    public void testListenersTerminateRandomNode() {
        var instances = factory.newInstances(HazelcastTestSupport::smallInstanceConfigWithoutJetAndMetrics, 3);
        client = factory.newHazelcastClient(clientConfig);

        setupListener(instances.length);
        int idx = terminateRandomNode(instances);

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

        instances[idx] = factory.newHazelcastInstance(smallInstanceConfigWithoutJetAndMetrics());

        assertOpenEventually(memberAddedLatch);
        validateRegistrationsAndListenerFunctionality(instances.length);
    }

    //-------------------------- testListenersTemporaryNetworkBlockage --------------------- //

    @Test
    public void testListenersTemporaryNetworkBlockage_when_singleServer() {
        testListenersTemporaryNetworkBlockage(1);
    }

    @Test
    public void testListenersTemporaryNetworkBlockage_when_multipleServer() {
        testListenersTemporaryNetworkBlockage(3);
    }

    private void testListenersTemporaryNetworkBlockage(int clusterSize) {
        var instances = factory.newInstances(HazelcastTestSupport::smallInstanceConfigWithoutJetAndMetrics, clusterSize);

        client = factory.newHazelcastClient(clientConfig);

        setupListener(instances.length);

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

        validateRegistrationsAndListenerFunctionality(clusterSize);
    }

    //-------------------------- testListenersHeartbeatTimeoutToCluster --------------------- //

    @Test
    public void testListenersHeartbeatTimeoutToCluster_when_single_server() {
        testListenersHeartbeatTimeoutToCluster(1);
    }

    @Test
    public void testListenersHeartbeatTimeoutToCluster_when_multiple_server() {
        testListenersHeartbeatTimeoutToCluster(3);
    }

    private void testListenersHeartbeatTimeoutToCluster(int nodeCount) {
        var instances = factory.newInstances(() -> {
            Config config = smallInstanceConfig();
            config.getJetConfig().setEnabled(false);
            return config;
        }, nodeCount);

        ClientConfig clientConfig = createClientConfig();

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

        setupListener(instances.length);

        for (HazelcastInstance instance : factory.getAllHazelcastInstances()) {
            blockMessagesFromInstance(instance, client);
        }

        assertOpenEventually(disconnectedLatch);

        for (HazelcastInstance instance : factory.getAllHazelcastInstances()) {
            unblockMessagesFromInstance(instance, client);
        }

        assertOpenEventually(reconnectedLatch);
        validateRegistrationsAndListenerFunctionality(instances.length);
    }


    //-------------------------- testListenersTerminateCluster --------------------- //
    @Test
    public void testListenersTerminateCluster_when_singleServer() {
        testListenersTerminateCluster(1);
    }

    @Test
    public void testListenersTerminateCluster_when_multipleServer() {
        testListenersTerminateCluster(3);
    }

    private void testListenersTerminateCluster(int clusterSize) {
        var instances = factory.newInstances(HazelcastTestSupport::smallInstanceConfigWithoutJetAndMetrics, clusterSize);

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
        setupListener(instances.length);

        validateRegistrationsOnMembers(factory, 1);

        for (HazelcastInstance instance : factory.getAllHazelcastInstances()) {
            instance.getLifecycleService().terminate();
        }

        instances = factory.newInstances(HazelcastTestSupport::smallInstanceConfigWithoutJetAndMetrics, clusterSize);
        assertClusterSizeEventually(clusterSize, client);

        assertOpenEventually(disconnectedLatch);
        assertOpenEventually(reconnectedLatch);

        validateRegistrationsAndListenerFunctionality(clusterSize);
    }

    //-------------------------- utility and validation methods --------------------- //

    private void setupListener(int clusterSize) {
        assertClusterSizeEventually(clusterSize, client);
        registrationId = addListener();
    }

    private void validateRegistrationsAndListenerFunctionality(int clusterSize) {
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
                assertEquals(member + " Current registrations:" + registrationIdMap, expected,
                        registrationIdMap.size());
                ILogger logger = nodeEngineImpl.getLogger(AbstractListenersOnReconnectTest.class);
                logger.warning("Current registrations at member " + member + ": " + registrationIdMap);
            }
        });
    }

    abstract String getServiceName();

    private void validateRegistrations(final int clusterSize, final UUID registrationId,
                                       final HazelcastClientInstanceImpl clientInstanceImpl) {
        final boolean allMembersRouting = clientInstanceImpl.getClientConfig().getNetworkConfig()
                .getClusterRoutingConfig().getRoutingMode() == RoutingMode.ALL_MEMBERS;

        assertTrueEventually(() -> {
            int size = allMembersRouting ? clusterSize : 1;
            Map<ClientConnection, ClientConnectionRegistration> registrations = getClientEventRegistrations(client,
                    registrationId);
            assertEquals(size, registrations.size());
            if (allMembersRouting) {
                Collection<Member> members = clientInstanceImpl.getClientClusterService().getMemberList();
                for (ClientConnection registeredSubscriber : registrations.keySet()) {
                    boolean contains = false;
                    for (Member member : members) {
                        contains |= registeredSubscriber.getRemoteAddress().equals(member.getAddress());
                    }
                    assertTrue("Registered member " + registeredSubscriber + " is not in the cluster member list " + members,
                            contains);
                }
            } else {
                Iterator<ClientConnection> expectedIterator = registrations.keySet().iterator();
                assertTrue(expectedIterator.hasNext());
                Iterator<ClientConnection> iterator = clientInstanceImpl.getConnectionManager().getActiveConnections().iterator();
                assertTrue(iterator.hasNext());
                assertEquals(iterator.next(), expectedIterator.next());
            }
        });
    }

    private void validateListenerFunctionality() {
        eventCount.set(0);
        eventsLatch = new CountDownLatch(1);
        for (int i = 0; i < EVENT_COUNT; i++) {
            events.add(i + randomString());
        }

        for (String event : events) {
            produceEvent(event);
        }

        assertOpenEventually(eventsLatch);

        assertTrueAllTheTime(() -> {
            int count = eventCount.get();
            assertEquals("Received event count is " + count
                    + " but it is expected to stay at " + EVENT_COUNT, EVENT_COUNT, count);
        }, 1);
    }

    private int terminateRandomNode(HazelcastInstance[] instances) {
        int nodeIndex = ThreadLocalRandom.current().nextInt(instances.length);
        instances[nodeIndex].getLifecycleService().terminate();
        return nodeIndex;
    }

    private Map<ClientConnection, ClientConnectionRegistration> getClientEventRegistrations(HazelcastInstance client, UUID id) {
        HazelcastClientInstanceImpl clientImpl = ClientTestUtil.getHazelcastClientInstanceImpl(client);
        ClientListenerServiceImpl listenerService = (ClientListenerServiceImpl) clientImpl.getListenerService();
        return listenerService.getActiveRegistrations(id);
    }

    private ClientConfig createClientConfig() {
        ClientConfig clientConfig = ConfigRoutingUtil.newClientConfig(routingMode);
        clientConfig.setProperty(HEARTBEAT_TIMEOUT.getName(), String.valueOf(TimeUnit.SECONDS.toMillis(20)));
        clientConfig.setProperty(HEARTBEAT_INTERVAL.getName(), String.valueOf(TimeUnit.SECONDS.toMillis(1)));

        clientConfig.getConnectionStrategyConfig()
                .getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);

        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
        networkConfig.setRedoOperation(true);

        return clientConfig;
    }

    protected abstract UUID addListener();

    protected abstract void produceEvent(String event);

    void onEvent(String event) {
        eventCount.incrementAndGet();
        events.remove(event);
        if (events.isEmpty()) {
            eventsLatch.countDown();
        }
    }

    protected abstract boolean removeListener(UUID registrationId);
}
