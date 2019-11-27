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

package com.hazelcast.client.listeners;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.clientside.ClientTestUtil;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.spi.impl.listener.AbstractClientListenerService;
import com.hazelcast.client.spi.impl.listener.ClientEventRegistration;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.eventservice.impl.EventServiceImpl;
import com.hazelcast.spi.impl.eventservice.impl.EventServiceSegment;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.client.spi.properties.ClientProperty.HEARTBEAT_INTERVAL;
import static com.hazelcast.client.spi.properties.ClientProperty.HEARTBEAT_TIMEOUT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public abstract class AbstractListenersOnReconnectTest extends ClientTestSupport {

    private static final int EVENT_COUNT = 10;
    private final AtomicInteger eventCount = new AtomicInteger();
    private final TestHazelcastFactory factory = new TestHazelcastFactory();
    private CountDownLatch eventsLatch = new CountDownLatch(1);
    private final Set<String> events = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
    private String registrationId;
    private int clusterSize;
    protected HazelcastInstance client;

    @After
    public void tearDown() {
        factory.terminateAll();
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
    public void testListenersWaitMemberDestroySmartRouting() {
        Config config = new Config();
        int endpointDelaySeconds = 2;
        config.setProperty(GroupProperty.CLIENT_ENDPOINT_REMOVE_DELAY_SECONDS.getName(), String.valueOf(endpointDelaySeconds));
        factory.newInstances(config, 3);
        ClientConfig clientConfig = createClientConfig(true);
        ListenerConfig listenerConfig = new ListenerConfig();
        final CountDownLatch disconnectedLatch = new CountDownLatch(1);
        final AtomicInteger connectCount = new AtomicInteger();
        final CountDownLatch connectedLatch = new CountDownLatch(1);
        final CountDownLatch reconnectedLatch = new CountDownLatch(1);
        listenerConfig.setImplementation(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
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
            }
        });
        clientConfig.addListenerConfig(listenerConfig);
        client = factory.newHazelcastClient(clientConfig);
        assertOpenEventually(connectedLatch);

        setupListener();

        Collection<HazelcastInstance> allHazelcastInstances = factory.getAllHazelcastInstances();

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
        assertOpenEventually(reconnectedLatch);

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
        ClientConfig clientConfig = createClientConfig(true);
        ListenerConfig listenerConfig = new ListenerConfig();
        final CountDownLatch disconnectedLatch = new CountDownLatch(1);
        final AtomicInteger connectCount = new AtomicInteger();
        final CountDownLatch connectedLatch = new CountDownLatch(1);
        final CountDownLatch reconnectedLatch = new CountDownLatch(1);
        listenerConfig.setImplementation(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
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
            }
        });
        clientConfig.addListenerConfig(listenerConfig);
        client = factory.newHazelcastClient(clientConfig);
        assertOpenEventually(connectedLatch);
        HazelcastInstance server2 = factory.newHazelcastInstance(config);

        setupListener();

        blockMessagesToInstance(server2, client);

        ownerServer.shutdown();

        sleepAtLeastMillis(TimeUnit.SECONDS.toMillis(endpointDelaySeconds) * 2);

        unblockMessagesToInstance(server2, client);

        assertOpenEventually(disconnectedLatch);
        assertOpenEventually(reconnectedLatch);

        clusterSize = clusterSize - 1;
        validateRegistrationsAndListenerFunctionality();
    }

    @Test
    @Category(SlowTest.class)
    public void testListenersWhenClientDisconnectedOperationRuns_whenOwnerConnectionRemoved() {
        Config config = new Config();
        int endpointDelaySeconds = 10;
        config.setProperty(GroupProperty.CLIENT_ENDPOINT_REMOVE_DELAY_SECONDS.getName(), String.valueOf(endpointDelaySeconds));
        config.setProperty(GroupProperty.CLIENT_HEARTBEAT_TIMEOUT_SECONDS.getName(), "20");
        HazelcastInstance ownerServer = factory.newHazelcastInstance(config);
        ClientConfig clientConfig = createClientConfig(true);
        ListenerConfig listenerConfig = new ListenerConfig();
        final CountDownLatch disconnectedLatch = new CountDownLatch(1);
        final AtomicInteger connectCount = new AtomicInteger();
        final CountDownLatch connectedLatch = new CountDownLatch(1);
        final CountDownLatch reconnectedLatch = new CountDownLatch(1);
        listenerConfig.setImplementation(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
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
            }
        });
        clientConfig.addListenerConfig(listenerConfig);
        client = factory.newHazelcastClient(clientConfig);
        factory.newHazelcastInstance(config);
        assertOpenEventually(connectedLatch);
        setupListener();

        blockMessagesToInstance(ownerServer, client);

        assertOpenEventually(disconnectedLatch);
        sleepAtLeastMillis(TimeUnit.SECONDS.toMillis(endpointDelaySeconds) * 2);

        unblockMessagesToInstance(ownerServer, client);

        assertOpenEventually(reconnectedLatch);

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
        HazelcastInstance server = getOwnerServer(factory, clientInstanceImpl);

        long timeout = clientInstanceImpl.getProperties().getMillis(HEARTBEAT_TIMEOUT);
        long waitTime = timeout / 2;

        blockMessagesFromInstance(server, client);

        sleepMillis((int) waitTime);

        unblockMessagesFromInstance(server, client);

        validateRegistrationsAndListenerFunctionality();
    }

    //-------------------------- testListenersHeartbeatTimeoutToOwner --------------------- //

    @Test
    public void testClusterReconnectDueToHeartbeatSmartRouting() {
        testListenersHeartbeatTimeoutToOwner(true, 1);
    }

    @Test
    public void testClusterReconnectMultipleServersDueToHeartbeatSmartRouting() {
        testListenersHeartbeatTimeoutToOwner(true, 3);
    }

    @Test
    public void testClusterReconnectDueToHeartbeatNonSmartRouting() {
        testListenersHeartbeatTimeoutToOwner(false, 1);
    }

    @Test
    public void testClusterReconnectMultipleServerDueToHeartbeatNonSmartRouting() {
        testListenersHeartbeatTimeoutToOwner(false, 3);
    }

    private void testListenersHeartbeatTimeoutToOwner(boolean isSmart, int clusterSize) {
        factory.newInstances(null, clusterSize);

        ClientConfig clientConfig = createClientConfig(isSmart);
        ListenerConfig listenerConfig = new ListenerConfig();
        final CountDownLatch disconnectedLatch = new CountDownLatch(1);
        final AtomicInteger connectCount = new AtomicInteger();
        final CountDownLatch connectedLatch = new CountDownLatch(1);
        final CountDownLatch reconnectedLatch = new CountDownLatch(1);
        listenerConfig.setImplementation(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
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
            }
        });
        clientConfig.addListenerConfig(listenerConfig);
        client = factory.newHazelcastClient(clientConfig);
        assertOpenEventually(connectedLatch);

        setupListener();

        HazelcastClientInstanceImpl clientInstanceImpl = getHazelcastClientInstanceImpl(client);
        HazelcastInstance server = getOwnerServer(factory, clientInstanceImpl);

        blockMessagesFromInstance(server, client);
        assertOpenEventually(disconnectedLatch);

        unblockMessagesFromInstance(server, client);
        assertOpenEventually(reconnectedLatch);

        validateRegistrationsAndListenerFunctionality();
    }


    //-------------------------- testListenersTerminateOwnerNode --------------------- //

    @Test
    public void testListenersSmartRoutingMultipleServer() {
        testListenersTerminateOwnerNode(true, 3);
    }

    @Test
    public void testListenersNonSmartRoutingMultipleServer() {
        testListenersTerminateOwnerNode(false, 3);
    }

    @Test
    public void testListenersSmartRouting() {
        testListenersTerminateOwnerNode(true, 1);
    }

    @Test
    public void testListenersNonSmartRouting() {
        testListenersTerminateOwnerNode(false, 1);
    }

    private void testListenersTerminateOwnerNode(boolean isSmartClient, int clusterSize) {
        factory.newInstances(null, clusterSize);

        ClientConfig clientConfig = createClientConfig(isSmartClient);
        ListenerConfig listenerConfig = new ListenerConfig();
        final CountDownLatch disconnectedLatch = new CountDownLatch(1);
        final AtomicInteger connectCount = new AtomicInteger();
        final CountDownLatch connectedLatch = new CountDownLatch(1);
        final CountDownLatch reconnectedLatch = new CountDownLatch(1);
        listenerConfig.setImplementation(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
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
            }
        });
        clientConfig.addListenerConfig(listenerConfig);
        client = factory.newHazelcastClient(clientConfig);
        assertOpenEventually(connectedLatch);

        setupListener();

        validateRegistrationsOnMembers(factory);

        HazelcastClientInstanceImpl clientInstanceImpl = getHazelcastClientInstanceImpl(client);
        HazelcastInstance server = getOwnerServer(factory, clientInstanceImpl);
        server.getLifecycleService().terminate();

        factory.newHazelcastInstance();
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
        validateRegistrationsOnMembers(factory);
        validateRegistrations(clusterSize, registrationId, getHazelcastClientInstanceImpl(client));
        validateListenerFunctionality();
        assertTrue(removeListener(registrationId));
    }

    protected void validateRegistrationsOnMembers(final TestHazelcastFactory factory) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (HazelcastInstance instance : factory.getAllHazelcastInstances()) {
                    NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(instance);
                    EventServiceImpl eventService = (EventServiceImpl) nodeEngineImpl.getEventService();
                    EventServiceSegment serviceSegment = eventService.getSegment(getServiceName(), false);
                    Member member = instance.getCluster().getLocalMember();
                    assertNotNull(member.toString(), serviceSegment);
                    ConcurrentMap registrationIdMap = serviceSegment.getRegistrationIdMap();
                    assertEquals(member.toString() + " Current registrations:" + registrationIdMap, 1,
                            registrationIdMap.size());
                    ILogger logger = nodeEngineImpl.getLogger(AbstractListenersOnReconnectTest.class);
                    logger.warning("Current registrations at member " + member.toString() + ": " + registrationIdMap);
                }
            }
        });
    }

    abstract String getServiceName();

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
                    Iterator<ClientEventRegistration> expectedIterator = registrations.iterator();
                    assertTrue(expectedIterator.hasNext());
                    ClientEventRegistration registration = registrations.iterator().next();
                    assertEquals(clientInstanceImpl.getConnectionManager().getOwnerConnectionAddress(),
                            registration.getSubscriber().getEndPoint());
                }
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
        AbstractClientListenerService listenerService = (AbstractClientListenerService) clientImpl.getListenerService();
        return listenerService.getActiveRegistrations(id);
    }

    private ClientConfig createClientConfig(boolean isSmart) {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setConnectionAttemptLimit(Integer.MAX_VALUE);
        clientConfig.getNetworkConfig().setSmartRouting(isSmart);
        clientConfig.getNetworkConfig().setRedoOperation(true);
        clientConfig.setProperty(HEARTBEAT_TIMEOUT.getName(), String.valueOf(TimeUnit.SECONDS.toMillis(20)));
        clientConfig.setProperty(HEARTBEAT_INTERVAL.getName(), String.valueOf(TimeUnit.SECONDS.toMillis(1)));
        return clientConfig;
    }

    protected abstract String addListener();

    protected abstract void produceEvent(String event);

    void onEvent(String event) {
        events.remove(event);
        eventCount.incrementAndGet();
        if (events.isEmpty()) {
            eventsLatch.countDown();
        }
    }

    protected abstract boolean removeListener(String registrationId);
}
