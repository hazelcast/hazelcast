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

package com.hazelcast.client.connectionstrategy;

import com.hazelcast.client.HazelcastClientNotActiveException;
import com.hazelcast.client.HazelcastClientOfflineException;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.connection.ClientConnectionStrategy;
import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.client.config.ClientConnectionStrategyConfig.ReconnectMode.ASYNC;
import static com.hazelcast.client.config.ClientConnectionStrategyConfig.ReconnectMode.OFF;
import static com.hazelcast.core.LifecycleEvent.LifecycleState.CLIENT_CONNECTED;
import static com.hazelcast.core.LifecycleEvent.LifecycleState.CLIENT_DISCONNECTED;
import static com.hazelcast.core.LifecycleEvent.LifecycleState.STARTING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ConfiguredBehaviourTest
        extends ClientTestSupport {
    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    private class StateTrackingListener implements LifecycleListener {
        private volatile LifecycleEvent.LifecycleState latestState = STARTING;

        @Override
        public void stateChanged(LifecycleEvent event) {
            latestState = STARTING;
        }

        LifecycleEvent.LifecycleState getLatestState() {
            return latestState;
        }
    }

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void testDefaultStartMode() {
        hazelcastFactory.newHazelcastInstance();

        HazelcastInstance client = hazelcastFactory.newHazelcastClient();

        // Since default start mode is synch mode, this should not throw exception
        client.getMap(randomMapName());
    }

    @Test(expected = HazelcastClientOfflineException.class)
    public void testAsyncStartTrueNoCluster() {

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().setAsyncStart(true);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        client.getMap(randomMapName());
    }

    public void testAsyncStartTrue() {
        final CountDownLatch connectedLatch = new CountDownLatch(1);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.addListenerConfig(new ListenerConfig(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                if (event.getState().equals(CLIENT_CONNECTED)) {
                    connectedLatch.countDown();
                }
            }
        }));
        clientConfig.getConnectionStrategyConfig().setClientStartAsync(true);

        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        assertTrue(client.getLifecycleService().isRunning());

        hazelcastFactory.newHazelcastInstance();

        assertOpenEventually(connectedLatch);

        client.getMap(randomMapName());
    }

    @Test(expected = HazelcastClientNotActiveException.class)
    public void testReconnectModeOFFSingleMember() {
        HazelcastInstance hazelcastInstance = hazelcastFactory.newHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().setReconnectMode(OFF);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        // no exception at this point
        IMap<Integer, Integer> map = client.getMap(randomMapName());
        map.put(1, 5);

        hazelcastInstance.shutdown();

        map.put(1, 5);
    }

    @Test(expected = HazelcastClientNotActiveException.class)
    public void testReconnectModeOFFTwoMembers() {
        hazelcastFactory.newInstances(getConfig(), 2);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().setReconnectMode(OFF);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        // no exception at this point
        IMap<Integer, Integer> map = client.getMap(randomMapName());
        map.put(1, 5);

        HazelcastClientInstanceImpl clientInstanceImpl = getHazelcastClientInstanceImpl(client);
        HazelcastInstance ownerServer = getOwnerServer(hazelcastFactory, clientInstanceImpl);
        ownerServer.shutdown();

        map.put(1, 5);
    }

    @Test
    public void testReconnectModeONSingleMember() {
        HazelcastInstance hazelcastInstance = hazelcastFactory.newHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();
        // Default reconnect mode is ON
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        // no exception at this point
        IMap<Integer, Integer> map = client.getMap(randomMapName());
        map.put(1, 5);

        hazelcastInstance.shutdown();

        hazelcastFactory.newHazelcastInstance();

        map.put(1, 5);
    }

    @Test
    public void testReconnectModeONTwoMembers() {
        HazelcastInstance[] hazelcastInstances = hazelcastFactory.newInstances(getConfig(), 2);

        ClientConfig clientConfig = new ClientConfig();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        // no exception at this point
        IMap<Integer, Integer> map = client.getMap(randomMapName());
        map.put(1, 5);

        hazelcastInstances[0].shutdown();

        map.put(1, 5);
    }

    @Test(expected = HazelcastClientOfflineException.class)
    public void testReconnectModeASYNCSingleMemberInitiallyOffline() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().setReconnectMode(ASYNC);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        assertTrue(client.getLifecycleService().isRunning());

        client.getMap(randomMapName());
    }

    @Test
    public void testReconnectModeASYNCSingleMember() {
        hazelcastFactory.newHazelcastInstance();

        final CountDownLatch connectedLatch = new CountDownLatch(1);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.addListenerConfig(new ListenerConfig(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                if (event.getState().equals(CLIENT_CONNECTED)) {
                    connectedLatch.countDown();
                }
            }
        }));
        clientConfig.getConnectionStrategyConfig().setReconnectMode(ASYNC);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        assertTrue(client.getLifecycleService().isRunning());

        assertOpenEventually(connectedLatch);

        client.getMap(randomMapName());
    }

    @Test
    public void testReconnectModeASYNCSingleMemberStartLate() {
        final CountDownLatch reconnectedLatch = new CountDownLatch(1);

        HazelcastInstance hazelcastInstance = hazelcastFactory.newHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().setReconnectMode(ASYNC);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        assertTrue(client.getLifecycleService().isRunning());

        hazelcastInstance.shutdown();

        client.getLifecycleService().addLifecycleListener(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                if (event.getState().equals(CLIENT_CONNECTED)) {
                    reconnectedLatch.countDown();
                }
            }
        });

        hazelcastFactory.newHazelcastInstance();

        assertOpenEventually(reconnectedLatch);

        client.getMap(randomMapName());
    }

    @Test
    public void testReconnectModeASYNCTwoMembers() {
        hazelcastFactory.newInstances(getConfig(), 2);

        final CountDownLatch connectedLatch = new CountDownLatch(1);
        final CountDownLatch disconnectedLatch = new CountDownLatch(1);
        final CountDownLatch reconnectedLatch = new CountDownLatch(1);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.addListenerConfig(new ListenerConfig(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                if (event.getState().equals(CLIENT_CONNECTED)) {
                    connectedLatch.countDown();
                }
            }
        }));
        clientConfig.getConnectionStrategyConfig().setReconnectMode(ASYNC);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        assertTrue(client.getLifecycleService().isRunning());

        assertOpenEventually(connectedLatch);

        IMap<Integer, Integer> map = client.getMap(randomMapName());
        map.put(1, 5);

        client.getLifecycleService().addLifecycleListener(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                if (LifecycleEvent.LifecycleState.CLIENT_DISCONNECTED.equals(event.getState())) {
                    disconnectedLatch.countDown();
                }
                if (LifecycleEvent.LifecycleState.CLIENT_CONNECTED.equals(event.getState())) {
                    reconnectedLatch.countDown();
                }
            }
        });

        HazelcastClientInstanceImpl clientInstanceImpl = getHazelcastClientInstanceImpl(client);
        HazelcastInstance ownerServer = getOwnerServer(hazelcastFactory, clientInstanceImpl);
        ownerServer.shutdown();

        assertOpenEventually(reconnectedLatch);

        map.get(1);
    }

    @Test
    public void testCustomStrategyImplSyncReConnect() {
        final AtomicInteger initCount = new AtomicInteger(0);
        final AtomicInteger onConnectToClusterCount = new AtomicInteger();
        final AtomicInteger onDisconnectFromClusterCount = new AtomicInteger();
        final AtomicInteger beforeOpenConnectionCount = new AtomicInteger();
        final AtomicInteger onConnectCount = new AtomicInteger(0);
        final AtomicInteger onDisconnectCount = new AtomicInteger(0);

        hazelcastFactory.newInstances(getConfig(), 2);

        ClientConfig clientConfig = new ClientConfig();

        final StateTrackingListener stateTrackingListener = new StateTrackingListener();
        clientConfig.addListenerConfig(new ListenerConfig(stateTrackingListener));

        clientConfig.getConnectionStrategyConfig().setImplementation(new ClientConnectionStrategy() {
            @Override
            public void init() {
                assertEquals(0, initCount.getAndIncrement());
                assertEquals(0, onConnectToClusterCount.get());
                assertEquals(0, onDisconnectFromClusterCount.get());
                assertEquals(0, beforeOpenConnectionCount.get());
                assertEquals(0, onConnectCount.get());
                assertEquals(0, onDisconnectCount.get());

                assertEquals(STARTING, stateTrackingListener.getLatestState());
                connectToCluster();
                assertEquals(CLIENT_CONNECTED, stateTrackingListener.getLatestState());
            }

            @Override
            public void beforeGetConnection(Address target) {
                assertEquals(0, initCount.get());
            }

            @Override
            public void beforeOpenConnection(Address target) {
                assertEquals(0, initCount.get());
                beforeOpenConnectionCount.incrementAndGet();
            }

            @Override
            public void onConnectToCluster() {
                assertEquals(0, initCount.get());
                assertTrue(onConnectToClusterCount.getAndIncrement() < 2);
                assertTrue(onDisconnectFromClusterCount.get() < 2);
                assertTrue(beforeOpenConnectionCount.get() < 4);
                assertTrue(onConnectCount.get() < 4);
                assertTrue(onDisconnectCount.get() < 2);

                assertEquals(CLIENT_CONNECTED, stateTrackingListener.getLatestState());
            }

            @Override
            public void onDisconnectFromCluster() {
                assertEquals(0, initCount.get());
                assertEquals(1, onConnectToClusterCount.get());
                assertEquals(0, onDisconnectFromClusterCount.getAndIncrement());
                assertEquals(2, beforeOpenConnectionCount.get());
                assertEquals(2, onConnectCount.get());
                assertEquals(1, onDisconnectCount.get());

                assertEquals(CLIENT_DISCONNECTED, stateTrackingListener.getLatestState());
                connectToCluster();
            }

            @Override
            public void onConnect(ClientConnection connection) {
                assertEquals(0, initCount.get());
                assertTrue(onConnectToClusterCount.get() < 2);
                assertTrue(onDisconnectFromClusterCount.get() < 2);
                assertTrue(beforeOpenConnectionCount.get() < 4);
                assertTrue(onConnectCount.getAndIncrement() < 3);
                assertTrue(onDisconnectCount.get() < 2);
            }

            @Override
            public void onDisconnect(ClientConnection connection) {
                assertEquals(0, initCount.get());
                assertEquals(1, onConnectToClusterCount.get());
                assertEquals(0, onDisconnectFromClusterCount.getAndIncrement());
                assertEquals(2, beforeOpenConnectionCount.get());
                assertEquals(2, onConnectCount.get());
                assertEquals(0, onDisconnectCount.getAndIncrement());
            }

            @Override
            public void onHeartbeatStopped(ClientConnection connection) {
                fail("onHeartbeatStopped should not be called");
            }

            @Override
            public void onHeartbeatResumed(ClientConnection connection) {
                fail("onHeartbeatResumed should not be called");
            }
        });

        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        assertTrue(client.getLifecycleService().isRunning());

        IMap<Integer, Integer> map = client.getMap(randomMapName());

        assertEquals(0, initCount.get());
        assertEquals(1, onConnectToClusterCount.get());
        assertEquals(0, onDisconnectFromClusterCount.get());
        assertEquals(2, beforeOpenConnectionCount.get());
        assertEquals(2, onConnectCount.get());
        assertEquals(0, onDisconnectCount.get());

        map.put(1, 5);

        HazelcastInstance ownerServer = getOwnerServer(hazelcastFactory, getHazelcastClientInstanceImpl(client));
        ownerServer.shutdown();

        // no exception on this call
        map.get(1);

        assertEquals(0, initCount.get());
        assertEquals(1, onConnectToClusterCount.get());
        assertEquals(1, onDisconnectFromClusterCount.get());
        assertEquals(3, beforeOpenConnectionCount.get());
        assertEquals(3, onConnectCount.get());
        assertEquals(1, onDisconnectCount.get());

    }
}
