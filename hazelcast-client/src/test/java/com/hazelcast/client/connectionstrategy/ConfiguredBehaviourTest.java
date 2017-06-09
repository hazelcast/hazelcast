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
import com.hazelcast.util.ExceptionUtil;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

import static com.hazelcast.client.config.ClientConnectionStrategyConfig.ReconnectMode.ASYNC;
import static com.hazelcast.client.config.ClientConnectionStrategyConfig.ReconnectMode.OFF;
import static com.hazelcast.core.LifecycleEvent.LifecycleState.CLIENT_CONNECTED;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ConfiguredBehaviourTest
        extends ClientTestSupport {
    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

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
    public void testAsyncStartMode() {

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().setClientStartAsync(true);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        client.getMap(randomMapName());
    }

    @Test(expected = HazelcastClientOfflineException.class)
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

    @Test(expected = HazelcastClientOfflineException.class)
    public void testReconnectModeOFFTwoMembers() {
        HazelcastInstance[] hazelcastInstances = hazelcastFactory.newInstances(getConfig(), 2);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().setReconnectMode(OFF);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        // no exception at this point
        IMap<Integer, Integer> map = client.getMap(randomMapName());
        map.put(1, 5);

        hazelcastInstances[0].shutdown();

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
    public void testReconnectModeASYNCTwoMembers() {
        hazelcastFactory.newInstances(getConfig(), 2);

        final CountDownLatch connectedLatch = new CountDownLatch(1);
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
                reconnectedLatch.countDown();
            }
        });

        assertOpenEventually(reconnectedLatch);

        HazelcastClientInstanceImpl clientInstanceImpl = getHazelcastClientInstanceImpl(client);
        HazelcastInstance ownerServer = getOwnerServer(hazelcastFactory, clientInstanceImpl);
        ownerServer.shutdown();

        map.get(1);
    }

    @Test
    public void testCustomImplSyncReConnect() {
        hazelcastFactory.newInstances(getConfig(), 2);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().setImplementation(new ClientConnectionStrategy() {
            @Override
            public void init() {
                try {
                    connectToCluster();
                } catch (Exception e) {
                    ExceptionUtil.rethrow(e);
                }
            }

            @Override
            public void beforeGetConnection(Address target) {

            }

            @Override
            public void beforeOpenConnection(Address target) {

            }

            @Override
            public void onConnectToCluster() {

            }

            @Override
            public void onDisconnectFromCluster() {
                try {
                    connectToCluster();
                } catch (Exception e) {
                    ExceptionUtil.rethrow(e);
                }
            }

            @Override
            public void onConnect(ClientConnection connection) {

            }

            @Override
            public void onDisconnect(ClientConnection connection) {

            }

            @Override
            public void onHeartbeatStopped(ClientConnection connection) {
            }

            @Override
            public void onHeartbeatResumed(ClientConnection connection) {

            }
        });

        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        assertTrue(client.getLifecycleService().isRunning());

        IMap<Integer, Integer> map = client.getMap(randomMapName());

        map.put(1, 5);

        HazelcastInstance ownerServer = getOwnerServer(hazelcastFactory, getHazelcastClientInstanceImpl(client));
        ownerServer.shutdown();

        // no exception on this call
        map.get(1);
    }
}
