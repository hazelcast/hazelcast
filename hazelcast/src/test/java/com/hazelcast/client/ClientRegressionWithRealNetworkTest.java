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

package com.hazelcast.client;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientConnectionStrategyConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.impl.connection.AddressProvider;
import com.hazelcast.client.impl.connection.Addresses;
import com.hazelcast.client.impl.connection.ClientConnectionManager;
import com.hazelcast.client.impl.connection.nio.ClientConnectionManagerImpl;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.properties.ClientProperty;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.client.util.AddressHelper;
import com.hazelcast.config.Config;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.cluster.Address;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientRegressionWithRealNetworkTest extends ClientTestSupport {

    @After
    public void cleanUp() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testClientPortConnection() {
        final Config config1 = new Config();
        config1.setClusterName("foo");
        config1.getNetworkConfig().setPort(5701);
        final HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config1);
        instance1.getMap("map").put("key", "value");

        final Config config2 = new Config();
        config2.setClusterName("bar");
        config2.getNetworkConfig().setPort(5702);
        Hazelcast.newHazelcastInstance(config2);

        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName("bar");
        final HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        final IMap<Object, Object> map = client.getMap("map");
        assertNull(map.put("key", "value"));
        assertEquals(1, map.size());
    }

    @Test
    public void testClientConnectionBeforeServerReady() throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                Hazelcast.newHazelcastInstance();
            }
        });

        final CountDownLatch clientLatch = new CountDownLatch(1);
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                ClientConfig config = new ClientConfig();
                config.getConnectionStrategyConfig().getConnectionRetryConfig().setFailOnMaxBackoff(false);
                HazelcastClient.newHazelcastClient(config);
                clientLatch.countDown();
            }
        });

        assertOpenEventually(clientLatch);
    }

    @Test
    public void testConnectionCountAfterOwnerReconnect_memberHostname_clientIp() {
        testConnectionCountAfterOwnerReconnect("localhost", "127.0.0.1");
    }

    @Test
    public void testConnectionCountAfterOwnerReconnect_memberHostname_clientHostname() {
        testConnectionCountAfterOwnerReconnect("localhost", "localhost");
    }

    @Test
    public void testConnectionCountAfterOwnerReconnect_memberIp_clientIp() {
        testConnectionCountAfterOwnerReconnect("127.0.0.1", "127.0.0.1");
    }

    @Test
    public void testConnectionCountAfterOwnerReconnect_memberIp_clientHostname() {
        testConnectionCountAfterOwnerReconnect("127.0.0.1", "localhost");
    }

    private void testConnectionCountAfterOwnerReconnect(String memberAddress, String clientAddress) {
        Config config = new Config();
        config.getNetworkConfig().setPublicAddress(memberAddress);
        HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().addAddress(clientAddress);
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setFailOnMaxBackoff(false);

        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        HazelcastClientInstanceImpl clientInstanceImpl = getHazelcastClientInstanceImpl(client);
        final ClientConnectionManager connectionManager = clientInstanceImpl.getConnectionManager();

        Hazelcast.newHazelcastInstance(config);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(2, connectionManager.getActiveConnections().size());
            }
        });

        final CountDownLatch disconnectedLatch = new CountDownLatch(1);
        final CountDownLatch connectedLatch = new CountDownLatch(1);
        client.getLifecycleService().addLifecycleListener(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                LifecycleEvent.LifecycleState state = event.getState();
                if (state.equals(LifecycleEvent.LifecycleState.CLIENT_DISCONNECTED)) {
                    disconnectedLatch.countDown();
                } else if (state.equals(LifecycleEvent.LifecycleState.CLIENT_CONNECTED)) {
                    connectedLatch.countDown();
                }
            }
        });

        hazelcastInstance.shutdown();

        assertOpenEventually(disconnectedLatch);

        assertOpenEventually(connectedLatch);

        assertEquals(1, connectionManager.getActiveConnections().size());
    }

    @Test
    public void testListenersAfterOwnerDisconnect_memberHostname_clientIp() {
        testListenersAfterOwnerDisconnect("localhost", "127.0.0.1");
    }

    @Test
    public void testListenersAfterOwnerDisconnect_memberHostname_clientHostname() {
        testListenersAfterOwnerDisconnect("localhost", "localhost");
    }

    @Test
    public void testListenersAfterOwnerDisconnect_memberIp_clientIp() {
        testListenersAfterOwnerDisconnect("127.0.0.1", "127.0.0.1");
    }

    @Test
    public void testListenersAfterOwnerDisconnect_memberIp_clientHostname() {
        testListenersAfterOwnerDisconnect("127.0.0.1", "localhost");
    }

    private void testListenersAfterOwnerDisconnect(String memberAddress, String clientAddress) {
        Config config = new Config();
        int heartBeatSeconds = 6;
        config.getNetworkConfig().setPublicAddress(memberAddress);
        config.setProperty(GroupProperty.CLIENT_HEARTBEAT_TIMEOUT_SECONDS.getName(), Integer.toString(heartBeatSeconds));
        HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
        networkConfig.addAddress(clientAddress);
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setFailOnMaxBackoff(false);
        final HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        final IMap<Integer, Integer> map = client.getMap("test");

        final AtomicInteger eventCount = new AtomicInteger(0);

        map.addEntryListener(new EntryAddedListener() {
            @Override
            public void entryAdded(EntryEvent event) {
                eventCount.incrementAndGet();
            }
        }, false);

        Hazelcast.newHazelcastInstance(config);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                HazelcastClientInstanceImpl clientInstanceImpl = getHazelcastClientInstanceImpl(client);
                int size = clientInstanceImpl.getConnectionManager().getActiveConnections().size();
                assertEquals(2, size);

            }
        });

        hazelcastInstance.shutdown();

        sleepAtLeastSeconds(2 * heartBeatSeconds);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                map.put(1, 2);
                assertNotEquals(0, eventCount.get());
            }
        });
    }

    @Test
    public void testOperationsContinueWhenOwnerDisconnected_reconnectModeAsync() throws Exception {
        testOperationsContinueWhenOwnerDisconnected(ClientConnectionStrategyConfig.ReconnectMode.ASYNC);
    }

    @Test
    public void testOperationsContinueWhenOwnerDisconnected_reconnectModeOn() throws Exception {
        testOperationsContinueWhenOwnerDisconnected(ClientConnectionStrategyConfig.ReconnectMode.ON);
    }

    private void testOperationsContinueWhenOwnerDisconnected(ClientConnectionStrategyConfig.ReconnectMode reconnectMode) throws Exception {
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().setReconnectMode(reconnectMode);
        final AtomicBoolean waitFlag = new AtomicBoolean();
        final CountDownLatch testFinished = new CountDownLatch(1);
        final AddressProvider addressProvider = new AddressProvider() {
            @Override
            public Addresses loadAddresses() {
                if (waitFlag.get()) {
                    try {
                        testFinished.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                return AddressHelper.getSocketAddresses("127.0.0.1");
            }

            @Override
            public Address translate(Address address) {
                return address;
            }
        };
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setFailOnMaxBackoff(false);
        clientConfig.setProperty(ClientProperty.INVOCATION_TIMEOUT_SECONDS.getName(), "3");
        HazelcastInstance client = HazelcastClientUtil.newHazelcastClient(addressProvider, clientConfig);


        HazelcastInstance instance2 = Hazelcast.newHazelcastInstance();

        warmUpPartitions(instance1, instance2);
        String keyOwnedBy2 = generateKeyOwnedBy(instance2);


        IMap<Object, Object> clientMap = client.getMap("test");

        //we are closing a connection and making sure It is not established ever again
        waitFlag.set(true);
        instance1.shutdown();

        //we expect these operations to run without throwing exception, since they are done on live instance.
        clientMap.put(keyOwnedBy2, 1);
        assertEquals(1, clientMap.get(keyOwnedBy2));

        testFinished.countDown();

    }

    @Test
    public void testNioChannelLeakTest() {
        ClientConfig config = new ClientConfig();
        config.getConnectionStrategyConfig().setAsyncStart(true).
                setReconnectMode(ClientConnectionStrategyConfig.ReconnectMode.ASYNC)
                .getConnectionRetryConfig().setInitialBackoffMillis(1).setMaxBackoffMillis(1000);
        HazelcastInstance client = HazelcastClient.newHazelcastClient(config);
        final HazelcastClientInstanceImpl clientInstanceImpl = getHazelcastClientInstanceImpl(client);
        final ClientConnectionManagerImpl connectionManager = (ClientConnectionManagerImpl) clientInstanceImpl.getConnectionManager();
        sleepSeconds(2);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(0, connectionManager.getNetworking().getChannels().size());
            }
        });
        client.shutdown();

    }
}
