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

package com.hazelcast.client;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientConnectionStrategyConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.connection.AddressProvider;
import com.hazelcast.client.impl.connection.Addresses;
import com.hazelcast.client.impl.connection.ClientConnectionManager;
import com.hazelcast.client.impl.connection.tcp.TcpClientConnectionManager;
import com.hazelcast.client.properties.ClientProperty;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.client.util.AddressHelper;
import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class ClientRegressionWithRealNetworkTest extends ClientTestSupport {

    @After
    public void cleanUp() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    @Category(QuickTest.class)
    public void testClientPortConnection() {
        Config config1 = new Config();
        config1.setClusterName("foo");
        config1.getNetworkConfig().setPort(5701);
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config1);
        instance1.getMap("map").put("key", "value");

        Config config2 = new Config();
        config2.setClusterName("bar");
        config2.getNetworkConfig().setPort(5702);
        Hazelcast.newHazelcastInstance(config2);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName("bar");
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        IMap<Object, Object> map = client.getMap("map");
        assertNull(map.put("key", "value"));
        assertEquals(1, map.size());
    }

    @Test
    public void testClientConnectionBeforeServerReady() {
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        executorService.submit((Runnable) Hazelcast::newHazelcastInstance);

        CountDownLatch clientLatch = new CountDownLatch(1);
        executorService.submit(() -> {
            ClientConfig config = new ClientConfig();
            config.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
            HazelcastClient.newHazelcastClient(config);
            clientLatch.countDown();
        });

        assertOpenEventually(clientLatch);
    }

    @Test
    public void testConnectionCountAfterClientReconnect_memberHostname_clientIp() {
        testConnectionCountAfterClientReconnect("localhost", "127.0.0.1");
    }

    @Test
    public void testConnectionCountAfterClientReconnect_memberHostname_clientHostname() {
        testConnectionCountAfterClientReconnect("localhost", "localhost");
    }

    @Test
    public void testConnectionCountAfterClientReconnect_memberIp_clientIp() {
        testConnectionCountAfterClientReconnect("127.0.0.1", "127.0.0.1");
    }

    @Test
    public void testConnectionCountAfterClientReconnect_memberIp_clientHostname() {
        testConnectionCountAfterClientReconnect("127.0.0.1", "localhost");
    }

    private void testConnectionCountAfterClientReconnect(String memberAddress, String clientAddress) {
        Config config = new Config();
        config.getNetworkConfig().setPublicAddress(memberAddress);
        HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().addAddress(clientAddress);
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);

        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        HazelcastClientInstanceImpl clientInstanceImpl = getHazelcastClientInstanceImpl(client);
        ClientConnectionManager connectionManager = clientInstanceImpl.getConnectionManager();

        assertTrueEventually(() -> assertEquals(1, connectionManager.getActiveConnections().size()));

        ReconnectListener reconnectListener = new ReconnectListener();
        client.getLifecycleService().addLifecycleListener(reconnectListener);

        hazelcastInstance.shutdown();
        assertOpenEventually(reconnectListener.disconnectedLatch);
        Hazelcast.newHazelcastInstance(config);

        assertOpenEventually(reconnectListener.reconnectedLatch);
        assertEquals(1, connectionManager.getActiveConnections().size());
    }

    @Test
    public void testListenersAfterClientDisconnected_memberHostname_clientIp() {
        testListenersAfterClientDisconnected("localhost", "127.0.0.1");
    }

    @Test
    public void testListenersAfterClientDisconnected_memberHostname_clientHostname() {
        testListenersAfterClientDisconnected("localhost", "localhost");
    }

    @Test
    public void testListenersAfterClientDisconnected_memberIp_clientIp() {
        testListenersAfterClientDisconnected("127.0.0.1", "127.0.0.1");
    }

    @Test
    public void testListenersAfterClientDisconnected_memberIp_clientHostname() {
        testListenersAfterClientDisconnected("127.0.0.1", "localhost");
    }

    private void testListenersAfterClientDisconnected(String memberAddress, String clientAddress) {
        Config config = new Config();
        int heartBeatSeconds = 6;
        config.getNetworkConfig().setPublicAddress(memberAddress);
        config.setProperty(ClusterProperty.CLIENT_HEARTBEAT_TIMEOUT_SECONDS.getName(), Integer.toString(heartBeatSeconds));
        HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
        networkConfig.addAddress(clientAddress);
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        IMap<Integer, Integer> map = client.getMap("test");

        AtomicInteger eventCount = new AtomicInteger(0);

        map.addEntryListener((EntryAddedListener<Object, Object>) event -> eventCount.incrementAndGet(), false);

        assertTrueEventually(() -> {
            HazelcastClientInstanceImpl clientInstanceImpl = getHazelcastClientInstanceImpl(client);
            int size = clientInstanceImpl.getConnectionManager().getActiveConnections().size();
            assertEquals(1, size);

        });

        hazelcastInstance.shutdown();
        sleepAtLeastSeconds(2 * heartBeatSeconds);
        Hazelcast.newHazelcastInstance(config);

        assertTrueEventually(() -> {
            map.remove(1);
            map.put(1, 2);
            assertNotEquals(0, eventCount.get());
        });
    }

    @Test
    public void testOperationsContinueWhenClientDisconnected_reconnectModeAsync() {
        testOperationsContinueWhenClientDisconnected(ClientConnectionStrategyConfig.ReconnectMode.ASYNC);
    }

    @Test
    public void testOperationsContinueWhenClientDisconnected_reconnectModeOn() {
        testOperationsContinueWhenClientDisconnected(ClientConnectionStrategyConfig.ReconnectMode.ON);
    }

    private void testOperationsContinueWhenClientDisconnected(ClientConnectionStrategyConfig.ReconnectMode reconnectMode) {
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().setReconnectMode(reconnectMode);
        AtomicBoolean waitFlag = new AtomicBoolean();
        CountDownLatch testFinished = new CountDownLatch(1);
        AddressProvider addressProvider = new AddressProvider() {
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

            @Override
            public Address translate(Member member) throws Exception {
                return member.getAddress();
            }
        };
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        clientConfig.setProperty(ClientProperty.INVOCATION_TIMEOUT_SECONDS.getName(), "3");
        HazelcastInstance client = HazelcastClientUtil.newHazelcastClient(addressProvider, clientConfig);


        HazelcastInstance instance2 = Hazelcast.newHazelcastInstance();

        warmUpPartitions(instance1, instance2);
        String keyOwnedBy2 = generateKeyOwnedBy(instance2);
        makeSureConnectedToServers(client, 2);

        IMap<Object, Object> clientMap = client.getMap("test");

        //we are closing a connection and making sure It is not established ever again
        waitFlag.set(true);
        UUID memberUUID = instance1.getLocalEndpoint().getUuid();
        instance1.shutdown();

        makeSureDisconnectedFromServer(client, memberUUID);
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
                .getConnectionRetryConfig().setInitialBackoffMillis(1).setClusterConnectTimeoutMillis(1000);
        HazelcastInstance client = HazelcastClient.newHazelcastClient(config);
        HazelcastClientInstanceImpl clientInstanceImpl = getHazelcastClientInstanceImpl(client);
        TcpClientConnectionManager connectionManager = (TcpClientConnectionManager) clientInstanceImpl.getConnectionManager();
        sleepSeconds(2);
        assertTrueEventually(() -> assertEquals(0, connectionManager.getNetworking().getChannels().size()));
        client.shutdown();
    }
}
