/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.tpc;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.connection.ClientConnection;
import com.hazelcast.client.impl.connection.ClientConnectionManager;
import com.hazelcast.client.impl.connection.tcp.TcpClientConnection;
import com.hazelcast.client.impl.connection.tcp.TpcChannelClientConnectionAdapter;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MapPutCodec;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.client.impl.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.OutboundFrame;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.util.ThreadUtil;
import com.hazelcast.map.IMap;
import com.hazelcast.map.MapStoreAdapter;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.spi.exception.TargetDisconnectedException;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.client.properties.ClientProperty.HEARTBEAT_INTERVAL;
import static com.hazelcast.client.properties.ClientProperty.HEARTBEAT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({SlowTest.class})
public class ClientTpcTest extends ClientTestSupport {

    @After
    public void cleanup() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testClientConnectsAllTpcPorts() {
        Config memberConfig = newMemberConfig();
        Hazelcast.newHazelcastInstance(memberConfig);
        Hazelcast.newHazelcastInstance(memberConfig);

        ClientConfig clientConfig = newClientConfig();
        clientConfig.getTpcConfig().setConnectionCount(0);
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        Collection<ClientConnection> connections = getConnectionManager(client).getActiveConnections();
        assertTrueEventually(() -> assertEquals(2, connections.size()));

        assertClientConnectsAllTpcPortsEventually(connections, memberConfig, clientConfig);
    }

    @Test
    public void testClientConnectsAllTpcPorts_whenNewMemberJoins() {
        Config memberConfig = newMemberConfig();
        Hazelcast.newHazelcastInstance(memberConfig);

        ClientConfig clientConfig = newClientConfig();
        clientConfig.getTpcConfig().setConnectionCount(0);
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        Hazelcast.newHazelcastInstance(memberConfig);

        Collection<ClientConnection> connections = getConnectionManager(client).getActiveConnections();
        assertTrueEventually(() -> assertEquals(2, connections.size()));

        assertClientConnectsAllTpcPortsEventually(connections, memberConfig, clientConfig);
    }

    @Test
    public void testClientConnectsAllTpcPorts_afterRestart() {
        Config memberConfig = newMemberConfig();
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(memberConfig);

        ClientConfig clientConfig = newClientConfig();
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        CountDownLatch disconnected = new CountDownLatch(1);
        client.getLifecycleService().addLifecycleListener(state -> {
            if (state.getState() == LifecycleEvent.LifecycleState.CLIENT_DISCONNECTED) {
                disconnected.countDown();
            }
        });

        Collection<ClientConnection> connections = getConnectionManager(client).getActiveConnections();
        assertClientConnectsAllTpcPortsEventually(connections, memberConfig, clientConfig);

        instance.shutdown();
        assertOpenEventually(disconnected);
        assertTrueEventually(() -> assertEquals(0, connections.size()));

        Hazelcast.newHazelcastInstance(memberConfig);

        assertTrueEventually(() -> assertEquals(1, connections.size()));
        assertClientConnectsAllTpcPortsEventually(connections, memberConfig, clientConfig);
    }

    @Test
    public void testClientRoutesPartitionBoundRequestsToTpcConnections() {
        Config memberConfig = newMemberConfig();
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(memberConfig);
        HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(memberConfig);
        warmUpPartitions(instance1, instance2);

        ClientConfig clientConfig = newClientConfig();
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        IMap<String, Integer> map = client.getMap(randomMapName());

        ClientConnectionManager connectionManager = getConnectionManager(client);
        Collection<ClientConnection> connections = connectionManager.getActiveConnections();
        assertTrueEventually(() -> assertEquals(2, connections.size()));

        assertClientConnectsAllTpcPortsEventually(connections, memberConfig, clientConfig);

        // Wait until the partition table is received on the client-side
        PartitionService partitionService = client.getPartitionService();
        assertTrueEventually(() -> assertFalse(partitionService.getPartitions().isEmpty()));

        int partitionCount = partitionService.getPartitions().size();
        for (int i = 0; i < partitionCount; i++) {
            String key = generateKeyForPartition(instance1, i);
            long currentTimeMillis = System.currentTimeMillis();
            map.put(key, i);

            UUID ownerUuid = partitionService.getPartition(key).getOwner().getUuid();
            TcpClientConnection partitionOwner = (TcpClientConnection) connectionManager.getConnection(ownerUuid);
            assertNotNull(partitionOwner);

            Channel[] tpcChannels = partitionOwner.getTpcChannels();
            assertNotNull(tpcChannels);

            Channel tpcChannel = tpcChannels[i % tpcChannels.length];
            assertFalse(tpcChannel.isClosed());
            assertTrue(tpcChannel.lastWriteTimeMillis() >= currentTimeMillis);
        }
    }

    @Test
    public void testClientRoutesNonPartitionBoundRequestsToClassicConnections() {
        Config memberConfig = newMemberConfig();
        Hazelcast.newHazelcastInstance(memberConfig);

        ClientConfig clientConfig = newClientConfig();
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        IMap<String, Integer> map = client.getMap(randomMapName());

        ClientConnectionManager connectionManager = getConnectionManager(client);
        Collection<ClientConnection> connections = connectionManager.getActiveConnections();

        assertClientConnectsAllTpcPortsEventually(connections, memberConfig, clientConfig);

        long currentTimeMillis = System.currentTimeMillis();
        map.size();
        ClientConnection connection = connectionManager.getRandomConnection();
        assertTrue(connection.lastWriteTimeMillis() >= currentTimeMillis);
    }

    @Test
    public void testConnectionCloses_whenTpcChannelsClose() {
        Config memberConfig = newMemberConfig();
        Hazelcast.newHazelcastInstance(memberConfig);

        ClientConfig clientConfig = newClientConfig();
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        ClientConnectionManager connectionManager = getConnectionManager(client);
        Collection<ClientConnection> connections = connectionManager.getActiveConnections();

        assertClientConnectsAllTpcPortsEventually(connections, memberConfig, clientConfig);

        CountDownLatch disconnected = new CountDownLatch(1);
        CountDownLatch reconnected = new CountDownLatch(1);
        client.getLifecycleService().addLifecycleListener(state -> {
            if (state.getState() == LifecycleEvent.LifecycleState.CLIENT_DISCONNECTED) {
                disconnected.countDown();
            } else if (state.getState() == LifecycleEvent.LifecycleState.CLIENT_CONNECTED) {
                reconnected.countDown();
            }
        });

        assertEquals(1, connections.size());
        TcpClientConnection connection = (TcpClientConnection) connections.iterator().next();
        Channel[] channels = connection.getTpcChannels();

        // Write an unexpected frame to cause problem in the pipeline
        // and close the channel
        channels[0].write(new OutboundFrame() {
            @Override
            public boolean isUrgent() {
                return false;
            }

            @Override
            public int getFrameLength() {
                return 0;
            }
        });

        assertOpenEventually(disconnected);

        assertFalse(connection.isAlive());
        for (Channel channel : channels) {
            // All the channels must be closed as well
            assertTrue(channel.isClosed());
        }

        assertOpenEventually(reconnected);

        assertClientConnectsAllTpcPortsEventually(connections, memberConfig, clientConfig);
    }

    @Test
    public void testTpcChannelsClose_whenConnectionCloses() {
        Config memberConfig = newMemberConfig();
        Hazelcast.newHazelcastInstance(memberConfig);

        ClientConfig clientConfig = newClientConfig();
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        ClientConnectionManager connectionManager = getConnectionManager(client);
        Collection<ClientConnection> connections = connectionManager.getActiveConnections();

        assertClientConnectsAllTpcPortsEventually(connections, memberConfig, clientConfig);

        CountDownLatch disconnected = new CountDownLatch(1);
        CountDownLatch reconnected = new CountDownLatch(1);
        client.getLifecycleService().addLifecycleListener(state -> {
            if (state.getState() == LifecycleEvent.LifecycleState.CLIENT_DISCONNECTED) {
                disconnected.countDown();
            } else if (state.getState() == LifecycleEvent.LifecycleState.CLIENT_CONNECTED) {
                reconnected.countDown();
            }
        });

        assertEquals(1, connections.size());
        TcpClientConnection connection = (TcpClientConnection) connections.iterator().next();
        Channel[] channels = connection.getTpcChannels();

        connection.close("Expected", null);

        assertOpenEventually(disconnected);

        assertFalse(connection.isAlive());
        for (Channel channel : channels) {
            // All the channels must be closed as well
            assertTrue(channel.isClosed());
        }

        assertOpenEventually(reconnected);

        assertClientConnectsAllTpcPortsEventually(connections, memberConfig, clientConfig);
    }

    @Test
    public void testPartitionBoundPendingInvocations_whenConnectionCloses() {
        String mapName = randomMapName();
        Config memberConfig = newMemberWithMapStoreConfig(mapName);
        Hazelcast.newHazelcastInstance(memberConfig);

        ClientConfig clientConfig = newClientConfig();
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        ClientConnectionManager connectionManager = getConnectionManager(client);
        Collection<ClientConnection> connections = connectionManager.getActiveConnections();

        assertClientConnectsAllTpcPortsEventually(connections, memberConfig, clientConfig);

        IMap<Integer, Integer> map = client.getMap(mapName);
        CompletableFuture<Integer> future = map.putAsync(1, 1).toCompletableFuture();
        connections.iterator().next().close("Expected", null);

        // Should get TargetDisconnectedException and retried based on our rules,
        // which bubbles the exception to the user for non-retryable messages
        assertThatThrownBy(future::join)
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(TargetDisconnectedException.class);
    }

    @Test
    public void testTPCChannelTargetedPendingInvocations_whenConnectionCloses() {
        // We don't send invocations to TPC channels this way, but this is just
        // to make sure that invocation directly to the TPC channels work, and
        // closing the connection (hence the channel) cleanups the pending invocations
        String mapName = randomMapName();
        Config memberConfig = newMemberWithMapStoreConfig(mapName);
        Hazelcast.newHazelcastInstance(memberConfig);

        ClientConfig clientConfig = newClientConfig();
        HazelcastClientInstanceImpl client
                = getHazelcastClientInstanceImpl(HazelcastClient.newHazelcastClient(clientConfig));
        ClientConnectionManager connectionManager = client.getConnectionManager();
        Collection<ClientConnection> connections = connectionManager.getActiveConnections();

        assertClientConnectsAllTpcPortsEventually(connections, memberConfig, clientConfig);

        ClientConnection connection = connections.iterator().next();
        Channel[] tpcChannels = connection.getTpcChannels();

        int key = 1;
        int value = 1;

        int partitionId = client.getPartitionService().getPartition(key).getPartitionId();
        Channel targetChannel = tpcChannels[partitionId % tpcChannels.length];
        ClientConnection adapter
                = (ClientConnection) targetChannel.attributeMap().get(TpcChannelClientConnectionAdapter.class);

        InternalSerializationService serializationService = client.getSerializationService();

        Data keyData = serializationService.toData(key);
        Data valueData = serializationService.toData(value);

        ClientMessage request = MapPutCodec.encodeRequest(mapName, keyData, valueData, ThreadUtil.getThreadId(), -1);
        ClientInvocationFuture future = new ClientInvocation(client, request, mapName, adapter).invoke();

        connection.close("Expected", null);

        // Should get TargetDisconnectedException and retried based on our rules,
        // which bubbles the exception to the user for non-retryable messages
        assertThatThrownBy(future::join)
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(TargetDisconnectedException.class);
    }

    @Test
    public void testTpcEnabledClient_inTpcDisabledCluster() {
        Hazelcast.newHazelcastInstance();
        Hazelcast.newHazelcastInstance();

        HazelcastInstance client = HazelcastClient.newHazelcastClient(newClientConfig());
        IMap<String, String> map = client.getMap(randomMapName());

        ClientConnectionManager connectionManager = getConnectionManager(client);
        Collection<ClientConnection> connections = connectionManager.getActiveConnections();
        assertTrueEventually(() -> assertEquals(2, connections.size()));

        assertNoConnectionToTpcPortsAllTheTime(connections);

        map.put("42", "42");
        assertEquals("42", map.get("42"));
    }

    @Test
    public void testTpcDisabledClient_inTpcEnabledCluster() {
        Config memberConfig = newMemberConfig();
        Hazelcast.newHazelcastInstance(memberConfig);
        Hazelcast.newHazelcastInstance(memberConfig);

        HazelcastInstance client = HazelcastClient.newHazelcastClient();
        IMap<String, String> map = client.getMap(randomMapName());

        ClientConnectionManager connectionManager = getConnectionManager(client);
        Collection<ClientConnection> connections = connectionManager.getActiveConnections();
        assertTrueEventually(() -> assertEquals(2, connections.size()));

        assertNoConnectionToTpcPortsAllTheTime(connections);

        map.put("42", "42");
        assertEquals("42", map.get("42"));
    }

    @Test
    public void testTpcClient_heartbeatsToIdleTpcChannels() {
        Config memberConfig = newMemberConfig();
        Hazelcast.newHazelcastInstance(memberConfig);

        ClientConfig clientConfig = newClientConfig();
        clientConfig.setProperty(HEARTBEAT_INTERVAL.getName(), "1000");

        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        ClientConnectionManager connectionManager = getConnectionManager(client);
        Collection<ClientConnection> connections = connectionManager.getActiveConnections();
        assertClientConnectsAllTpcPortsEventually(connections, memberConfig, clientConfig);

        ClientConnection connection = connectionManager.getRandomConnection();
        assertTrue(connection.isAlive());

        Channel[] tpcChannels = connection.getTpcChannels();
        assertNotNull(tpcChannels);

        long now = System.currentTimeMillis();
        assertTrueEventually(() -> {
            for (Channel channel : tpcChannels) {
                assertTrue(channel.lastWriteTimeMillis() > now);
            }
        });
    }

    // this test is broken and needs to be fixed. Will be done as a part of a seperate
    // pr because it goes beyond changing the number of connections.
    @Ignore
    @Test
    public void testTPCClient_heartbeatsToNotRespondingTPCChannelsTimeouts() {
        Config memberConfig = newMemberConfig();
        Hazelcast.newHazelcastInstance(memberConfig);

        ClientConfig clientConfig = newClientConfig();
        clientConfig.setProperty(HEARTBEAT_INTERVAL.getName(), "1000");
        clientConfig.setProperty(HEARTBEAT_TIMEOUT.getName(), "3000");

        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        ClientConnectionManager connectionManager = getConnectionManager(client);
        Collection<ClientConnection> connections = connectionManager.getActiveConnections();

        assertClientConnectsAllTpcPortsEventually(connections, memberConfig, clientConfig);

        ClientConnection connection = connections.iterator().next();
        assertTrue(connection.isAlive());

        // This is a long-running task that will block the operation thread, and it
        // should not be able to respond to ping requests
        spawn(() -> {
            String mapName = randomMapName();
            IMap<Integer, Integer> map = client.getMap(mapName);
            map.put(1, 1);
            map.executeOnKey(1, entry -> {
                try {
                    Thread.sleep(TimeUnit.SECONDS.toMillis(1_000));
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return null;
            });
        });

        assertTrueEventually(() -> assertFalse(connection.isAlive()));
    }

    private void assertNoConnectionToTpcPortsAllTheTime(Collection<ClientConnection> connections) {
        assertTrueAllTheTime(() -> {
            for (ClientConnection connection : connections) {
                TcpClientConnection clientConnection = (TcpClientConnection) connection;
                assertTrue(clientConnection.isAlive());
                assertNull(clientConnection.getTpcChannels());
            }
        }, 3);
    }

    private void assertClientConnectsAllTpcPortsEventually(Collection<ClientConnection> connections,
                                                           Config memberConfig,
                                                           ClientConfig clientConfig) {
        final int connectionCount = clientConfig.getTpcConfig().getConnectionCount() == 0
                ? memberConfig.getTpcConfig().getEventloopCount()
                : clientConfig.getTpcConfig().getConnectionCount();

        assertTrueEventually(() -> {
            for (ClientConnection connection : connections) {
                TcpClientConnection clientConnection = (TcpClientConnection) connection;

                Channel[] tpcChannels = clientConnection.getTpcChannels();
                assertNotNull(tpcChannels);
                assertEquals(connectionCount, tpcChannels.length);

                for (Channel channel : tpcChannels) {
                    assertNotNull(channel);
                    assertFalse(channel.isClosed());
                }
            }
        });
    }

    private ClientConnectionManager getConnectionManager(HazelcastInstance client) {
        return getHazelcastClientInstanceImpl(client).getConnectionManager();
    }

    private ClientConfig newClientConfig() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getTpcConfig().setEnabled(true);
        return clientConfig;
    }

    private Config newMemberConfig() {
        Config config = new Config();
        // Jet prints too many logs
        config.getJetConfig().setEnabled(false);

        int loopCount = Math.min(Runtime.getRuntime().availableProcessors(), 3);
        config.getTpcConfig()
                .setEnabled(true)
                .setEventloopCount(loopCount);
        return config;
    }

    private Config newMemberWithMapStoreConfig(String mapName) {
        Config config = newMemberConfig();
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true).setImplementation(new MapStoreAdapter<Integer, Integer>() {
            @Override
            public Integer load(Integer key) {
                // Simulate a long-running operation
                sleepSeconds(1000);
                return super.load(key);
            }
        });
        config.addMapConfig(new MapConfig(mapName).setMapStoreConfig(mapStoreConfig));
        return config;
    }
}
