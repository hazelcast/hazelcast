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
package com.hazelcast.client.impl.connection.tcp;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.hazelcast.client.impl.connection.tcp.ConnectionManagerTestUtil.newConnectionManager;
import static com.hazelcast.test.HazelcastTestSupport.assertContainsAll;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class TcpClientConnectionManagerTest {

    @Test
    public void testStartOpensConnectionToCluster() {
        TcpClientConnectionManager connectionManager = newConnectionManager(new ClientConfig());
        connectionManager.start();
        Collection<Connection> activeConnections = connectionManager.getActiveConnections();
        assertEquals(1, activeConnections.size());

        connectionManager.shutdown();
    }

    @Test
    public void testStartOpensConnectionToClusterEventually_onAsyncStart() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().setAsyncStart(true);
        TcpClientConnectionManager connectionManager = newConnectionManager(clientConfig);
        connectionManager.start();
        connectionManager.startClusterThread();

        Collection<Connection> activeConnections = connectionManager.getActiveConnections();

        assertTrueEventually(() -> assertEquals(1, activeConnections.size()));

        connectionManager.shutdown();
    }

    @Test
    public void testTryConnectToAllClusterMembers() {
        ConnectionManagerTestUtil.ClusterContext context = new ConnectionManagerTestUtil.ClusterContext();
        List<UUID> expected = Arrays.asList(context.addMember("127.0.0.1", 5701),
                context.addMember("127.0.0.1", 5702));
        TcpClientConnectionManager connectionManager = newConnectionManager(new ClientConfig(), context);
        connectionManager.start();
        connectionManager.tryConnectToAllClusterMembers();

        Collection<Connection> activeConnections = connectionManager.getActiveConnections();
        assertEquals(2, activeConnections.size());
        List<UUID> actual = activeConnections.stream().map(Connection::getRemoteUuid).collect(Collectors.toList());
        assertContainsAll(actual, expected);

        connectionManager.shutdown();
    }

    @Test
    public void testTryConnectToAllClusterMembersEventually_onAsyncStart() {
        ConnectionManagerTestUtil.ClusterContext context = new ConnectionManagerTestUtil.ClusterContext();
        List<UUID> expected = Arrays.asList(context.addMember("127.0.0.1", 5701),
                context.addMember("127.0.0.1", 5702));
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().setAsyncStart(true);
        TcpClientConnectionManager connectionManager = newConnectionManager(clientConfig, context);
        connectionManager.start();
        connectionManager.tryConnectToAllClusterMembers();
        connectionManager.startClusterThread();

        assertTrueEventually(() -> {
            Collection<Connection> activeConnections = connectionManager.getActiveConnections();
            assertEquals(2, activeConnections.size());
            List<UUID> actual = activeConnections.stream().map(Connection::getRemoteUuid).collect(Collectors.toList());
            assertContainsAll(actual, expected);
        });

        connectionManager.shutdown();
    }

    @Test
    public void testConnectToAllIsNoopWhenAlreadyConnectedToAll() {
        ConnectionManagerTestUtil.ClusterContext context = new ConnectionManagerTestUtil.ClusterContext();
        context.addMember("127.0.0.1", 5701);
        context.addMember("127.0.0.1", 5702);

        TcpClientConnectionManager connectionManager = newConnectionManager(new ClientConfig(), context);
        connectionManager.start();
        connectionManager.tryConnectToAllClusterMembers();

        Collection<Connection> activeConnections = connectionManager.getActiveConnections();
        assertEquals(2, activeConnections.size());

        connectionManager.connectToAllClusterMembers();

        assertEquals(2, activeConnections.size());
        connectionManager.shutdown();
    }

    @Test
    public void testClusterUuidChange_whenConnected() {
        ConnectionManagerTestUtil.ClusterContext context = new ConnectionManagerTestUtil.ClusterContext();
        context.addMember("127.0.0.1", 5701);
        context.addMember("127.0.0.1", 5702);

        TcpClientConnectionManager connectionManager = newConnectionManager(new ClientConfig(), context);
        connectionManager.start();
        connectionManager.tryConnectToAllClusterMembers();
        connectionManager.startClusterThread();

        // open a new member with a different cluster id
        context.addMember("127.0.0.1", 5703, UUID.randomUUID());

        connectionManager.connectToAllClusterMembers();

        assertEquals(2, connectionManager.getActiveConnections().size());
    }

    @Test
    public void testClusterUuidChange_whenDisconnected() {
        ConnectionManagerTestUtil.ClusterContext context = new ConnectionManagerTestUtil.ClusterContext();
        UUID uuid1 = context.addMember("127.0.0.1", 5701);
        UUID uuid2 = context.addMember("127.0.0.1", 5702);
        AtomicInteger clusterRestartCount = new AtomicInteger();
        context.onClusterConnect = clusterRestartCount::incrementAndGet;

        TcpClientConnectionManager connectionManager = newConnectionManager(new ClientConfig(), context);
        connectionManager.start();
        connectionManager.tryConnectToAllClusterMembers();

        // remove members
        context.removeMember(uuid1);
        context.removeMember(uuid2);
        // open a new member with a different cluster id
        context.addMember("127.0.0.1", 5703, UUID.randomUUID());
        // close connections to the old cluster
        connectionManager.getActiveConnections().
                forEach(connection -> connection.close(null, null));

        connectionManager.consumeTaskQueue();

        assertEquals(1, connectionManager.getActiveConnections().size());
        assertEquals(1, clusterRestartCount.get());
    }
}
