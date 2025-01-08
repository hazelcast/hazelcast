/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.test;

import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.impl.connection.ClientConnection;
import com.hazelcast.client.impl.connection.ClientConnectionManager;
import com.hazelcast.client.impl.spi.EventHandler;
import com.hazelcast.cluster.Address;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.test.HazelcastTestSupport;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ClientTestSupport extends HazelcastTestSupport {

    /**
     * Blocks incoming messages to client from given instance
     */
    protected void blockMessagesFromInstance(HazelcastInstance instance, HazelcastInstance client) {
        HazelcastClientInstanceImpl clientImpl = getHazelcastClientInstanceImpl(client);
        ClientConnectionManager connectionManager = clientImpl.getConnectionManager();
        Address address = instance.getCluster().getLocalMember().getAddress();
        ((TestClientRegistry.MockTcpClientConnectionManager) connectionManager).blockFrom(address);
    }

    /**
     * Unblocks incoming messages to client from given instance
     */
    protected void unblockMessagesFromInstance(HazelcastInstance instance, HazelcastInstance client) {
        HazelcastClientInstanceImpl clientImpl = getHazelcastClientInstanceImpl(client);
        ClientConnectionManager connectionManager = clientImpl.getConnectionManager();
        Address address = instance.getCluster().getLocalMember().getAddress();
        ((TestClientRegistry.MockTcpClientConnectionManager) connectionManager).unblockFrom(address);
    }

    /**
     * Blocks outgoing messages from client to given instance
     */
    protected void blockMessagesToInstance(HazelcastInstance instance, HazelcastInstance client) {
        HazelcastClientInstanceImpl clientImpl = getHazelcastClientInstanceImpl(client);
        ClientConnectionManager connectionManager = clientImpl.getConnectionManager();
        Address address = instance.getCluster().getLocalMember().getAddress();
        ((TestClientRegistry.MockTcpClientConnectionManager) connectionManager).blockTo(address);
    }

    /**
     * Unblocks outgoing messages from client to given instance
     */
    protected void unblockMessagesToInstance(HazelcastInstance instance, HazelcastInstance client) {
        HazelcastClientInstanceImpl clientImpl = getHazelcastClientInstanceImpl(client);
        ClientConnectionManager connectionManager = clientImpl.getConnectionManager();
        Address address = instance.getCluster().getLocalMember().getAddress();
        ((TestClientRegistry.MockTcpClientConnectionManager) connectionManager).unblockTo(address);
    }

    protected static HazelcastClientInstanceImpl getHazelcastClientInstanceImpl(HazelcastInstance client) {
        if (client instanceof HazelcastClientInstanceImpl impl) {
            return impl;
        }
        HazelcastClientProxy clientProxy = (HazelcastClientProxy) client;
        return clientProxy.client;
    }

    public static void makeSureDisconnectedFromServer(final HazelcastInstance client, UUID memberUUID) {
        assertTrueEventually(() -> {
            ClientConnectionManager connectionManager = getHazelcastClientInstanceImpl(client).getConnectionManager();
            assertNull(connectionManager.getActiveConnection(memberUUID));
        });
    }

    public static void makeSureConnectedToServers(final HazelcastInstance client,
                                               final int expectedConnectedServerCount) {
        assertTrueEventually(() -> {
            assert client != null;
            HazelcastClientInstanceImpl clientInstanceImpl = getHazelcastClientInstanceImpl(client);
            ClientConnectionManager connectionManager = clientInstanceImpl.getConnectionManager();
            Collection<ClientConnection> activeConnections = connectionManager.getActiveConnections();
            assertEquals(activeConnections.toString(), expectedConnectedServerCount, activeConnections.size());
        });
    }

    protected Map<Long, EventHandler> getAllEventHandlers(HazelcastInstance client) {
        ClientConnectionManager connectionManager = getHazelcastClientInstanceImpl(client).getConnectionManager();
        Collection<ClientConnection> activeConnections = connectionManager.getActiveConnections();
        HashMap<Long, EventHandler> map = new HashMap<>();
        for (ClientConnection activeConnection : activeConnections) {
            map.putAll(activeConnection.getEventHandlers());
        }
        return map;
    }

    public static class ReconnectListener implements LifecycleListener {

        public final CountDownLatch disconnectedLatch = new CountDownLatch(1);
        public final CountDownLatch reconnectedLatch = new CountDownLatch(1);
        private final AtomicBoolean disconnected = new AtomicBoolean();

        @Override
        public void stateChanged(LifecycleEvent event) {
            LifecycleEvent.LifecycleState state = event.getState();
            if (state == LifecycleEvent.LifecycleState.CLIENT_DISCONNECTED) {
                disconnected.set(true);
                disconnectedLatch.countDown();
            } else if (state == LifecycleEvent.LifecycleState.CLIENT_CONNECTED) {
                if (disconnected.get()) {
                    reconnectedLatch.countDown();
                }
            }
        }
    }
}
