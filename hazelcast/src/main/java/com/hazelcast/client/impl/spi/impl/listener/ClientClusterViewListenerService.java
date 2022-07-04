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

package com.hazelcast.client.impl.spi.impl.listener;

import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.connection.ClientConnection;
import com.hazelcast.client.impl.connection.ClientConnectionManager;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientAddClusterViewListenerCodec;
import com.hazelcast.client.impl.spi.ClientListenerService;
import com.hazelcast.client.impl.spi.EventHandler;
import com.hazelcast.client.impl.spi.impl.ClientClusterServiceImpl;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.client.impl.spi.impl.ClientPartitionServiceImpl;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.ConnectionListener;
import com.hazelcast.logging.ILogger;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Adds cluster listener to one of the connections. If that connection is removed,
 * it registers connection to any other connection
 */
public class ClientClusterViewListenerService implements ConnectionListener {

    private final HazelcastClientInstanceImpl client;
    private final ClientConnectionManager connectionManager;
    private final ClientPartitionServiceImpl partitionService;
    private final ClientClusterServiceImpl clusterService;
    private final ILogger logger;
    private final AtomicReference<Connection> listenerAddedConnection = new AtomicReference<>();

    public ClientClusterViewListenerService(HazelcastClientInstanceImpl client) {
        this.client = client;
        this.logger = client.getLoggingService().getLogger(ClientListenerService.class);
        this.connectionManager = client.getConnectionManager();
        partitionService = (ClientPartitionServiceImpl) client.getClientPartitionService();
        clusterService = (ClientClusterServiceImpl) client.getClientClusterService();
    }

    public void start() {
        connectionManager.addConnectionListener(this);
    }

    private final class ClusterViewListenerHandler extends ClientAddClusterViewListenerCodec.AbstractEventHandler
            implements EventHandler<ClientMessage> {

        private final ClientConnection connection;

        private ClusterViewListenerHandler(ClientConnection connection) {
            this.connection = connection;
        }

        @Override
        public void beforeListenerRegister(Connection connection) {
            clusterService.onClusterConnect();
            if (logger.isFinestEnabled()) {
                logger.finest("Register attempt of ClusterViewListenerHandler to " + connection);
            }
        }

        @Override
        public void onListenerRegister(Connection connection) {
            if (logger.isFinestEnabled()) {
                logger.finest("Registered ClusterViewListenerHandler to " + connection);
            }
        }

        @Override
        public void handleMembersViewEvent(int memberListVersion, Collection<MemberInfo> memberInfos) {
            clusterService.handleMembersViewEvent(memberListVersion, memberInfos, connection.getClusterUuid());
        }

        @Override
        public void handlePartitionsViewEvent(int version, Collection<Map.Entry<UUID, List<Integer>>> partitions) {
            partitionService.handlePartitionsViewEvent(connection, partitions, version);
        }
    }

    @Override
    public void connectionAdded(Connection connection) {
        tryRegister((ClientConnection) connection);
    }

    @Override
    public void connectionRemoved(Connection connection) {
        tryReregisterToRandomConnection(connection);
    }

    private void tryReregisterToRandomConnection(Connection oldConnection) {
        if (!listenerAddedConnection.compareAndSet(oldConnection, null)) {
            //somebody else already trying to rereigster
            return;
        }
        ClientConnection newConnection = connectionManager.getRandomConnection();
        if (newConnection != null) {
            tryRegister(newConnection);
        }
    }

    private void tryRegister(ClientConnection connection) {
        if (!listenerAddedConnection.compareAndSet(null, connection)) {
            //already registering/registered to another connection
            return;
        }
        ClientMessage clientMessage = ClientAddClusterViewListenerCodec.encodeRequest();
        ClientInvocation invocation = new ClientInvocation(client, clientMessage, null, connection);
        ClusterViewListenerHandler handler = new ClusterViewListenerHandler(connection);
        invocation.setEventHandler(handler);
        handler.beforeListenerRegister(connection);
        invocation.invokeUrgent().whenCompleteAsync((message, throwable) -> {
            if (message != null) {
                handler.onListenerRegister(connection);
                return;
            }
            //completes with exception, listener needs to be reregistered
            tryReregisterToRandomConnection(connection);
        });
    }

}
