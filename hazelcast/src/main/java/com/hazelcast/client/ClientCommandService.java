/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.ClusterServiceImpl;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ConnectionListener;
import com.hazelcast.nio.Protocol;
import com.hazelcast.nio.TcpIpConnection;
import com.hazelcast.nio.protocol.Command;
import com.hazelcast.spi.ClientProtocolService;
import com.hazelcast.spi.Connection;
import com.hazelcast.spi.Invocation;
import com.hazelcast.spi.impl.ResponseHandlerFactory;
import com.hazelcast.util.executor.FastExecutor;
import com.hazelcast.util.executor.PoolExecutorThreadFactory;
import com.hazelcast.util.UuidUtil;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

public class ClientCommandService implements ConnectionListener {

    private final Node node;
    private final ILogger logger;
    private final Map<TcpIpConnection, ClientEndpoint> mapClientEndpoints = new ConcurrentHashMap<TcpIpConnection, ClientEndpoint>();
    private final ConcurrentHashMap<Command, ClientCommandHandler> services;
    private final ClientCommandHandler unknownCommandHandler;
    private final FastExecutor executor;
    private volatile boolean started = false;

    public ClientCommandService(Node node) {
        this.node = node;
        logger = node.getLogger(ClientCommandService.class.getName());
        final String poolNamePrefix = node.getThreadPoolNamePrefix("client");
        executor = new FastExecutor(3, 100, 1 << 16, 250L, poolNamePrefix,
                new PoolExecutorThreadFactory(node.threadGroup, poolNamePrefix, node.getConfig().getClassLoader()),
                TimeUnit.SECONDS.toMillis(30), true, false);
        node.getConnectionManager().addConnectionListener(this);
        services = new ConcurrentHashMap<Command, ClientCommandHandler>();
        unknownCommandHandler = new ClientCommandHandler() {
            @Override
            public Protocol processCall(Node node, Protocol protocol) {
                return protocol.error(null, "unknown_command");
            }
        };
    }

    // Always called by an io-thread.
    public void handle(final Protocol protocol) {
        final ClientEndpoint clientEndpoint = getClientEndpoint(protocol.conn);
        if (!clientEndpoint.isAuthenticated() && !Command.AUTH.equals(protocol.command)) {
            checkAuth(protocol.conn);
            return;
        }
        ClientRequestHandler clientRequestHandler = new ClientRequestHandler(node, clientEndpoint, protocol);
        executor.execute(clientRequestHandler);
    }

    public ClientEndpoint getClientEndpoint(TcpIpConnection conn) {
        ClientEndpoint clientEndpoint = mapClientEndpoints.get(conn);
        if (clientEndpoint == null) {
            clientEndpoint = new ClientEndpoint(node, conn, UuidUtil.createClientUuid(conn.getEndPoint()));
            mapClientEndpoints.put(conn, clientEndpoint);
            if (!started) {
                executor.start(); // calling multiple times has no effect.
                started = true;
            }
        }
        return clientEndpoint;
    }

    private void checkAuth(TcpIpConnection conn) {
        logger.log(Level.SEVERE, "A Client " + conn + " must authenticate before any operation.");
        node.clientCommandService.removeClientEndpoint(conn);
        if (conn != null)
            conn.close();
    }

    public void removeClientEndpoint(TcpIpConnection conn) {
        mapClientEndpoints.remove(conn);
    }

    public void register(ClientProtocolService service) {
        final Map<Command, ClientCommandHandler> commandMap = service.getCommandsAsMap();
        if (commandMap != null && !commandMap.isEmpty()) {
            services.putAll(commandMap);
        }
    }

    public ClientCommandHandler getService(Protocol protocol) {
        ClientCommandHandler handler = services.get(protocol.command);
        return (handler == null) ? unknownCommandHandler : handler;
    }

    public void shutdown() {
        executor.shutdown();
        mapClientEndpoints.clear();
        services.clear();
    }

    public void connectionAdded(Connection connection) {
    }

    public void connectionRemoved(Connection connection) {
        if (connection.isClient() && connection instanceof TcpIpConnection) {
            final ClientEndpoint clientEndpoint = mapClientEndpoints.remove(connection);
            if (clientEndpoint != null) {
                final Collection<MemberImpl> memberList = node.nodeEngine.getClusterService().getMemberList();
                for (MemberImpl member : memberList) {
                    if (member.localMember()) {
                        final ClientDisconnectionOperation op = new ClientDisconnectionOperation(clientEndpoint.uuid);
                        op.setNodeEngine(node.nodeEngine).setResponseHandler(ResponseHandlerFactory.createEmptyResponseHandler());
                        node.nodeEngine.getOperationService().executeOperation(op);
                    } else {
                        final Invocation inv = node.nodeEngine.getOperationService()
                                .createInvocationBuilder(ClusterServiceImpl.SERVICE_NAME,
                                        new ClientDisconnectionOperation(clientEndpoint.uuid), member.getAddress()).build();
                        inv.invoke();
                    }
                }
            }
        }
    }
}
