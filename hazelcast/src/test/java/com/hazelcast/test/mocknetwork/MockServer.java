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

package com.hazelcast.test.mocknetwork;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.instance.impl.NodeState;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.ConnectionLifecycleListener;
import com.hazelcast.internal.nio.ConnectionListener;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.server.NetworkStats;
import com.hazelcast.internal.server.Server;
import com.hazelcast.internal.server.ServerConnection;
import com.hazelcast.internal.server.ServerConnectionManager;
import com.hazelcast.internal.server.ServerContext;
import com.hazelcast.internal.server.tcp.LinkedAddresses;
import com.hazelcast.internal.server.tcp.LocalAddressRegistry;
import com.hazelcast.internal.util.concurrent.ThreadFactoryImpl;
import com.hazelcast.internal.util.executor.StripedRunnable;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.executionservice.ExecutionService;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import static com.hazelcast.internal.util.ThreadUtil.createThreadPoolName;
import static com.hazelcast.test.HazelcastTestSupport.suspectMember;
import static java.util.Collections.singletonMap;

class MockServer implements Server {

    final ConcurrentMap<UUID, MockServerConnection> connectionMap = new ConcurrentHashMap<>(10);
    private static final int RETRY_NUMBER = 5;
    private static final int DELAY_FACTOR = 100;

    private final TestNodeRegistry nodeRegistry;
    private final LocalAddressRegistry addressRegistry;
    private final Node node;
    private final ScheduledExecutorService scheduler;
    private final ServerContext serverContext;
    private final ILogger logger;
    private final ServerConnectionManager connectionManager;

    private volatile boolean live;

    MockServer(ServerContext serverContext, Node node, TestNodeRegistry testNodeRegistry) {
        this.serverContext = serverContext;
        this.nodeRegistry = testNodeRegistry;
        this.node = node;
        this.addressRegistry = node.getLocalAddressRegistry();
        this.connectionManager = new MockServerConnectionManager(this);
        this.scheduler = new ScheduledThreadPoolExecutor(4,
                new ThreadFactoryImpl(createThreadPoolName(serverContext.getHazelcastName(), "MockConnectionManager")));
        this.logger = serverContext.getLoggingService().getLogger(MockServer.class);
    }

    class MockServerConnectionManager
            implements ServerConnectionManager {

        private final MockServer server;
        private final ConnectionLifecycleListener lifecycleListener = new MockServerConnectionManager.MockConnLifecycleListener();
        private final Set<ConnectionListener> connectionListeners = new CopyOnWriteArraySet<>();

        MockServerConnectionManager(MockServer server) {
            this.server = server;
        }

        @Override
        public Server getServer() {
            return server;
        }

        @Override
        public ServerConnection get(@Nonnull Address address, int streamId) {
            UUID memberUuid = server.nodeRegistry.uuidOf(address);
            return memberUuid != null ? get(memberUuid) : null;
        }

        public MockServerConnection get(UUID memberUuid) {
            return server.connectionMap.get(memberUuid);
        }

        @Override
        @Nonnull
        public List<ServerConnection> getAllConnections(@Nonnull Address address) {
            UUID memberUuid = server.nodeRegistry.uuidOf(address);
            if (memberUuid == null) {
                return Collections.emptyList();
            }
            ServerConnection conn = get(memberUuid);
            return conn != null ? Collections.singletonList(conn) : Collections.emptyList();
        }

        @Override
        public MockServerConnection getOrConnect(@Nonnull Address address, int stream) {
            UUID uuid = server.nodeRegistry.uuidOf(address);
            MockServerConnection conn = null;
            if (uuid != null) {
                conn = server.connectionMap.get(uuid);
            }
            if (conn != null && conn.isAlive()) {
                return conn;
            }
            if (!server.live) {
                return null;
            }
            Node targetNode = server.nodeRegistry.getNode(address);
            if (targetNode == null || isTargetLeft(targetNode)) {
                suspectAddress(address);
                return null;
            }

            return getOrCreateConnection(targetNode);
        }

        @Override
        public void accept(Packet packet) {
        }

        private void suspectAddress(Address endpointAddress) {
            // see ServerContext#removeEndpoint()
            server.node.getNodeEngine().getExecutionService().execute(ExecutionService.IO_EXECUTOR,
                    () -> server.node.getClusterService().suspectAddressIfNotConnected(endpointAddress));
        }

        private synchronized MockServerConnection getOrCreateConnection(Node targetNode) {
            if (!server.live) {
                throw new IllegalStateException("connection manager is not live!");
            }

            Node node = server.node;
            Address localAddress = node.getThisAddress();
            Address remoteAddress = targetNode.getThisAddress();

            UUID localMemberUuid = node.getThisUuid();
            UUID remoteMemberUuid = targetNode.getThisUuid();

            // To avoid duplicate connection creation, we need to check available connections again
            MockServerConnection conn = server.connectionMap.get(remoteMemberUuid);
            if (conn != null && conn.isAlive()) {
                return conn;
            }

            // Create a unidirectional connection that is split into
            // two distinct connection objects (one for the local member
            // side and the other for the remote member side)
            // These two connections below are only used for sending
            // packets from the local member to the remote member.

            // this connection is only used to send packets to remote member
            MockServerConnection connectionFromLocalToRemote = new MockServerConnection(
                    lifecycleListener,
                    localAddress,
                    remoteAddress,
                    localMemberUuid,
                    remoteMemberUuid,
                    node.getNodeEngine(),
                    targetNode.getNodeEngine(),
                    node.getServer().getConnectionManager(EndpointQualifier.MEMBER)
            );

            // This connection is only used to receive packets on the remote member
            // which are sent from the local member's connection created above.
            // Since this connection is not registered in the connection map of remote
            // member's connection server, when the remote member intends to send a
            // packet to this local member, it won't have access to this connection,
            // and then it will create a new pair of connections. This means that a
            // bidirectional connection consists of 4 MockServerConnections.
            MockServerConnection connectionFromRemoteToLocal = new MockServerConnection(
                    lifecycleListener,
                    remoteAddress,
                    localAddress,
                    remoteMemberUuid,
                    localMemberUuid,
                    targetNode.getNodeEngine(),
                    node.getNodeEngine(),
                    targetNode.getServer().getConnectionManager(EndpointQualifier.MEMBER)
            );

            connectionFromRemoteToLocal.otherConnection = connectionFromLocalToRemote;
            connectionFromLocalToRemote.otherConnection = connectionFromRemoteToLocal;

            if (!connectionFromRemoteToLocal.isAlive()) {
                // targetNode is not alive anymore.
                suspectAddress(remoteAddress);
                return null;
            }

            addressRegistry.register(remoteMemberUuid, LinkedAddresses.getResolvedAddresses(remoteAddress));
            server.connectionMap.put(remoteMemberUuid, connectionFromLocalToRemote);
            server.logger.info("Created connection to endpoint: " + remoteAddress + "-" + remoteMemberUuid + ", connection: "
                    + connectionFromLocalToRemote);

            if (!connectionFromLocalToRemote.isAlive()) {
                // If connection is not alive after inserting it into connection map,
                // that means remote node is being stopping during connection creation.
                suspectAddress(remoteAddress);
            }
            return connectionFromLocalToRemote;
        }

        @Override
        public MockServerConnection getOrConnect(@Nonnull Address address, boolean silent, int stream) {
            return getOrConnect(address, stream);
        }

        @Override
        public synchronized boolean register(
                Address remoteAddress,
                Address targetAddress,
                Collection<Address> remoteAddressAliases,
                UUID remoteUuid,
                ServerConnection c,
                int streamId
        ) {
            MockServerConnection connection = (MockServerConnection) c;
            if (!server.live) {
                throw new IllegalStateException("connection manager is not live!");
            }
            if (!connection.isAlive()) {
                return false;
            }
            connection.setRemoteUuid(remoteUuid);
            connection.setLifecycleListener(lifecycleListener);
            server.connectionMap.put(remoteUuid, connection);
            LinkedAddresses addressesToRegister = LinkedAddresses.getResolvedAddresses(remoteAddress);
            if (targetAddress != null) {
                addressesToRegister.addAllResolvedAddresses(targetAddress);
            }
            if (remoteAddressAliases != null) {
                for (Address remoteAddressAlias : remoteAddressAliases) {
                    addressesToRegister.addAllResolvedAddresses(remoteAddressAlias);
                }
            }
            addressRegistry.register(remoteUuid, addressesToRegister);

            server.serverContext.getEventService().executeEventCallback(new StripedRunnable() {
                @Override
                public void run() {
                    for (ConnectionListener listener : connectionListeners) {
                        listener.connectionAdded(connection);
                    }
                }

                @Override
                public int getKey() {
                    return remoteAddress.hashCode();
                }
            });
            return true;
        }

        @Override
        public void addConnectionListener(ConnectionListener connectionListener) {
            connectionListeners.add(connectionListener);
        }

        private void fireConnectionRemovedEvent(final MockServerConnection connection, UUID endpointUuid) {
            if (server.live) {
                server.serverContext.getEventService().executeEventCallback(new StripedRunnable() {
                    @Override
                    public void run() {
                        connectionListeners.forEach(listener -> listener.connectionRemoved(connection));
                    }

                    @Override
                    public int getKey() {
                        return endpointUuid.hashCode();
                    }
                });
            }
        }

        @Override
        public @Nonnull Collection getConnections() {
            return server.connectionMap.values();
        }

        @Override
        public int connectionCount(Predicate<ServerConnection> predicate) {
            return (int) server.connectionMap.values().stream().filter(predicate).count();
        }

        /**
         * Retries sending packet maximum 5 times until connection to target becomes available.
         */
        @Override
        public boolean transmit(Packet packet, Address targetAddress, int streamId) {
            return send(packet, targetAddress, null);
        }

        private boolean send(Packet packet, Address targetAddress, SendTask sendTask) {
            UUID targetUuid = server.nodeRegistry.uuidOf(targetAddress);
            MockServerConnection connection = null;
            if (targetUuid != null) {
                connection = get(targetUuid);
            }
            if (connection != null) {
                return connection.write(packet);
            }

            if (sendTask == null) {
                sendTask = new SendTask(packet, targetAddress);
            }

            int retries = sendTask.retries.get();
            if (retries < RETRY_NUMBER && server.serverContext.isNodeActive()) {
                getOrConnect(targetAddress, true);
                // TODO: Caution: may break the order guarantee of the packets sent from the same thread!
                try {
                    server.scheduler.schedule(sendTask, (retries + 1) * DELAY_FACTOR, TimeUnit.MILLISECONDS);
                } catch (RejectedExecutionException e) {
                    if (server.live) {
                        throw e;
                    }
                    if (server.logger.isFinestEnabled()) {
                        server.logger.finest("Packet send task is rejected. Packet cannot be sent to " + targetUuid);
                    }
                }
                return true;
            }
            return false;
        }

        @Override
        public NetworkStats getNetworkStats() {
            return new MockNetworkStats();
        }

        private class MockConnLifecycleListener
                implements ConnectionLifecycleListener<MockServerConnection> {

            @Override
            public void onConnectionClose(MockServerConnection connection, Throwable t, boolean silent) {
                Address endpointAddress = connection.getRemoteAddress();
                UUID endpointUuid = connection.getRemoteUuid();
                assert endpointUuid != null;
                if (!server.connectionMap.remove(endpointUuid, connection)) {
                    return;
                }
                addressRegistry.tryRemoveRegistration(endpointUuid, endpointAddress);

                Server remoteServer = connection.remoteNodeEngine.getNode().getServer();
                // all mock implementations of networking service ignore the provided endpoint qualifier
                // so we pass in null. Once they are changed to use the parameter, we should be notified
                // and this parameter can be changed
                Connection remoteConnection = remoteServer.getConnectionManager(null)
                        .get(connection.localAddress, 0);
                if (remoteConnection != null) {
                    remoteConnection.close("Connection closed by the other side", null);
                }

                MockServerConnectionManager.this.server.logger.info("Removed connection to endpoint: [address="
                        + endpointAddress + ", uuid=" + endpointUuid + "], connection: " + connection);
                fireConnectionRemovedEvent(connection, endpointUuid);
            }

        }

        private final class SendTask implements Runnable {

            private final AtomicInteger retries = new AtomicInteger();

            private final Packet packet;
            private final Address target;

            private SendTask(Packet packet, Address target) {
                this.packet = packet;
                this.target = target;
            }

            @Override
            public void run() {
                int actualRetries = retries.incrementAndGet();
                if (server.logger.isFinestEnabled()) {
                    server.logger.finest("Retrying[" + actualRetries + "] packet send operation to: " + target);
                }
                send(packet, target, this);
            }
        }

        private class MockNetworkStats implements NetworkStats {

            @Override
            public long getBytesReceived() {
                return 0;
            }

            @Override
            public long getBytesSent() {
                return 0;
            }
        }
    }

    public LocalAddressRegistry getAddressRegistry() {
        return addressRegistry;
    }

    @Override
    public ServerContext getContext() {
        return serverContext;
    }

    @Override
    public @Nonnull Collection<ServerConnection> getConnections() {
        return connectionManager.getConnections();
    }

    @Override
    public Map<EndpointQualifier, NetworkStats> getNetworkStats() {
        return singletonMap(EndpointQualifier.MEMBER, connectionManager.getNetworkStats());
    }

    @Override
    public void addConnectionListener(ConnectionListener<ServerConnection> listener) {
        connectionManager.addConnectionListener(listener);
    }

    @Override
    public ServerConnectionManager getConnectionManager(EndpointQualifier qualifier) {
        return connectionManager;
    }

    @Override
    public boolean isLive() {
        return live;
    }

    @Override
    public synchronized void start() {
        logger.fine("Starting connection manager");
        live = true;
    }

    @Override
    public synchronized void stop() {
        if (!live) {
            return;
        }
        logger.fine("Stopping connection manager");
        live = false;

        connectionMap.values().forEach(connection -> connection.close(null, null));
        connectionMap.clear();

        final Member localMember = node.getLocalMember();
        final Address thisAddress = localMember.getAddress();

        for (Address address : nodeRegistry.getAddresses()) {
            if (address.equals(thisAddress)) {
                continue;
            }

            Node otherNode = nodeRegistry.getNode(address);
            if (otherNode != null && otherNode.getState() != NodeState.SHUT_DOWN) {
                logger.fine(otherNode.getThisAddress() + " is instructed to suspect from " + thisAddress);
                try {
                    suspectMember(otherNode, node, "Connection manager is stopped on " + localMember);
                } catch (Throwable e) {
                    ILogger otherLogger = otherNode.getLogger(MockServer.class);
                    otherLogger.warning("While removing " + thisAddress, e);
                }
            }
        }
    }

    public static boolean isTargetLeft(Node targetNode) {
        return !targetNode.isRunning() && !targetNode.getClusterService().isJoined();
    }

    @Override
    public synchronized void shutdown() {
        stop();
        scheduler.shutdownNow();
    }
}
