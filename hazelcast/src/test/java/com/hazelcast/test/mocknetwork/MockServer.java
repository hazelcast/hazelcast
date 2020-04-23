/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.util.concurrent.ThreadFactoryImpl;
import com.hazelcast.internal.util.executor.StripedRunnable;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.executionservice.ExecutionService;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
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

    private static final int RETRY_NUMBER = 5;
    private static final int DELAY_FACTOR = 100;

    private final ConcurrentMap<Address, MockServerConnection> connectionMap = new ConcurrentHashMap<>(10);
    private final TestNodeRegistry nodeRegistry;
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
        this.connectionManager = new MockServerConnectionManager(this);
        this.scheduler = new ScheduledThreadPoolExecutor(4,
                new ThreadFactoryImpl(createThreadPoolName(serverContext.getHazelcastName(), "MockConnectionManager")));
        this.logger = serverContext.getLoggingService().getLogger(MockServer.class);
    }

    static class MockServerConnectionManager
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
        public MockServerConnection get(Address address) {
            return server.connectionMap.get(address);
        }

        @Override
        public MockServerConnection getOrConnect(Address address, boolean silent) {
            MockServerConnection conn = server.connectionMap.get(address);
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

            return createConnection(targetNode);
        }

        @Override
        public void accept(Packet packet) {
        }

        private void suspectAddress(final Address address) {
            // see ServerContext#removeEndpoint()
            server.node.getNodeEngine().getExecutionService().execute(ExecutionService.IO_EXECUTOR,
                    () -> server.node.getClusterService().suspectAddressIfNotConnected(address));
        }

        public static boolean isTargetLeft(Node targetNode) {
            return !targetNode.isRunning() && !targetNode.getClusterService().isJoined();
        }

        private synchronized MockServerConnection createConnection(Node targetNode) {
            if (!server.live) {
                throw new IllegalStateException("connection manager is not live!");
            }

            Node node = server.node;
            Address local = node.getThisAddress();
            Address remote = targetNode.getThisAddress();

            MockServerConnection thisConnection = new MockServerConnection(lifecycleListener, remote, local,
                    node.getNodeEngine(), targetNode.getServer().getConnectionManager(EndpointQualifier.MEMBER));

            MockServerConnection remoteConnection = new MockServerConnection(lifecycleListener, local, remote,
                    targetNode.getNodeEngine(), node.getServer().getConnectionManager(EndpointQualifier.MEMBER));

            remoteConnection.localConnection = thisConnection;
            thisConnection.localConnection = remoteConnection;

            if (!remoteConnection.isAlive()) {
                // targetNode is not alive anymore.
                suspectAddress(remote);
                return null;
            }

            server.connectionMap.put(remote, remoteConnection);
            server.logger.info("Created connection to endpoint: " + remote + ", connection: " + remoteConnection);

            if (!remoteConnection.isAlive()) {
                // If connection is not alive after inserting it into connection map,
                // that means remote node is being stopping during connection creation.
                suspectAddress(remote);
            }
            return remoteConnection;
        }

        @Override
        public synchronized boolean register(final Address remoteAddress, final ServerConnection c) {
            MockServerConnection connection = (MockServerConnection) c;
            if (!server.live) {
                throw new IllegalStateException("connection manager is not live!");
            }
            if (!connection.isAlive()) {
                return false;
            }

            connection.setLifecycleListener(lifecycleListener);
            server.connectionMap.put(remoteAddress, connection);
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

        private void fireConnectionRemovedEvent(final MockServerConnection connection, final Address endPoint) {
            if (server.live) {
                server.serverContext.getEventService().executeEventCallback(new StripedRunnable() {
                    @Override
                    public void run() {
                        connectionListeners.forEach(listener -> listener.connectionRemoved(connection));
                    }

                    @Override
                    public int getKey() {
                        return endPoint.hashCode();
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

        @Override
        public boolean transmit(Packet packet, ServerConnection connection) {
            return connection != null && connection.write(packet);
        }

        /**
         * Retries sending packet maximum 5 times until connection to target becomes available.
         */
        @Override
        public boolean transmit(Packet packet, Address target) {
            return send(packet, target, null);
        }

        private boolean send(Packet packet, Address target, SendTask sendTask) {
            MockServerConnection connection = get(target);
            if (connection != null) {
                return transmit(packet, connection);
            }

            if (sendTask == null) {
                sendTask = new SendTask(packet, target);
            }

            int retries = sendTask.retries.get();
            if (retries < RETRY_NUMBER && server.serverContext.isNodeActive()) {
                getOrConnect(target, true);
                // TODO: Caution: may break the order guarantee of the packets sent from the same thread!
                try {
                    server.scheduler.schedule(sendTask, (retries + 1) * DELAY_FACTOR, TimeUnit.MILLISECONDS);
                } catch (RejectedExecutionException e) {
                    if (server.live) {
                        throw e;
                    }
                    if (server.logger.isFinestEnabled()) {
                        server.logger.finest("Packet send task is rejected. Packet cannot be sent to " + target);
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
                final Address endPoint = connection.getRemoteAddress();
                if (!server.connectionMap.remove(endPoint, connection)) {
                    return;
                }

                Server server = connection.remoteNodeEngine.getNode().getServer();
                // all mock implementations of networking service ignore the provided endpoint qualifier
                // so we pass in null. Once they are changed to use the parameter, we should be notified
                // and this parameter can be changed
                Connection remoteConnection = server.getConnectionManager(null)
                        .get(connection.localAddress);
                if (remoteConnection != null) {
                    remoteConnection.close("Connection closed by the other side", null);
                }

                MockServerConnectionManager.this.server.logger.info("Removed connection to endpoint: " + endPoint + ", connection: " + connection);
                fireConnectionRemovedEvent(connection, endPoint);
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

        private static class MockNetworkStats implements NetworkStats {

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

    @Override
    public synchronized void shutdown() {
        stop();
        scheduler.shutdownNow();
    }
}
