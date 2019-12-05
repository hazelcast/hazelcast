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

package com.hazelcast.test.mocknetwork;

import com.hazelcast.cluster.Member;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.instance.impl.NodeState;
import com.hazelcast.internal.networking.NetworkStats;
import com.hazelcast.internal.networking.Networking;
import com.hazelcast.internal.util.concurrent.ThreadFactoryImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.nio.AggregateEndpointManager;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.ConnectionLifecycleListener;
import com.hazelcast.internal.nio.ConnectionListener;
import com.hazelcast.internal.nio.DefaultAggregateEndpointManager;
import com.hazelcast.internal.nio.EndpointManager;
import com.hazelcast.internal.nio.IOService;
import com.hazelcast.internal.nio.NetworkingService;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.internal.util.executor.StripedRunnable;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.instance.EndpointQualifier.MEMBER;
import static com.hazelcast.test.HazelcastTestSupport.suspectMember;
import static com.hazelcast.internal.util.ThreadUtil.createThreadPoolName;
import static java.util.Collections.singletonMap;

class MockNetworkingService
        implements NetworkingService {

    private static final int RETRY_NUMBER = 5;
    private static final int DELAY_FACTOR = 100;

    private final ConcurrentMap<Address, MockConnection> mapConnections
            = new ConcurrentHashMap<Address, MockConnection>(10);
    private final TestNodeRegistry nodeRegistry;
    private final Node node;

    private final ScheduledExecutorService scheduler;
    private final IOService ioService;
    private final ILogger logger;

    private volatile boolean live;

    private final EndpointManager mockEndpointMgr;
    private final AggregateEndpointManager mockAggrEndpointManager;

    MockNetworkingService(IOService ioService, Node node, TestNodeRegistry testNodeRegistry) {
        this.ioService = ioService;
        this.nodeRegistry = testNodeRegistry;
        this.node = node;
        this.mockEndpointMgr = new MockEndpointManager(this);
        this.mockAggrEndpointManager = new DefaultAggregateEndpointManager(
                new ConcurrentHashMap(singletonMap(MEMBER, mockEndpointMgr)));
        this.scheduler = new ScheduledThreadPoolExecutor(4,
                new ThreadFactoryImpl(createThreadPoolName(ioService.getHazelcastName(), "MockConnectionManager")));
        this.logger = ioService.getLoggingService().getLogger(MockNetworkingService.class);
    }



    static class MockEndpointManager
            implements EndpointManager<MockConnection> {

        private final MockNetworkingService ns;
        private final ConnectionLifecycleListener lifecycleListener = new MockEndpointManager.MockConnLifecycleListener();
        private final Set<ConnectionListener> connectionListeners = new CopyOnWriteArraySet<ConnectionListener>();

        MockEndpointManager(MockNetworkingService ns) {
            this.ns = ns;
        }

        @Override
        public MockConnection getConnection(Address address) {
            return ns.mapConnections.get(address);
        }

        @Override
        public MockConnection getOrConnect(Address address) {
            MockConnection conn = ns.mapConnections.get(address);
            if (conn != null && conn.isAlive()) {
                return conn;
            }
            if (!ns.live) {
                return null;
            }

            Node targetNode = ns.nodeRegistry.getNode(address);
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
            // see NodeIOService#removeEndpoint()
            ns.node.getNodeEngine().getExecutionService().execute(ExecutionService.IO_EXECUTOR, new Runnable() {
                @Override
                public void run() {
                    ns.node.getClusterService().suspectAddressIfNotConnected(address);
                }
            });
        }

        public static boolean isTargetLeft(Node targetNode) {
            return !targetNode.isRunning() && !targetNode.getClusterService().isJoined();
        }

        private synchronized MockConnection createConnection(Node targetNode) {
            if (!ns.live) {
                throw new IllegalStateException("connection manager is not live!");
            }

            Node node = ns.node;
            Address local = node.getThisAddress();
            Address remote = targetNode.getThisAddress();

            MockConnection thisConnection = new MockConnection(lifecycleListener, remote, local,
                    node.getNodeEngine(), targetNode.getEndpointManager());

            MockConnection remoteConnection = new MockConnection(lifecycleListener, local, remote,
                    targetNode.getNodeEngine(), node.getEndpointManager());

            remoteConnection.localConnection = thisConnection;
            thisConnection.localConnection = remoteConnection;

            if (!remoteConnection.isAlive()) {
                // targetNode is not alive anymore.
                suspectAddress(remote);
                return null;
            }

            ns.mapConnections.put(remote, remoteConnection);
            ns.logger.info("Created connection to endpoint: " + remote + ", connection: " + remoteConnection);

            if (!remoteConnection.isAlive()) {
                // If connection is not alive after inserting it into connection map,
                // that means remote node is being stopping during connection creation.
                suspectAddress(remote);
            }
            return remoteConnection;
        }

        @Override
        public MockConnection getOrConnect(Address address, boolean silent) {
            return getOrConnect(address);
        }

        @Override
        public synchronized boolean registerConnection(final Address remoteEndpoint, final MockConnection connection) {
            if (!ns.live) {
                throw new IllegalStateException("connection manager is not live!");
            }
            if (!connection.isAlive()) {
                return false;
            }

            connection.setLifecycleListener(lifecycleListener);
            ns.mapConnections.put(remoteEndpoint, connection);
            ns.ioService.getEventService().executeEventCallback(new StripedRunnable() {
                @Override
                public void run() {
                    for (ConnectionListener listener : connectionListeners) {
                        listener.connectionAdded(connection);
                    }
                }

                @Override
                public int getKey() {
                    return remoteEndpoint.hashCode();
                }
            });
            return true;
        }

        @Override
        public void addConnectionListener(ConnectionListener connectionListener) {
            connectionListeners.add(connectionListener);
        }

        private void fireConnectionRemovedEvent(final MockConnection connection, final Address endPoint) {
            if (ns.live) {
                ns.ioService.getEventService().executeEventCallback(new StripedRunnable() {
                    @Override
                    public void run() {
                        for (ConnectionListener listener : connectionListeners) {
                            listener.connectionRemoved(connection);
                        }
                    }

                    @Override
                    public int getKey() {
                        return endPoint.hashCode();
                    }
                });
            }
        }

        @Override
        public Collection getConnections() {
            return ns.mapConnections.values();
        }

        @Override
        public Collection getActiveConnections() {
            return ns.mapConnections.values();
        }

        @Override
        public boolean transmit(Packet packet, MockConnection connection) {
            return (connection != null && connection.write(packet));
        }

        /**
         * Retries sending packet maximum 5 times until connection to target becomes available.
         */
        @Override
        public boolean transmit(Packet packet, Address target) {
            return send(packet, target, null);
        }

        private boolean send(Packet packet, Address target, SendTask sendTask) {
            MockConnection connection = getConnection(target);
            if (connection != null) {
                return transmit(packet, connection);
            }

            if (sendTask == null) {
                sendTask = new SendTask(packet, target);
            }

            int retries = sendTask.retries.get();
            if (retries < RETRY_NUMBER && ns.ioService.isActive()) {
                getOrConnect(target, true);
                // TODO: Caution: may break the order guarantee of the packets sent from the same thread!
                try {
                    ns.scheduler.schedule(sendTask, (retries + 1) * DELAY_FACTOR, TimeUnit.MILLISECONDS);
                } catch (RejectedExecutionException e) {
                    if (ns.live) {
                        throw e;
                    }
                    if (ns.logger.isFinestEnabled()) {
                        ns.logger.finest("Packet send task is rejected. Packet cannot be sent to " + target);
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
                implements ConnectionLifecycleListener<MockConnection> {

            @Override
            public void onConnectionClose(MockConnection connection, Throwable t, boolean silent) {
                final Address endPoint = connection.getEndPoint();
                if (!ns.mapConnections.remove(endPoint, connection)) {
                    return;
                }

                NetworkingService remoteNetworkingService = connection.remoteNodeEngine.getNode().getNetworkingService();
                // all mock implementations of networking service ignore the provided endpoint qualifier
                // so we pass in null. Once they are changed to use the parameter, we should be notified
                // and this parameter can be changed
                Connection remoteConnection = remoteNetworkingService.getEndpointManager(null)
                                                                     .getConnection(connection.localEndpoint);
                if (remoteConnection != null) {
                    remoteConnection.close("Connection closed by the other side", null);
                }

                ns.logger.info("Removed connection to endpoint: " + endPoint + ", connection: " + connection);
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
                if (ns.logger.isFinestEnabled()) {
                    ns.logger.finest("Retrying[" + actualRetries + "] packet send operation to: " + target);
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
    public IOService getIoService() {
        return ioService;
    }

    @Override
    public AggregateEndpointManager getAggregateEndpointManager() {
        return mockAggrEndpointManager;
    }

    @Override
    public EndpointManager getEndpointManager(EndpointQualifier qualifier) {
        return mockEndpointMgr;
    }

    @Override
    public void scheduleDeferred(Runnable task, long delay, TimeUnit unit) {
        scheduler.schedule(task, delay, unit);
    }

    @Override
    public boolean isLive() {
        return live;
    }

    @Override
    public Networking getNetworking() {
        return null;
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

        for (Connection connection : mapConnections.values()) {
            connection.close(null, null);
        }
        mapConnections.clear();

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
                    ILogger otherLogger = otherNode.getLogger(MockNetworkingService.class);
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
