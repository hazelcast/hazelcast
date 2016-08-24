/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
 *
 */

package com.hazelcast.test.mocknetwork;

import com.hazelcast.core.Member;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeState;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionListener;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.nio.IOService;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.util.executor.StripedRunnable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class MockConnectionManager implements ConnectionManager {

    private static final int RETRY_NUMBER = 5;
    private static final int DELAY_FACTOR = 100;

    private final ConcurrentMap<Address, Connection> mapConnections = new ConcurrentHashMap<Address, Connection>(10);
    private final TestNodeRegistry registry;
    private final Node node;

    private final Set<ConnectionListener> connectionListeners = new CopyOnWriteArraySet<ConnectionListener>();
    private final ScheduledExecutorService scheduler = new ScheduledThreadPoolExecutor(4);
    private final IOService ioService;
    private final ILogger logger;

    private volatile boolean live;

    public MockConnectionManager(IOService ioService, Node node, TestNodeRegistry registry) {
        this.ioService = ioService;
        this.registry = registry;
        this.node = node;
        this.logger = ioService.getLogger(MockConnectionManager.class.getName());
    }

    @Override
    public Connection getConnection(Address address) {
        return mapConnections.get(address);
    }

    @Override
    public Connection getOrConnect(Address address) {
        Connection conn = mapConnections.get(address);
        if (live && (conn == null || !conn.isAlive())) {
            Node otherNode = registry.getNode(address);
            if (otherNode != null && otherNode.getState() != NodeState.SHUT_DOWN) {
                MockConnection thisConnection = new MockConnection(address, node.getThisAddress(), node.getNodeEngine());
                MockConnection mockConn = new MockConnection(node.getThisAddress(), address, otherNode.getNodeEngine());
                mockConn.localConnection = thisConnection;
                thisConnection.localConnection = mockConn;
                mapConnections.put(address, mockConn);
                logger.info("Created connection to endpoint: " + address + ", connection: " + mockConn);
                return mockConn;
            }
        }
        return conn;
    }

    @Override
    public Connection getOrConnect(Address address, boolean silent) {
        return getOrConnect(address);
    }

    @Override
    public void start() {
        logger.fine("Starting connection manager");
        live = true;
    }

    @Override
    public void stop() {
        logger.fine("Stopping connection manager");
        live = false;

        final Member localMember = node.getLocalMember();
        final Address thisAddress = localMember.getAddress();

        for (Address address : registry.getAddresses()) {
            if (address.equals(thisAddress)) {
                continue;
            }

            final Node otherNode = registry.getNode(address);
            if (otherNode != null && otherNode.getState() != NodeState.SHUT_DOWN) {
                final ClusterServiceImpl clusterService = otherNode.getClusterService();
                if (clusterService.getMember(thisAddress) == null) {
                    continue;
                }

                logger.fine(otherNode.getThisAddress() + " is instructed to remove us.");
                otherNode.getNodeEngine().getExecutionService().execute(ExecutionService.SYSTEM_EXECUTOR, new Runnable() {
                    public void run() {
                        ILogger otherLogger = otherNode.getLogger(MockConnectionManager.class);
                        otherLogger.fine(localMember + " will be removed from the cluster if present, "
                                + "because it has requested to leave.");
                        clusterService.removeAddress(localMember.getAddress(), localMember.getUuid(),
                                "Connection manager is stopped on " + localMember);
                    }
                });
            }
        }
        for (Connection connection : mapConnections.values()) {
            connection.close(null, null);
        }
    }

    @Override
    public void shutdown() {
        stop();
    }

    @Override
    public boolean registerConnection(final Address remoteEndpoint, final Connection connection) {
        mapConnections.put(remoteEndpoint, connection);
        ioService.getEventService().executeEventCallback(new StripedRunnable() {
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

    public void destroyConnection(final Connection connection) {
        final Address endPoint = connection.getEndPoint();
        if (null != endPoint && mapConnections.remove(endPoint, connection)) {
            logger.info("Removed connection to endpoint: " + endPoint + ", connection: " + connection);

            connection.close(null, null);

            ioService.getEventService().executeEventCallback(new StripedRunnable() {
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
        } else {
            connection.close(null, null);
        }
    }

    @Override
    public int getActiveConnectionCount() {
        return 0;
    }

    @Override
    public int getCurrentClientConnections() {
        return 0;
    }

    @Override
    public int getConnectionCount() {
        return 0;
    }

    @Override
    public int getAllTextConnections() {
        return 0;
    }

    @Override
    public boolean transmit(Packet packet, Connection connection) {
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
        Connection connection = getConnection(target);
        if (connection != null) {
            return transmit(packet, connection);
        }

        if (sendTask == null) {
            sendTask = new SendTask(packet, target);
        }

        int retries = sendTask.retries;
        if (retries < RETRY_NUMBER && ioService.isActive()) {
            getOrConnect(target, true);
            // TODO: Caution: may break the order guarantee of the packets sent from the same thread!
            scheduler.schedule(sendTask, (retries + 1) * DELAY_FACTOR, TimeUnit.MILLISECONDS);
            return true;
        }
        return false;
    }

    private final class SendTask implements Runnable {

        private final Packet packet;
        private final Address target;

        private volatile int retries;

        private SendTask(Packet packet, Address target) {
            this.packet = packet;
            this.target = target;
        }

        @SuppressFBWarnings(value = "VO_VOLATILE_INCREMENT", justification = "single-writer, many-reader")
        @Override
        public void run() {
            retries++;
            if (logger.isFinestEnabled()) {
                logger.finest("Retrying[" + retries + "] packet send operation to: " + target);
            }
            send(packet, target, this);
        }
    }
}
