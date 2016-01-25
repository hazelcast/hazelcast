/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.impl.ClusterServiceImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionListener;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.nio.IOService;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.impl.NodeEngineImpl;
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

    final ConcurrentMap<Address, MockConnection> mapConnections = new ConcurrentHashMap<Address, MockConnection>(10);
    final ConcurrentMap<Address, NodeEngineImpl> nodes;
    final Node node;
    final Object joinerLock;

    private final Set<ConnectionListener> connectionListeners = new CopyOnWriteArraySet<ConnectionListener>();
    private final ScheduledExecutorService scheduler = new ScheduledThreadPoolExecutor(4);
    private final IOService ioService;
    private final ILogger logger;

    public MockConnectionManager(IOService ioService, ConcurrentMap<Address, NodeEngineImpl> nodes, Node node, Object joinerLock) {
        this.ioService = ioService;
        this.nodes = nodes;
        this.node = node;
        this.joinerLock = joinerLock;
        this.logger = ioService.getLogger(MockConnectionManager.class.getName());
        synchronized (this.joinerLock) {
            this.nodes.put(node.getThisAddress(), node.nodeEngine);
        }
    }

    @Override
    public Connection getConnection(Address address) {
        return mapConnections.get(address);
    }

    @Override
    public Connection getOrConnect(Address address) {
        MockConnection conn = mapConnections.get(address);
        if (conn == null || !conn.isAlive()) {
            NodeEngineImpl nodeEngine = nodes.get(address);
            if (nodeEngine != null && nodeEngine.isRunning()) {
                MockConnection thisConnection = new MockConnection(address, node.getThisAddress(), node.nodeEngine);
                conn = new MockConnection(node.getThisAddress(), address, nodeEngine);
                conn.localConnection = thisConnection;
                thisConnection.localConnection = conn;
                mapConnections.put(address, conn);
                logger.info("Created connection to endpoint: " + address + ", connection: " + conn);
            }
        }
        return conn;
    }

    @Override
    public Connection getOrConnect(Address address, boolean silent) {
        return getOrConnect(address);
    }

    @Override
    public void shutdown() {
        for (Address address : nodes.keySet()) {
            if (address.equals(node.getThisAddress())) {
                continue;
            }

            final NodeEngineImpl nodeEngine = nodes.get(address);
            if (nodeEngine != null && nodeEngine.isRunning()) {
                nodeEngine.getExecutionService().execute(ExecutionService.SYSTEM_EXECUTOR, new Runnable() {
                    public void run() {
                        ClusterServiceImpl clusterService = (ClusterServiceImpl) nodeEngine.getClusterService();
                        clusterService.removeAddress(node.getThisAddress());
                    }
                });
            }
        }
        for (MockConnection connection : mapConnections.values()) {
            connection.close();
        }
    }

    @Override
    public boolean registerConnection(final Address remoteEndpoint, final Connection connection) {
        mapConnections.put(remoteEndpoint, (MockConnection) connection);
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
    public void start() {
    }

    @Override
    public void addConnectionListener(ConnectionListener connectionListener) {
        connectionListeners.add(connectionListener);
    }

    public void destroyConnection(final Connection connection) {
        final Address endPoint = connection.getEndPoint();
        final boolean removed = mapConnections.remove(endPoint, connection);
        if (!removed) {
            return;
        }

        logger.info("Removed connection to endpoint: " + endPoint + ", connection: " + connection);
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
    }

    @Override
    public void stop() {
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
