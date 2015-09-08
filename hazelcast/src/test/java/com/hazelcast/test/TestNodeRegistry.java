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
 */

package com.hazelcast.test;

import com.hazelcast.cluster.Joiner;
import com.hazelcast.cluster.impl.AbstractJoiner;
import com.hazelcast.cluster.impl.ClusterServiceImpl;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.AddressPicker;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeContext;
import com.hazelcast.instance.NodeState;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionListener;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.nio.ConnectionType;
import com.hazelcast.nio.IOService;
import com.hazelcast.nio.NodeIOService;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.OutboundFrame;
import com.hazelcast.nio.tcp.FirewallingMockConnectionManager;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.executor.StripedRunnable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public final class TestNodeRegistry {

    private final ConcurrentMap<Address, NodeEngineImpl> nodes = new ConcurrentHashMap<Address, NodeEngineImpl>(10);
    private final Object joinerLock = new Object();
    private final CopyOnWriteArrayList<Address> joinAddresses;

    TestNodeRegistry(CopyOnWriteArrayList<Address> addresses) {
        this.joinAddresses = addresses;
    }

    public NodeContext createNodeContext(Address address) {
        return new MockNodeContext(joinAddresses, nodes, address, joinerLock);
    }

    public HazelcastInstance getInstance(Address address) {
        NodeEngineImpl nodeEngine = nodes.get(address);
        return nodeEngine != null && nodeEngine.isActive() ? nodeEngine.getHazelcastInstance() : null;
    }

    Collection<HazelcastInstance> getAllHazelcastInstances() {
        Collection<HazelcastInstance> all = new LinkedList<HazelcastInstance>();
        for (NodeEngineImpl nodeEngine : nodes.values()) {
            if (nodeEngine.isActive()) {
                all.add(nodeEngine.getHazelcastInstance());
            }
        }
        return all;
    }

    void shutdown() {
        final Collection<NodeEngineImpl> values = new ArrayList<NodeEngineImpl>(nodes.values());
        nodes.clear();
        for (NodeEngineImpl value : values) {
            value.getHazelcastInstance().shutdown();
        }
    }

    void terminate() {
        final Collection<NodeEngineImpl> values = new ArrayList<NodeEngineImpl>(nodes.values());
        nodes.clear();
        for (NodeEngineImpl value : values) {
            HazelcastInstance hz = value.getHazelcastInstance();
            hz.getLifecycleService().terminate();
        }
    }

    private static class MockNodeContext implements NodeContext {

        final CopyOnWriteArrayList<Address> joinAddresses;
        final ConcurrentMap<Address, NodeEngineImpl> nodes;
        final Address thisAddress;
        final Object joinerLock;

        public MockNodeContext(CopyOnWriteArrayList<Address> addresses, ConcurrentMap<Address, NodeEngineImpl> nodes, Address thisAddress, Object joinerLock) {
            this.joinAddresses = addresses;
            this.nodes = nodes;
            this.thisAddress = thisAddress;
            this.joinerLock = joinerLock;
        }

        public AddressPicker createAddressPicker(Node node) {
            return new StaticAddressPicker(thisAddress);
        }

        public Joiner createJoiner(Node node) {
            return new MockJoiner(node, joinAddresses, nodes, joinerLock);
        }

        public ConnectionManager createConnectionManager(Node node, ServerSocketChannel serverSocketChannel) {
            NodeIOService ioService = new NodeIOService(node, node.nodeEngine);
            return new FirewallingMockConnectionManager(ioService, nodes, node, joinerLock);
        }
    }

    private static class StaticAddressPicker implements AddressPicker {
        final Address thisAddress;

        private StaticAddressPicker(Address thisAddress) {
            this.thisAddress = thisAddress;
        }

        public void pickAddress() throws Exception {
        }

        public Address getBindAddress() {
            return thisAddress;
        }

        public Address getPublicAddress() {
            return thisAddress;
        }

        public ServerSocketChannel getServerSocketChannel() {
            return null;
        }
    }

    private static class MockJoiner extends AbstractJoiner {

        final CopyOnWriteArrayList<Address> joinAddresses;
        final ConcurrentMap<Address, NodeEngineImpl> nodes;
        final Object joinerLock;

        MockJoiner(Node node, CopyOnWriteArrayList<Address> addresses, ConcurrentMap<Address, NodeEngineImpl> nodes, Object joinerLock) {
            super(node);
            this.joinAddresses = addresses;
            this.nodes = nodes;
            this.joinerLock = joinerLock;
        }

        public void doJoin() {
            NodeEngineImpl nodeEngine = null;
            synchronized (joinerLock) {
                for (Address address : joinAddresses) {
                    NodeEngineImpl ne = nodes.get(address);
                    if (ne != null && ne.isActive() && ne.getNode().joined()) {
                        nodeEngine = ne;
                        break;
                    }
                }
                Address master = null;
                if (nodeEngine != null) {
                    if (nodeEngine.getNode().isMaster()) {
                        master = nodeEngine.getThisAddress();
                    } else {
                        master = nodeEngine.getMasterAddress();
                    }
                }
                if (master == null) {
                    master = node.getThisAddress();
                }
                node.setMasterAddress(master);
                if (node.getMasterAddress().equals(node.getThisAddress())) {
                    node.setJoined();
                    node.setAsMaster();
                } else {
                    for (int i = 0; !node.joined() && node.getState() == NodeState.ACTIVE && i < 2000; i++) {
                        try {
                            node.clusterService.sendJoinRequest(node.getMasterAddress(), true);
                            Thread.sleep(50);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                            break;
                        }
                    }
                    if (!node.joined()) {
                        logger.severe("Node[" + node.getThisAddress()
                                + "] should have been joined to " + node.getMasterAddress());
                        node.shutdown(true);
                    }
                }
            }
        }

        public void searchForOtherClusters() {
        }

        @Override
        public String getType() {
            return "mock";
        }

        public String toString() {
            return "MockJoiner";
        }

        @Override
        public void blacklist(Address address, boolean permanent) {
        }

        @Override
        public boolean unblacklist(Address address) {
            return false;
        }

        @Override
        public boolean isBlacklisted(Address address) {
            return false;
        }
    }

    public static class MockConnectionManager implements ConnectionManager {
        private static final int RETRY_NUMBER = 5;
        private static final int DELAY_FACTOR = 100;

        final ConcurrentMap<Address, NodeEngineImpl> nodes;
        final Map<Address, MockConnection> mapConnections = new ConcurrentHashMap<Address, MockConnection>(10);
        final Node node;
        final Object joinerLock;
        private final Set<ConnectionListener> connectionListeners = new CopyOnWriteArraySet<ConnectionListener>();
        private final IOService ioService;
        private final ILogger logger;
        private final ScheduledExecutorService scheduler = new ScheduledThreadPoolExecutor(4);

        public MockConnectionManager(IOService ioService, ConcurrentMap<Address, NodeEngineImpl> nodes,
                                     Node node, Object joinerLock) {
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
            MockConnection conn = mapConnections.get(address);
            if (conn == null) {
                NodeEngineImpl nodeEngine = nodes.get(address);
                MockConnection thisConnection = new MockConnection(address, node.getThisAddress(), node.nodeEngine);
                conn = new MockConnection(node.getThisAddress(), address, nodeEngine);
                conn.localConnection = thisConnection;
                thisConnection.localConnection = conn;
                mapConnections.put(address, conn);
            }
            return conn;
        }

        @Override
        public Connection getOrConnect(Address address) {
            return getConnection(address);
        }

        @Override
        public Connection getOrConnect(Address address, boolean silent) {
            return getConnection(address);
        }

        @Override
        public void shutdown() {
            for (Address address : nodes.keySet()) {
                if (address.equals(node.getThisAddress())) continue;

                final NodeEngineImpl nodeEngine = nodes.get(address);
                if (nodeEngine != null && nodeEngine.isActive()) {
                    nodeEngine.getExecutionService().execute(ExecutionService.SYSTEM_EXECUTOR, new Runnable() {
                        public void run() {
                            final ClusterServiceImpl clusterService = (ClusterServiceImpl) nodeEngine.getClusterService();
                            clusterService.removeAddress(node.getThisAddress());
                        }
                    });
                }
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
            if (connection == null) {
                return false;
            }
            return connection.write(packet);
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

    public static class MockConnection implements Connection {
        protected final Address localEndpoint;
        volatile Connection localConnection;
        final Address remoteEndpoint;
        protected final NodeEngineImpl nodeEngine;
        private volatile boolean live = true;

        public MockConnection(Address localEndpoint, Address remoteEndpoint, NodeEngineImpl nodeEngine) {
            this.localEndpoint = localEndpoint;
            this.remoteEndpoint = remoteEndpoint;
            this.nodeEngine = nodeEngine;
        }

        public Address getEndPoint() {
            return remoteEndpoint;
        }

        public boolean live() {
            return live;
        }

        public boolean write(OutboundFrame frame) {
            final Packet packet = (Packet) frame;
            if (nodeEngine.getNode().getState() != NodeState.SHUT_DOWN) {
                Packet newPacket = readFromPacket(packet);
                nodeEngine.getPacketDispatcher().dispatch(newPacket);
                return true;
            }
            return false;
        }

        private Packet readFromPacket(Packet packet) {
            Packet newPacket = new Packet();
            ByteBuffer buffer = ByteBuffer.allocate(4096);
            boolean writeDone;
            boolean readDone;
            do {
                writeDone = packet.writeTo(buffer);
                buffer.flip();
                readDone = newPacket.readFrom(buffer);
                if (buffer.hasRemaining()) {
                    throw new IllegalStateException("Buffer should be empty! " + buffer);
                }
                buffer.clear();
            } while (!writeDone);

            if (!readDone) {
                throw new IllegalStateException("Read should be completed!");
            }

            newPacket.setConn(localConnection);
            return newPacket;
        }

        public long lastReadTimeMillis() {
            return System.currentTimeMillis();
        }

        public long lastWriteTimeMillis() {
            return System.currentTimeMillis();
        }

        public void close() {
            if (!live) {
                return;
            }
            live = false;
            nodeEngine.getNode().connectionManager.destroyConnection(this);
        }

        @Override
        public void setType(ConnectionType type) {
            //NO OP
        }

        public boolean isClient() {
            return false;
        }

        @Override
        public ConnectionType getType() {
            return ConnectionType.MEMBER;
        }

        public InetAddress getInetAddress() {
            try {
                return localEndpoint.getInetAddress();
            } catch (UnknownHostException e) {
                throw ExceptionUtil.rethrow(e);
            }
        }

        public InetSocketAddress getRemoteSocketAddress() {
            return new InetSocketAddress(getInetAddress(), getPort());
        }

        public int getPort() {
            return localEndpoint.getPort();
        }

        @Override
        public boolean isAlive() {
            return true;
        }

        @Override
        public String toString() {
            return "MockConnection{" +
                    "localEndpoint=" + localEndpoint +
                    ", remoteEndpoint=" + remoteEndpoint +
                    '}';
        }
    }
}
