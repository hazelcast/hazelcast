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

package com.hazelcast.test;

import com.hazelcast.cluster.Joiner;
import com.hazelcast.cluster.impl.AbstractJoiner;
import com.hazelcast.cluster.impl.ClusterServiceImpl;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.AddressPicker;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeContext;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionListener;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.nio.ConnectionType;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.SocketWritable;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.WriteResult;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.util.ExceptionUtil;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

final class TestNodeRegistry {

    private final Address[] addresses;
    private final ConcurrentMap<Address, NodeEngineImpl> nodes = new ConcurrentHashMap<Address, NodeEngineImpl>(10);
    private final Object joinerLock = new Object();

    TestNodeRegistry(Address[] addresses) {
        this.addresses = addresses;
    }

    NodeContext createNodeContext(Address address) {
        return new MockNodeContext(addresses, nodes, address, joinerLock);
    }

    HazelcastInstance getInstance(Address address) {
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

        final Address[] addresses;
        final ConcurrentMap<Address, NodeEngineImpl> nodes;
        final Address thisAddress;
        final Object joinerLock;

        public MockNodeContext(Address[] addresses, ConcurrentMap<Address, NodeEngineImpl> nodes, Address thisAddress, Object joinerLock) {
            this.addresses = addresses;
            this.nodes = nodes;
            this.thisAddress = thisAddress;
            this.joinerLock = joinerLock;
        }

        public AddressPicker createAddressPicker(Node node) {
            return new StaticAddressPicker(thisAddress);
        }

        public Joiner createJoiner(Node node) {
            return new MockJoiner(node, addresses, nodes, joinerLock);
        }

        public ConnectionManager createConnectionManager(Node node, ServerSocketChannel serverSocketChannel) {
            return new MockConnectionManager(addresses, nodes, node, joinerLock);
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

        final Address[] addresses;
        final ConcurrentMap<Address, NodeEngineImpl> nodes;
        final Object joinerLock;

        MockJoiner(Node node, Address[] addresses, ConcurrentMap<Address, NodeEngineImpl> nodes, Object joinerLock) {
            super(node);
            this.addresses = addresses;
            this.nodes = nodes;
            this.joinerLock = joinerLock;
        }

        public void doJoin() {
            NodeEngineImpl nodeEngine = null;
            synchronized (joinerLock) {
                for (Address address : addresses) {
                    NodeEngineImpl ne = nodes.get(address);
                    if (ne != null && ne.getNode().isActive() && ne.getNode().joined()) {
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
                } else {
                    for (int i = 0; !node.joined() && i < 1000; i++) {
                        try {
                            node.clusterService.sendJoinRequest(node.getMasterAddress(), true);
                            Thread.sleep(50);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    if (!node.joined()) {
                        throw new AssertionError("Node[" + node.getThisAddress()
                                + "] should have been joined to " + node.getMasterAddress());
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
        public void blacklist(Address callerAddress) {
        }

        @Override
        public boolean isBlacklisted(Address address) {
            return false;
        }
    }

    private static class MockConnectionManager implements ConnectionManager {
        final Address[] addresses;
        final ConcurrentMap<Address, NodeEngineImpl> nodes;
        final Map<Address, MockConnection> mapConnections = new ConcurrentHashMap<Address, MockConnection>(10);
        final Node node;
        final Object joinerLock;

        MockConnectionManager(Address[] addresses, ConcurrentMap<Address, NodeEngineImpl> nodes,
                              Node node, Object joinerLock) {
            this.addresses = addresses;
            this.nodes = nodes;
            this.node = node;
            this.joinerLock = joinerLock;
            synchronized (this.joinerLock) {
                this.nodes.put(node.getThisAddress(), node.nodeEngine);
            }
        }

        @Override
        public void dumpPerformanceMetrics(StringBuffer sb) {
        }

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

        public Connection getOrConnect(Address address) {
            return getConnection(address);
        }

        public Connection getOrConnect(Address address, boolean silent) {
            return getConnection(address);
        }

        public void shutdown() {
            for (Address address : addresses) {
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
        public boolean registerConnection(Address address, Connection connection) {
            mapConnections.put(address, (MockConnection) connection);
            return true;
        }

        public void start() {
        }

        public void addConnectionListener(ConnectionListener connectionListener) {
        }

        public void destroyConnection(Connection conn) {
        }


        public void restart() {
        }

        @Override
        public int getActiveConnectionCount() {
            return 0;
        }

        public int getCurrentClientConnections() {
            return 0;
        }

        @Override
        public int getConnectionCount() {
            return 0;
        }

        public int getAllTextConnections() {
            return 0;
        }
    }

    private static class MockConnection implements Connection {
        final Address localEndpoint;
        volatile Connection localConnection;
        final Address remoteEndpoint;
        final NodeEngineImpl nodeEngine;

        public MockConnection(Address localEndpoint, Address remoteEndpoint, NodeEngineImpl nodeEngine) {
            this.localEndpoint = localEndpoint;
            this.remoteEndpoint = remoteEndpoint;
            this.nodeEngine = nodeEngine;
        }

        public Address getEndPoint() {
            return remoteEndpoint;
        }

        public boolean live() {
            return true;
        }

        public int getId() {
            return 0;
        }

        @Override
        public WriteResult writeBackup(Packet packet) {
            return write(packet);
        }

        public WriteResult write(SocketWritable socketWritable) {
            final Packet packet = (Packet) socketWritable;
            if (nodeEngine.getNode().isActive()) {
                Packet newPacket = readFromPacket(packet);
                MemberImpl member = nodeEngine.getClusterService().getMember(localEndpoint);
                if (member != null) {
                    member.didRead();
                }
                nodeEngine.handlePacket(newPacket);
                return WriteResult.SUCCESS;
            }
            return WriteResult.FAILURE;
        }

        private Packet readFromPacket(Packet packet) {
            Packet newPacket = new Packet(nodeEngine.getSerializationService().getPortableContext());
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

        public long lastReadTime() {
            return System.currentTimeMillis();
        }

        public long lastWriteTime() {
            return System.currentTimeMillis();
        }

        public void close() {
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
        public void setAvailableSlots(int claimResponse) {

        }

        @Override
        public boolean isAlive() {
            return true;
        }
    }
}
