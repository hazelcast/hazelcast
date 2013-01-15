/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.spi.impl;

import com.hazelcast.cluster.AbstractJoiner;
import com.hazelcast.cluster.Joiner;
import com.hazelcast.cluster.SplitBrainHandler;
import com.hazelcast.instance.AddressPicker;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeContext;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ConnectionListener;
import com.hazelcast.nio.ConnectionMonitor;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationContext;
import com.hazelcast.spi.Connection;
import com.hazelcast.spi.ConnectionManager;
import com.hazelcast.spi.NodeEngine;

import java.nio.channels.ServerSocketChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class StaticNodeRegistry {

    final Address[] addresses;
    final Map<Address, NodeEngine> nodes = new ConcurrentHashMap<Address, NodeEngine>(10);

    public StaticNodeRegistry(Address[] addresses) {
        this.addresses = addresses;
    }

    public NodeContext createNodeContext(Address address) {
        StaticNodeContext staticNodeContext = new StaticNodeContext(address);
        return staticNodeContext;
    }

    void register(Address address, NodeEngine nodeEngine) {
        nodes.put(address, nodeEngine);
    }

    class StaticNodeContext implements NodeContext {

        final Address thisAddress;

        public StaticNodeContext(Address thisAddress) {
            this.thisAddress = thisAddress;
        }

        public AddressPicker createAddressPicker(Node node) {
            return new StaticAddressPicker();
        }

        public Joiner createJoiner(Node node) {
            return new StaticJoiner(node);
        }

        public ConnectionManager createConnectionManager(Node node, ServerSocketChannel serverSocketChannel) {
            return new StaticConnectionManager(node);
        }

        class StaticConnectionManager implements ConnectionManager {
            final Map<Address, Connection> mapConnections = new ConcurrentHashMap<Address, Connection>(10);
            private final Node node;
            final Connection thisConnection;

            StaticConnectionManager(Node node) {
                this.node = node;
                thisConnection = new StaticConnection(node.getThisAddress(), node.nodeEngine);
                register(node.getThisAddress(), node.nodeEngine);
            }

            public Connection getConnection(Address address) {
                Connection conn = mapConnections.get(address);
                if (conn == null) {
                    NodeEngine nodeEngine = nodes.get(address);
                    conn = new StaticConnection(address, nodeEngine);
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
            }

            public void start() {
            }

            public void addConnectionListener(ConnectionListener connectionListener) {
            }

            public Map<Address, Connection> getReadonlyConnectionMap() {
                return null;
            }

            public void destroyConnection(Connection conn) {
            }

            public void onRestart() {
            }

            public int getCurrentClientConnections() {
                return 0;
            }

            public int getAllTextConnections() {
                return 0;
            }

            public boolean bind(Connection connection, Address localAddress, Address targetAddress, boolean replyBack) {
                return false;
            }

            class StaticConnection implements Connection {
                final Address endpoint;
                private final NodeEngine nodeEngine;

                public StaticConnection(Address address, NodeEngine nodeEngine) {
                    this.endpoint = address;
                    this.nodeEngine = nodeEngine;
                }

                public Address getEndPoint() {
                    return endpoint;
                }

                public boolean live() {
                    return true;
                }

                public boolean write(Data data, SerializationContext context, int header) {
                    if (header == Packet.HEADER_OP) {
                        OperationServiceImpl os = (OperationServiceImpl) nodeEngine.getOperationService();
                        os.handleOperation(data, thisConnection);
                    } else {
                    }
                    return true;
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

                public void setEndPoint(Address remoteEndPoint) {
                }

                public void setMonitor(ConnectionMonitor connectionMonitor) {
                }
            }
        }

        class StaticAddressPicker implements AddressPicker {
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

        class StaticJoiner extends AbstractJoiner {

            StaticJoiner(Node node) {
                super(node);
            }

            public void doJoin(AtomicBoolean joined) {
                node.setMasterAddress(addresses[0]);
                if (node.getMasterAddress().equals(node.getThisAddress())) {
                    node.setJoined();
                } else {
                    for (int i = 0; !node.joined() && i < 10; i++) {
                        node.clusterService.sendJoinRequest(node.getMasterAddress(), true);
                        System.out.println("Sending join request");
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }

            public void searchForOtherClusters(SplitBrainHandler splitBrainHandler) {
            }
        }
    }
}
