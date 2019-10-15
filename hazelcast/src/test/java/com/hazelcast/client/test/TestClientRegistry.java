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

package com.hazelcast.client.test;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.impl.connection.ClientConnectionManager;
import com.hazelcast.client.impl.connection.nio.ClientConnection;
import com.hazelcast.client.impl.connection.nio.ClientConnectionManagerImpl;
import com.hazelcast.client.impl.clientside.ClientConnectionManagerFactory;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.test.TwoWayBlockableExecutor.LockPair;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.instance.impl.NodeState;
import com.hazelcast.internal.networking.OutboundFrame;
import com.hazelcast.internal.networking.nio.NioNetworking;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.nio.ConnectionType;
import com.hazelcast.spi.exception.TargetDisconnectedException;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.mocknetwork.MockConnection;
import com.hazelcast.test.mocknetwork.TestNodeRegistry;
import com.hazelcast.internal.util.ConstructorFunction;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.hazelcast.instance.EndpointQualifier.CLIENT;
import static com.hazelcast.test.HazelcastTestSupport.getNodeEngineImpl;
import static com.hazelcast.internal.util.ConcurrencyUtil.getOrPutIfAbsent;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;

class TestClientRegistry {

    private static final AtomicInteger CLIENT_PORTS = new AtomicInteger(40000);
    private static final ILogger LOGGER = Logger.getLogger(HazelcastClient.class);

    private final TestNodeRegistry nodeRegistry;

    TestClientRegistry(TestNodeRegistry nodeRegistry) {
        this.nodeRegistry = nodeRegistry;
    }

    ClientConnectionManagerFactory createClientServiceFactory() {
        return new MockClientConnectionManagerFactory("127.0.0.1", CLIENT_PORTS);
    }

    private class MockClientConnectionManagerFactory implements ClientConnectionManagerFactory {

        private final String host;
        private final AtomicInteger ports;

        MockClientConnectionManagerFactory(String host, AtomicInteger ports) {
            this.host = host;
            this.ports = ports;
        }

        @Override
        public ClientConnectionManager createConnectionManager(HazelcastClientInstanceImpl client) {
            return new MockClientConnectionManager(client, host, ports);
        }
    }

    class MockClientConnectionManager
            extends ClientConnectionManagerImpl {

        private final ConcurrentHashMap<Address, LockPair> addressBlockMap = new ConcurrentHashMap<Address, LockPair>();

        private final HazelcastClientInstanceImpl client;
        private final String host;
        private final AtomicInteger ports;

        MockClientConnectionManager(HazelcastClientInstanceImpl client, String host, AtomicInteger ports) {
            super(client);
            this.client = client;
            this.host = host;
            this.ports = ports;
        }

        @Override
        protected NioNetworking initNetworking(HazelcastClientInstanceImpl client) {
            return null;
        }

        @Override
        protected void startNetworking() {
        }

        @Override
        protected void stopNetworking() {
        }

        @Override
        protected ClientConnection createSocketConnection(Address address) throws IOException {
            if (!alive) {
                throw new HazelcastException("ConnectionManager is not active!!!");
            }
            try {
                HazelcastInstance instance = nodeRegistry.getInstance(address);
                if (instance == null) {
                    throw new IOException("Can not connected to " + address + ": instance does not exist");
                }
                Address localAddress = new Address(host, ports.incrementAndGet());
                LockPair lockPair = getLockPair(address);

                MockedClientConnection connection = new MockedClientConnection(client, connectionIdGen.incrementAndGet(),
                        getNodeEngineImpl(instance), address, localAddress, lockPair);
                LOGGER.info("Created connection to endpoint: " + address + ", connection: " + connection);
                return connection;
            } catch (Exception e) {
                throw rethrow(e, IOException.class);
            }
        }

        private LockPair getLockPair(Address address) {
            return getOrPutIfAbsent(addressBlockMap, address, new ConstructorFunction<Address, LockPair>() {
                @Override
                public LockPair createNew(Address arg) {
                    return new LockPair(new ReentrantReadWriteLock(), new ReentrantReadWriteLock());
                }
            });
        }

        /**
         * Blocks incoming messages to client from given address
         */
        void blockFrom(Address address) {
            LOGGER.info("Blocked messages from " + address);
            LockPair lockPair = getLockPair(address);
            lockPair.blockIncoming();
        }

        /**
         * Unblocks incoming messages to client from given address
         */
        void unblockFrom(Address address) {
            LOGGER.info("Unblocked messages from " + address);
            LockPair lockPair = getLockPair(address);
            lockPair.unblockIncoming();
        }

        /**
         * Blocks outgoing messages from client to given address
         */
        void blockTo(Address address) {
            LOGGER.info("Blocked messages to " + address);
            LockPair lockPair = getLockPair(address);
            lockPair.blockOutgoing();
        }

        /**
         * Unblocks outgoing messages from client to given address
         */
        void unblockTo(Address address) {
            LOGGER.info("Unblocked messages to " + address);
            LockPair lockPair = getLockPair(address);
            lockPair.unblockOutgoing();
        }
    }

    private class MockedClientConnection extends ClientConnection {

        private final NodeEngineImpl serverNodeEngine;
        private final Address remoteAddress;
        private final Address localAddress;
        private final TwoWayBlockableExecutor executor;
        private final MockedNodeConnection serverSideConnection;

        private volatile long lastReadTime;
        private volatile long lastWriteTime;

        MockedClientConnection(HazelcastClientInstanceImpl client,
                               int connectionId, NodeEngineImpl serverNodeEngine, Address address, Address localAddress,
                               LockPair lockPair) {
            super(client, connectionId);
            this.serverNodeEngine = serverNodeEngine;
            this.remoteAddress = address;
            this.localAddress = localAddress;
            this.executor = new TwoWayBlockableExecutor(lockPair);
            this.serverSideConnection = new MockedNodeConnection(connectionId, remoteAddress,
                    localAddress, serverNodeEngine, this);
        }

        @Override
        public void handleClientMessage(final ClientMessage clientMessage) {
            executor.executeIncoming(new Runnable() {
                @Override
                public void run() {
                    lastReadTime = System.currentTimeMillis();
                    MockedClientConnection.super.handleClientMessage(clientMessage);
                }

                @Override
                public String toString() {
                    return "Runnable message " + clientMessage + ", " + MockedClientConnection.this;
                }
            });
        }

        @Override
        public boolean write(final OutboundFrame frame) {
            if (!isAlive()) {
                return false;
            }
            final Node node = serverNodeEngine.getNode();
            if (node.getState() == NodeState.SHUT_DOWN) {
                return false;
            }
            executor.executeOutgoing(new Runnable() {
                @Override
                public String toString() {
                    return "Runnable message " + frame + ", " + MockedClientConnection.this;
                }

                @Override
                public void run() {
                    ClientMessage clientMessage = readFromPacket((ClientMessage) frame);
                    lastWriteTime = System.currentTimeMillis();
                    clientMessage.setConnection(serverSideConnection);
                    serverSideConnection.handleClientMessage(clientMessage);
                }
            });
            return true;
        }

        private ClientMessage readFromPacket(ClientMessage packet) {
            //Since frames are read, there should be no need to re-read to client message
            //return ClientMessage.createForDecode(packet.buffer(), 0);
            return packet;
        }

        @Override
        public long lastReadTimeMillis() {
            return lastReadTime;
        }

        @Override
        public long lastWriteTimeMillis() {
            return lastWriteTime;
        }

        @Override
        public InetAddress getInetAddress() {
            try {
                return remoteAddress.getInetAddress();
            } catch (UnknownHostException e) {
                e.printStackTrace();
                return null;
            }
        }

        @Override
        public InetSocketAddress getRemoteSocketAddress() {
            try {
                return remoteAddress.getInetSocketAddress();
            } catch (UnknownHostException e) {
                e.printStackTrace();
                return null;
            }
        }

        @Override
        public int getPort() {
            return remoteAddress.getPort();
        }

        @Override
        public InetSocketAddress getLocalSocketAddress() {
            try {
                return localAddress.getInetSocketAddress();
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
            return null;
        }

        @Override
        protected void innerClose() {
            executor.executeOutgoing((new Runnable() {
                @Override
                public void run() {
                    serverSideConnection.close(null, null);
                }

                @Override
                public String toString() {
                    return "Client Closed EOF. " + MockedClientConnection.this;
                }
            }));
            executor.shutdownIncoming();

        }

        void onServerClose(final String reason) {
            executor.executeIncoming(new Runnable() {
                @Override
                public String toString() {
                    return "Server Closed EOF. " + MockedClientConnection.this;
                }

                @Override
                public void run() {
                    MockedClientConnection.this.close(reason, new TargetDisconnectedException("Mocked Remote socket closed"));
                }
            });
            executor.shutdownOutgoing();
        }

        @Override
        public String toString() {
            return "MockedClientConnection{"
                    + "localAddress=" + localAddress
                    + ", super=" + super.toString()
                    + '}';
        }
    }

    private class MockedNodeConnection extends MockConnection {

        private final AtomicBoolean alive = new AtomicBoolean(true);

        private final MockedClientConnection responseConnection;
        private final int connectionId;

        private volatile long lastReadTimeMillis;
        private volatile long lastWriteTimeMillis;

        MockedNodeConnection(int connectionId, Address localEndpoint,
                             Address remoteEndpoint, NodeEngineImpl nodeEngine, MockedClientConnection responseConnection) {
            super(localEndpoint, remoteEndpoint, nodeEngine);
            this.responseConnection = responseConnection;
            this.connectionId = connectionId;
            register();
            lastReadTimeMillis = System.currentTimeMillis();
            lastWriteTimeMillis = System.currentTimeMillis();
        }

        private void register() {
            Node node = remoteNodeEngine.getNode();
            node.getEndpointManager(CLIENT).registerConnection(getEndPoint(), this);
        }

        @Override
        public boolean write(OutboundFrame frame) {
            final ClientMessage packet = (ClientMessage) frame;
            if (isAlive()) {
                lastWriteTimeMillis = System.currentTimeMillis();
                ClientMessage newPacket = readFromPacket(packet);
                newPacket.setConnection(responseConnection);
                responseConnection.handleClientMessage(newPacket);
                return true;
            }
            return false;
        }

        void handleClientMessage(ClientMessage newPacket) {
            lastReadTimeMillis = System.currentTimeMillis();
            remoteNodeEngine.getNode().clientEngine.accept(newPacket);
        }

        @Override
        public boolean isClient() {
            return true;
        }

        private ClientMessage readFromPacket(ClientMessage packet) {
            //Since frames are read, there should be no need to re-read to client message
            //return ClientMessage.createForDecode(packet.buffer(), 0);
            return packet;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            MockedNodeConnection that = (MockedNodeConnection) o;

            if (connectionId != that.connectionId) {
                return false;
            }
            Address remoteEndpoint = getEndPoint();
            return !(remoteEndpoint != null ? !remoteEndpoint.equals(that.getEndPoint()) : that.getEndPoint() != null);
        }

        @Override
        public void close(String reason, Throwable cause) {
            if (!alive.compareAndSet(true, false)) {
                return;
            }

            Logger.getLogger(MockedNodeConnection.class).warning("Server connection closed: " + reason, cause);
            super.close(reason, cause);
            responseConnection.onServerClose(reason);
        }

        @Override
        public int hashCode() {
            int result = connectionId;
            Address remoteEndpoint = getEndPoint();
            result = 31 * result + (remoteEndpoint != null ? remoteEndpoint.hashCode() : 0);
            return result;
        }

        @Override
        public long lastReadTimeMillis() {
            return lastReadTimeMillis;
        }

        @Override
        public long lastWriteTimeMillis() {
            return lastWriteTimeMillis;
        }

        @Override
        public ConnectionType getType() {
            return ConnectionType.JAVA_CLIENT;
        }

        @Override
        public String toString() {
            return "MockedNodeConnection{"
                    + " remoteEndpoint = " + getEndPoint()
                    + ", localEndpoint = " + localEndpoint
                    + ", connectionId = " + connectionId
                    + '}';
        }
    }
}
