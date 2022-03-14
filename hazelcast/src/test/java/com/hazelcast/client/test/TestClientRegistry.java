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

package com.hazelcast.client.test;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.clientside.ClientConnectionManagerFactory;
import com.hazelcast.client.impl.clientside.ClusterDiscoveryService;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.clientside.LifecycleServiceImpl;
import com.hazelcast.client.impl.connection.ClientConnectionManager;
import com.hazelcast.client.impl.connection.tcp.Authenticator;
import com.hazelcast.client.impl.connection.tcp.TcpClientConnection;
import com.hazelcast.client.impl.connection.tcp.TcpClientConnectionManager;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.util.ClientMessageHandler;
import com.hazelcast.client.impl.spi.ClientClusterService;
import com.hazelcast.client.impl.spi.impl.listener.ClientListenerServiceImpl;
import com.hazelcast.client.test.TwoWayBlockableExecutor.LockPair;
import com.hazelcast.cluster.Address;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleService;
import com.hazelcast.internal.networking.Networking;
import com.hazelcast.internal.networking.OutboundFrame;
import com.hazelcast.internal.util.ConstructorFunction;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.spi.exception.TargetDisconnectedException;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.test.mocknetwork.MockServerConnection;
import com.hazelcast.test.mocknetwork.TestNodeRegistry;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

import static com.hazelcast.client.impl.clientside.DefaultClientConnectionManagerFactory.createAuthenticator;
import static com.hazelcast.internal.util.ConcurrencyUtil.getOrPutIfAbsent;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;

class TestClientRegistry {

    private static final ILogger LOGGER = Logger.getLogger(HazelcastClient.class);
    private final AtomicInteger CLIENT_PORTS = new AtomicInteger(40000);

    private final TestNodeRegistry nodeRegistry;

    TestClientRegistry(TestNodeRegistry nodeRegistry) {
        this.nodeRegistry = nodeRegistry;
    }

    ClientConnectionManagerFactory createClientServiceFactory(String sourceIp) {
        return new MockClientConnectionManagerFactory(sourceIp == null ? "127.0.0.1" : sourceIp, CLIENT_PORTS);
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
            ClientListenerServiceImpl listenerService = (ClientListenerServiceImpl) client.getListenerService();
            Consumer<ClientMessage> responseHandler = client.getInvocationService().getResponseHandler();
            ClientMessageHandler messageHandler = new ClientMessageHandler(listenerService, responseHandler);

            ClientConfig clientConfig = client.getClientConfig();
            HazelcastProperties properties = client.getProperties();
            boolean failoverEnabled = client.getFailoverConfig() != null;
            ClusterDiscoveryService clusterDiscoveryService = client.getClusterDiscoveryService();
            return new MockTcpClientConnectionManager(client.getLoggingService(), clientConfig, properties,
                    clusterDiscoveryService, client.getName(), null,
                    (LifecycleServiceImpl) client.getLifecycleService(),
                    client.getClientClusterService(), client, createAuthenticator(client), host, ports, messageHandler);
        }
    }

    class MockTcpClientConnectionManager
            extends TcpClientConnectionManager {

        private final ConcurrentHashMap<Address, LockPair> addressBlockMap = new ConcurrentHashMap<>();
        private final ClientMessageHandler clientMessageHandler;
        private final String host;
        private final AtomicInteger ports;

        @SuppressWarnings("checkstyle:ParameterNumber")
        MockTcpClientConnectionManager(LoggingService loggingService, ClientConfig clientConfig,
                                       HazelcastProperties properties,
                                       ClusterDiscoveryService clusterDiscoveryService, String clientName,
                                       Networking networking, LifecycleServiceImpl lifecycleService,
                                       ClientClusterService clientClusterService,
                                       HazelcastClientInstanceImpl client,
                                       Authenticator authenticator, String host, AtomicInteger ports,
                                       ClientMessageHandler clientMessageHandler) {
            super(loggingService, clientConfig, properties, clusterDiscoveryService, clientName,
                    networking, lifecycleService, clientClusterService, client, authenticator);
            this.host = host;
            this.ports = ports;
            this.clientMessageHandler = clientMessageHandler;
        }

        @Override
        protected void startNetworking() {
        }

        @Override
        protected void stopNetworking() {
        }

        @Override
        protected TcpClientConnection createSocketConnection(Address remoteAddress) {
            checkClientActive();
            try {
                HazelcastInstance instance = nodeRegistry.getInstance(remoteAddress);
                UUID remoteUuid = nodeRegistry.uuidOf(remoteAddress);
                if (instance == null) {
                    throw new IOException("Can not connect to " + remoteAddress + ": instance does not exist");
                }
                Address localAddress = new Address(host, ports.incrementAndGet());
                LockPair lockPair = getLockPair(remoteAddress);

                MockedTcpClientConnection connection = new MockedTcpClientConnection(
                        this, lifecycleService, loggingService,
                        connectionIdGen.incrementAndGet(),
                        getNodeEngineImpl(instance),
                        localAddress,
                        remoteAddress,
                        remoteUuid,
                        lockPair,
                        clientMessageHandler
                );
                LOGGER.info("Created connection to endpoint: " + remoteAddress + ", connection: " + connection);
                return connection;
            } catch (Exception e) {
                throw rethrow(e);
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

    private class MockedTcpClientConnection extends TcpClientConnection {

        // the bind address of client
        private final Address localAddress;
        // the remote address that belongs to server side of the connection
        private final Address remoteAddress;
        private final TwoWayBlockableExecutor executor;
        private final MockedServerConnection serverConnection;
        private final ClientMessageHandler clientMessageHandler;
        private volatile long lastReadTime;
        private volatile long lastWriteTime;

        MockedTcpClientConnection(
                TcpClientConnectionManager connectionManager,
                LifecycleService lifecycleService, LoggingService loggingService,
                int connectionId,
                NodeEngineImpl serverNodeEngine,
                Address localAddress,
                Address remoteAddress,
                UUID serverUuid,
                LockPair lockPair,
                ClientMessageHandler clientMessageHandler) {
            super(connectionManager, lifecycleService, loggingService, connectionId, null);
            this.localAddress = localAddress;
            this.remoteAddress = remoteAddress;
            this.executor = new TwoWayBlockableExecutor(lockPair);
            this.serverConnection = new MockedServerConnection(
                    connectionId,
                    remoteAddress,
                    localAddress,
                    serverUuid,
                    null,
                    null,
                    serverNodeEngine,
                    this
            );
            this.clientMessageHandler = clientMessageHandler;
        }

        public void handleClientMessage(final ClientMessage clientMessage) {
            executor.executeIncoming(new Runnable() {
                @Override
                public void run() {
                    lastReadTime = System.currentTimeMillis();
                    clientMessageHandler.accept(clientMessage);
                }

                @Override
                public String toString() {
                    return "Runnable message " + clientMessage + ", " + MockedTcpClientConnection.this;
                }
            });
        }

        @Override
        public boolean write(final OutboundFrame frame) {
            if (!isAlive()) {
                return false;
            }
            executor.executeOutgoing(new Runnable() {
                @Override
                public String toString() {
                    return "Runnable message " + frame + ", " + MockedTcpClientConnection.this;
                }

                @Override
                public void run() {
                    ClientMessage clientMessage = readFromPacket((ClientMessage) frame);
                    lastWriteTime = System.currentTimeMillis();
                    clientMessage.setConnection(serverConnection);
                    serverConnection.handleClientMessage(clientMessage);
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
        public InetSocketAddress getRemoteSocketAddress() {
            try {
                return remoteAddress.getInetSocketAddress();
            } catch (UnknownHostException e) {
                e.printStackTrace();
                return null;
            }
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
                    serverConnection.close(null, null);
                }

                @Override
                public String toString() {
                    return "Client Closed EOF. " + MockedTcpClientConnection.this;
                }
            }));
            executor.shutdownIncoming();
        }

        void onServerClose(final String reason) {
            executor.executeIncoming(new Runnable() {
                @Override
                public String toString() {
                    return "Server Closed EOF. " + MockedTcpClientConnection.this;
                }

                @Override
                public void run() {
                    MockedTcpClientConnection.this.close(reason, new TargetDisconnectedException("Mocked Remote socket closed"));
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

    private class MockedServerConnection extends MockServerConnection {

        private final AtomicBoolean alive = new AtomicBoolean(true);

        private final MockedTcpClientConnection responseConnection;
        private final int connectionId;

        private volatile long lastReadTimeMillis;
        private volatile long lastWriteTimeMillis;
        private volatile String connectionType;

        MockedServerConnection(
                int connectionId,
                Address localEndpointAddress,
                Address remoteEndpointAddress,
                UUID localEndpointUuid,
                UUID remoteEndpointUuid,
                NodeEngineImpl localNodeEngine,
                NodeEngineImpl remoteNodeEngine,
                MockedTcpClientConnection responseConnection
        ) {
            super(localEndpointAddress, remoteEndpointAddress, localEndpointUuid, remoteEndpointUuid,
                    localNodeEngine, remoteNodeEngine);
            this.responseConnection = responseConnection;
            this.connectionId = connectionId;
            lastReadTimeMillis = System.currentTimeMillis();
            lastWriteTimeMillis = System.currentTimeMillis();
        }

        @Override
        public boolean write(OutboundFrame frame) {
            final ClientMessage clientMessage = (ClientMessage) frame;
            if (isAlive()) {
                lastWriteTimeMillis = System.currentTimeMillis();
                ClientMessage newClientMessage = readFromPacket(clientMessage);
                newClientMessage.setConnection(responseConnection);
                responseConnection.handleClientMessage(newClientMessage);
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

            MockedServerConnection that = (MockedServerConnection) o;

            if (connectionId != that.connectionId) {
                return false;
            }
            Address remoteAddress = getRemoteAddress();
            return !(remoteAddress != null ? !remoteAddress.equals(that.getRemoteAddress()) : that.getRemoteAddress() != null);
        }

        @Override
        public void close(String reason, Throwable cause) {
            if (!alive.compareAndSet(true, false)) {
                return;
            }

            Logger.getLogger(MockedServerConnection.class).warning("Server connection closed: " + reason, cause);
            super.close(reason, cause);
            responseConnection.onServerClose(reason);
        }

        @Override
        public int hashCode() {
            int result = connectionId;
            Address remoteAddress = getRemoteAddress();
            result = 31 * result + (remoteAddress != null ? remoteAddress.hashCode() : 0);
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
        public void setConnectionType(String connectionType) {
            this.connectionType = connectionType;
        }

        @Override
        public String getConnectionType() {
            return connectionType;
        }

        @Override
        public String toString() {
            return "MockedNodeConnection{"
                    + " remoteAddress = " + getRemoteAddress()
                    + ", localAddress = " + localAddress
                    + ", connectionId = " + connectionId
                    + '}';
        }
    }
}
