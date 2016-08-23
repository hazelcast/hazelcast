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
 */

package com.hazelcast.client.test;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientAwsConfig;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.connection.AddressTranslator;
import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.connection.nio.ClientConnectionManagerImpl;
import com.hazelcast.client.impl.ClientConnectionManagerFactory;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.spi.impl.AwsAddressTranslator;
import com.hazelcast.client.spi.impl.DefaultAddressTranslator;
import com.hazelcast.client.spi.impl.discovery.DiscoveryAddressTranslator;
import com.hazelcast.client.spi.properties.ClientProperty;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeState;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionType;
import com.hazelcast.nio.OutboundFrame;
import com.hazelcast.spi.discovery.integration.DiscoveryService;
import com.hazelcast.spi.exception.TargetDisconnectedException;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.mocknetwork.MockConnection;
import com.hazelcast.test.mocknetwork.TestNodeRegistry;
import com.hazelcast.util.ExceptionUtil;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

public class TestClientRegistry {

    private static final ILogger LOGGER = Logger.getLogger(HazelcastClient.class);
    private final TestNodeRegistry nodeRegistry;

    public TestClientRegistry(TestNodeRegistry nodeRegistry) {
        this.nodeRegistry = nodeRegistry;
    }

    public ClientConnectionManagerFactory createClientServiceFactory(String host, AtomicInteger ports) {
        return new MockClientConnectionManagerFactory(host, ports);
    }

    private class MockClientConnectionManagerFactory implements ClientConnectionManagerFactory {
        private final String host;
        private final AtomicInteger ports;

        public MockClientConnectionManagerFactory(String host, AtomicInteger ports) {
            this.host = host;
            this.ports = ports;
        }

        @Override
        public ClientConnectionManager createConnectionManager(ClientConfig config, HazelcastClientInstanceImpl client,
                                                               DiscoveryService discoveryService) {

            final ClientAwsConfig awsConfig = config.getNetworkConfig().getAwsConfig();
            AddressTranslator addressTranslator;
            if (awsConfig != null && awsConfig.isEnabled()) {
                try {
                    addressTranslator = new AwsAddressTranslator(awsConfig, client.getLoggingService());
                } catch (NoClassDefFoundError e) {
                    LOGGER.log(Level.WARNING, "hazelcast-cloud.jar might be missing!");
                    throw e;
                }
            } else if (discoveryService != null) {
                addressTranslator = new DiscoveryAddressTranslator(discoveryService,
                        client.getProperties().getBoolean(ClientProperty.DISCOVERY_SPI_PUBLIC_IP_ENABLED));
            } else {
                addressTranslator = new DefaultAddressTranslator();
            }
            return new MockClientConnectionManager(client, addressTranslator, host, ports);
        }
    }


    public class MockClientConnectionManager extends ClientConnectionManagerImpl {

        private final String host;
        private final AtomicInteger ports;
        private final HazelcastClientInstanceImpl client;
        private final Map<Address, State> stateMap = new ConcurrentHashMap<Address, State>();

        public MockClientConnectionManager(HazelcastClientInstanceImpl client, AddressTranslator addressTranslator,
                                           String host, AtomicInteger ports) {
            super(client, addressTranslator);
            this.client = client;
            this.host = host;
            this.ports = ports;
        }

        @Override
        protected void initializeSelectors(HazelcastClientInstanceImpl client) {

        }

        @Override
        protected void startSelectors() {

        }

        @Override
        protected void shutdownSelectors() {

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
                Node node = TestUtil.getNode(instance);
                Address localAddress = new Address(host, ports.incrementAndGet());
                MockedClientConnection connection = new MockedClientConnection(client,
                        connectionIdGen.incrementAndGet(), node.nodeEngine, address, localAddress, stateMap);
                LOGGER.info("Created connection to endpoint: " + address + ", connection: " + connection);
                return connection;
            } catch (Exception e) {
                throw ExceptionUtil.rethrow(e, IOException.class);
            }
        }

        /**
         * Stores incoming messages from address to a temporary queue
         * When unblocked first this queue will be processed after that new messages will be consumed
         *
         * @param address
         */
        public void block(Address address) {
            stateMap.put(address, State.BLOCKING);
        }

        /**
         * Drops incoming messages from address
         *
         * @param address
         */
        public void drop(Address address) {
            stateMap.put(address, State.DROPPING);
        }

        /**
         * Removes the filter that is put by either block or drop
         * Consumes from the temporary queue if there is anything then continues to normal behaviour
         *
         * @param address
         */
        public void unblock(Address address) {
            stateMap.remove(address);
        }
    }

    enum State {
        BLOCKING, DROPPING
    }

    public class MockedClientConnection extends ClientConnection {
        private volatile long lastReadTime;
        private volatile long lastWriteTime;
        private final NodeEngineImpl serverNodeEngine;
        private final Address remoteAddress;
        private final Address localAddress;
        private final Connection serverSideConnection;

        private final Queue<ClientMessage> incomingMessages = new ConcurrentLinkedQueue<ClientMessage>();
        private final Map<Address, State> stateMap;

        public MockedClientConnection(HazelcastClientInstanceImpl client, int connectionId, NodeEngineImpl serverNodeEngine,
                                      Address address, Address localAddress, Map<Address, State> stateMap) throws IOException {
            super(client, connectionId);
            this.serverNodeEngine = serverNodeEngine;
            this.remoteAddress = address;
            this.localAddress = localAddress;
            this.stateMap = stateMap;
            this.serverSideConnection = new MockedNodeConnection(connectionId, remoteAddress,
                    localAddress, serverNodeEngine, this);
        }

        @Override
        public void init() throws IOException {
            //No init for mock connections
        }

        void handleClientMessage(ClientMessage clientMessage) {
            if (getState() == State.DROPPING) {
                return;
            }

            if (getState() == State.BLOCKING) {
                incomingMessages.add(clientMessage);
                return;
            }
            ClientMessage message;
            while ((message = incomingMessages.poll()) != null) {
                lastReadTime = System.currentTimeMillis();
                getConnectionManager().handleClientMessage(message, this);
            }
            lastReadTime = System.currentTimeMillis();
            getConnectionManager().handleClientMessage(clientMessage, this);
        }

        private State getState() {
            return stateMap.get(remoteAddress);
        }

        @Override
        public boolean write(OutboundFrame frame) {
            Node node = serverNodeEngine.getNode();
            if (node.getState() == NodeState.SHUT_DOWN) {
                return false;
            }
            ClientMessage newPacket = readFromPacket((ClientMessage) frame);
            lastWriteTime = System.currentTimeMillis();
            node.clientEngine.handleClientMessage(newPacket, serverSideConnection);
            return true;
        }

        private ClientMessage readFromPacket(ClientMessage packet) {
            return ClientMessage.createForDecode(packet.buffer(), 0);
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
        protected void innerClose() throws IOException {
            serverSideConnection.close(null, null);
        }
    }

    private class MockedNodeConnection extends MockConnection {

        private final MockedClientConnection responseConnection;
        private final int connectionId;

        public MockedNodeConnection(int connectionId, Address localEndpoint, Address remoteEndpoint, NodeEngineImpl nodeEngine
                , MockedClientConnection responseConnection) {
            super(localEndpoint, remoteEndpoint, nodeEngine);
            this.responseConnection = responseConnection;
            this.connectionId = connectionId;
            register();
        }

        private void register() {
            Node node = nodeEngine.getNode();
            node.getConnectionManager().registerConnection(getEndPoint(), this);
        }

        @Override
        public boolean write(OutboundFrame frame) {
            final ClientMessage packet = (ClientMessage) frame;
            if (isAlive()) {
                ClientMessage newPacket = readFromPacket(packet);
                responseConnection.handleClientMessage(newPacket);
                return true;
            }
            return false;
        }

        @Override
        public boolean isClient() {
            return true;
        }

        private ClientMessage readFromPacket(ClientMessage packet) {
            return ClientMessage.createForDecode(packet.buffer(), 0);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            MockedNodeConnection that = (MockedNodeConnection) o;

            if (connectionId != that.connectionId) return false;
            Address remoteEndpoint = getEndPoint();
            return !(remoteEndpoint != null ? !remoteEndpoint.equals(that.getEndPoint()) : that.getEndPoint() != null);

        }

        @Override
        public void close(String reason, Throwable cause) {
            super.close(reason, cause);
            ClientConnectionManager connectionManager = responseConnection.getConnectionManager();
            connectionManager.destroyConnection(responseConnection, reason,
                    new TargetDisconnectedException("Mocked Remote socket closed"));
        }

        @Override
        public int hashCode() {
            int result = connectionId;
            Address remoteEndpoint = getEndPoint();
            result = 31 * result + (remoteEndpoint != null ? remoteEndpoint.hashCode() : 0);
            return result;
        }

        @Override
        public ConnectionType getType() {
            return ConnectionType.JAVA_CLIENT;
        }

        @Override
        public String toString() {
            return "MockedNodeConnection{" +
                    " remoteEndpoint = " + getEndPoint() +
                    ", localEndpoint = " + localEndpoint +
                    ", connectionId = " + connectionId +
                    '}';
        }
    }
}
