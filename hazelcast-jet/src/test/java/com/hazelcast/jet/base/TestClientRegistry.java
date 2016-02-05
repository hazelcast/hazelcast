package com.hazelcast.jet.base;


import java.io.IOException;
import java.net.InetAddress;
import java.util.logging.Level;

import com.hazelcast.nio.Address;

import java.net.InetSocketAddress;

import com.hazelcast.instance.Node;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Connection;

import java.net.UnknownHostException;

import com.hazelcast.logging.ILogger;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.nio.OutboundFrame;
import com.hazelcast.nio.ConnectionType;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientAwsConfig;
import com.hazelcast.test.mocknetwork.MockConnection;
import com.hazelcast.test.mocknetwork.TestNodeRegistry;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.connection.AddressTranslator;
import com.hazelcast.client.spi.impl.AwsAddressTranslator;
import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.spi.impl.DefaultAddressTranslator;
import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.spi.discovery.integration.DiscoveryService;
import com.hazelcast.client.impl.ClientConnectionManagerFactory;
import com.hazelcast.client.connection.nio.ClientConnectionManagerImpl;
import com.hazelcast.client.spi.impl.discovery.DiscoveryAddressTranslator;


public class TestClientRegistry {
    private static final ILogger LOGGER = Logger.getLogger(HazelcastClient.class);
    private final TestNodeRegistry nodeRegistry;

    public TestClientRegistry(TestNodeRegistry nodeRegistry) {
        this.nodeRegistry = nodeRegistry;
    }


    ClientConnectionManagerFactory createClientServiceFactory(Address clientAddress) {
        return new MockClientConnectionManagerFactory(clientAddress);
    }

    private class MockClientConnectionManagerFactory implements ClientConnectionManagerFactory {

        private final Address clientAddress;

        public MockClientConnectionManagerFactory(Address clientAddress) {
            this.clientAddress = clientAddress;
        }

        @Override
        public ClientConnectionManager createConnectionManager(ClientConfig config, HazelcastClientInstanceImpl client,
                                                               DiscoveryService discoveryService) {

            final ClientAwsConfig awsConfig = config.getNetworkConfig().getAwsConfig();
            AddressTranslator addressTranslator;
            if (awsConfig != null && awsConfig.isEnabled()) {
                try {
                    addressTranslator = new AwsAddressTranslator(awsConfig);
                } catch (NoClassDefFoundError e) {
                    LOGGER.log(Level.WARNING, "hazelcast-cloud.jar might be missing!");
                    throw e;
                }
            } else if (discoveryService != null) {
                addressTranslator = new DiscoveryAddressTranslator(discoveryService);
            } else {
                addressTranslator = new DefaultAddressTranslator();
            }
            return new MockClientConnectionManager(client, addressTranslator, clientAddress);
        }
    }


    private class MockClientConnectionManager extends ClientConnectionManagerImpl {

        private final Address clientAddress;
        private final HazelcastClientInstanceImpl client;

        public MockClientConnectionManager(HazelcastClientInstanceImpl client, AddressTranslator addressTranslator,
                                           Address clientAddress) {
            super(client, addressTranslator);
            this.client = client;
            this.clientAddress = clientAddress;
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
                return new MockedClientConnection(client, connectionIdGen.incrementAndGet(),
                        node.nodeEngine, address, clientAddress);
            } catch (Exception e) {
                throw ExceptionUtil.rethrow(e, IOException.class);
            }
        }


    }


    private class MockedClientConnection extends ClientConnection {
        private volatile long lastReadTime;
        private volatile long lastWriteTime;
        private final NodeEngineImpl serverNodeEngine;
        private final Address remoteAddress;
        private final Address localAddress;
        private final Connection serverSideConnection;

        public MockedClientConnection(HazelcastClientInstanceImpl client, int connectionId, NodeEngineImpl serverNodeEngine,
                                      Address address, Address localAddress) throws IOException {
            super(client, connectionId);
            this.serverNodeEngine = serverNodeEngine;
            this.remoteAddress = address;
            this.localAddress = localAddress;
            this.serverSideConnection = new MockedNodeConnection(connectionId, remoteAddress,
                    localAddress, serverNodeEngine, this);
        }

        void handleClientMessage(ClientMessage clientMessage) {
            lastReadTime = System.currentTimeMillis();
            getConnectionManager().handleClientMessage(clientMessage, this);
        }

        @Override
        public boolean write(OutboundFrame frame) {
            Node node = serverNodeEngine.getNode();
            if (!node.isRunning()) {
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
        public void init() throws IOException {

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
            serverSideConnection.close();
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
        public void close() {
            super.close();
            ClientConnectionManager connectionManager = responseConnection.getConnectionManager();
            connectionManager.destroyConnection(responseConnection);
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
