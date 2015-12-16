package com.hazelcast.instance;

import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.cluster.Joiner;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.wan.WanReplicationService;

import java.net.UnknownHostException;
import java.nio.channels.ServerSocketChannel;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestNodeContext implements NodeContext {

    private final Address address;
    private final NodeExtension nodeExtension = mock(NodeExtension.class);
    private final ConnectionManager connectionManager;

    public TestNodeContext() throws UnknownHostException {
        this(mock(ConnectionManager.class));
    }

    public TestNodeContext(ConnectionManager connectionManager) throws UnknownHostException {
        this(new Address("127.0.0.1", 5000), connectionManager);
    }

    public TestNodeContext(Address address, ConnectionManager connectionManager) {
        this.address = address;
        this.connectionManager = connectionManager;
    }

    public NodeExtension getNodeExtension() {
        return nodeExtension;
    }

    @Override
    public NodeExtension createNodeExtension(Node node) {
        when(nodeExtension.createService(MapService.class)).thenReturn(mock(MapService.class));
        when(nodeExtension.createService(ICacheService.class)).thenReturn(mock(ICacheService.class));
        when(nodeExtension.createService(WanReplicationService.class)).thenReturn(mock(WanReplicationService.class));
        when(nodeExtension.createSerializationService()).thenReturn(new DefaultSerializationServiceBuilder().build());
        return nodeExtension;
    }

    @Override
    public AddressPicker createAddressPicker(Node node) {
        return new TestAddressPicker(address);
    }

    @Override
    public Joiner createJoiner(Node node) {
        return null;
    }

    @Override
    public ConnectionManager createConnectionManager(Node node, ServerSocketChannel serverSocketChannel) {
        return connectionManager;
    }

    static class TestAddressPicker implements AddressPicker {

        final Address address;

        TestAddressPicker(Address address) {
            this.address = address;
        }

        @Override
        public void pickAddress() throws Exception {
        }

        @Override
        public Address getBindAddress() {
            return address;
        }

        @Override
        public Address getPublicAddress() {
            return address;
        }

        @Override
        public ServerSocketChannel getServerSocketChannel() {
            return null;
        }
    }
}
