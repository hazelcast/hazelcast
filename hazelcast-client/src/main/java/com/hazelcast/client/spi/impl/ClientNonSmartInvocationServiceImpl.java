package com.hazelcast.client.spi.impl;

import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;

import java.io.IOException;

public class ClientNonSmartInvocationServiceImpl extends ClientInvocationServiceSupport {

    private final ClusterListenerSupport clusterListenerSupport;

    public ClientNonSmartInvocationServiceImpl(HazelcastClientInstanceImpl client) {
        super(client);
        final ClientClusterServiceImpl clusterService = (ClientClusterServiceImpl) client.getClientClusterService();
        clusterListenerSupport = clusterService.getClusterListenerSupport();
    }

    @Override
    public void invokeOnRandomTarget(ClientInvocation invocation) throws IOException {
        sendToOwner(invocation);
    }

    @Override
    public void invokeOnConnection(ClientInvocation invocation, ClientConnection connection) throws IOException {
        if (connection == null) {
            throw new NullPointerException("Connection can not be null");
        }
        if (!connection.isAlive()) {
            throw new IllegalStateException("Connection is not active");
        }
        send(invocation, connection);
    }

    @Override
    public void invokeOnPartitionOwner(ClientInvocation invocation, int partitionId) throws IOException {
        sendToOwner(invocation);
    }

    @Override
    public void invokeOnTarget(ClientInvocation invocation, Address target) throws IOException {
        sendToOwner(invocation);
    }

    private void sendToOwner(ClientInvocation invocation) throws IOException {
        final Address ownerConnectionAddress = clusterListenerSupport.getOwnerConnectionAddress();
        if (ownerConnectionAddress == null) {
            throw new IOException("Packet is not send to owner address");
        }
        final Connection conn = connectionManager.getConnection(ownerConnectionAddress);
        if (conn == null) {
            throw new IOException("Packet is not send to owner address :" + ownerConnectionAddress);
        }
        send(invocation, (ClientConnection) conn);
    }
}
