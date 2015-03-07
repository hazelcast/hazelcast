package com.hazelcast.client.spi.impl;

import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.spi.ClientClusterService;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;

import java.io.IOException;

public class ClientNonSmartInvocationServiceImpl extends ClientInvocationServiceSupport {

    public ClientNonSmartInvocationServiceImpl(HazelcastClientInstanceImpl client) {
        super(client);
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
        ClientClusterService clusterService = client.getClientClusterService();
        Address ownerConnectionAddress = clusterService.getOwnerConnectionAddress();
        if (ownerConnectionAddress == null) {
            throw new IOException("Packet is not send to owner address");
        }
        Connection conn = connectionManager.getConnection(ownerConnectionAddress);
        if (conn == null) {
            throw new IOException("Packet is not send to owner address :" + ownerConnectionAddress);
        }
        send(invocation, (ClientConnection) conn);
    }
}
