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
        invocation.getClientMessage().setPartitionId(partitionId);
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
