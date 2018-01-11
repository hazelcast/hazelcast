/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.Address;

import java.io.IOException;

public class NonSmartClientInvocationService extends AbstractClientInvocationService {

    public NonSmartClientInvocationService(HazelcastClientInstanceImpl client) {
        super(client);
    }

    @Override
    public void invokeOnRandomTarget(ClientInvocation invocation) throws IOException {
        send(invocation, getOwnerConnection());
    }

    @Override
    public void invokeOnConnection(ClientInvocation invocation, ClientConnection connection) throws IOException {
        assert connection != null;
        send(invocation, connection);
    }

    @Override
    public void invokeOnPartitionOwner(ClientInvocation invocation, int partitionId) throws IOException {
        invocation.getClientMessage().setPartitionId(partitionId);
        send(invocation, getOwnerConnection());
    }

    @Override
    public void invokeOnTarget(ClientInvocation invocation, Address target) throws IOException {
        send(invocation, getOwnerConnection());
    }

    private ClientConnection getOwnerConnection() throws IOException {
        Address ownerConnectionAddress = connectionManager.getOwnerConnectionAddress();
        ClientConnection ownerConnection = (ClientConnection) connectionManager.getActiveConnection(ownerConnectionAddress);
        if (ownerConnection == null) {
            throw new IOException("NonSmartClientInvocationService: Owner connection is not available.");
        }
        return ownerConnection;
    }
}
