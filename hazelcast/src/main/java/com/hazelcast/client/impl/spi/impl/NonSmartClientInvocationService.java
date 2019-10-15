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

package com.hazelcast.client.impl.spi.impl;

import com.hazelcast.client.impl.connection.nio.ClientConnection;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.cluster.Address;

import java.io.IOException;

public class NonSmartClientInvocationService extends AbstractClientInvocationService {

    public NonSmartClientInvocationService(HazelcastClientInstanceImpl client) {
        super(client);
    }

    @Override
    public void invokeOnRandomTarget(ClientInvocation invocation) throws IOException {
        send(invocation, getConnection());
    }

    @Override
    public void invokeOnConnection(ClientInvocation invocation, ClientConnection connection) throws IOException {
        assert connection != null;
        send(invocation, connection);
    }

    @Override
    public void invokeOnPartitionOwner(ClientInvocation invocation, int partitionId) throws IOException {
        invocation.getClientMessage().setPartitionId(partitionId);
        send(invocation, getConnection());
    }

    @Override
    public void invokeOnTarget(ClientInvocation invocation, Address target) throws IOException {
        send(invocation, getConnection());
    }

    private ClientConnection getConnection() throws IOException {
        ClientConnection connection = connectionManager.getActiveConnections().iterator().next();
        if (connection == null) {
            throw new IOException("NonSmartClientInvocationService: No connection is available.");
        }
        return connection;
    }
}
