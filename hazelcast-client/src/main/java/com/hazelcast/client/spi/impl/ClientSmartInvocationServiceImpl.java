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

package com.hazelcast.client.spi.impl;

import com.hazelcast.client.LoadBalancer;
import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.spi.ClientClusterService;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.Member;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.exception.TargetNotMemberException;

import java.io.IOException;

public final class ClientSmartInvocationServiceImpl extends ClientInvocationServiceSupport {

    private final LoadBalancer loadBalancer;

    public ClientSmartInvocationServiceImpl(HazelcastClientInstanceImpl client, LoadBalancer loadBalancer) {
        super(client);
        this.loadBalancer = loadBalancer;
    }

    public void invokeOnPartitionOwner(ClientInvocation invocation, int partitionId) throws IOException {
        final Address owner = partitionService.getPartitionOwner(partitionId);
        if (owner == null) {
            throw new IOException("Partition does not have owner. partitionId : " + partitionId);
        }
        invocation.getClientMessage().setPartitionId(partitionId);
        Connection connection = getConnection(owner);
        send(invocation, (ClientConnection) connection);
    }

    @Override
    public void invokeOnRandomTarget(ClientInvocation invocation) throws IOException {
        final Address randomAddress = getRandomAddress();
        if (randomAddress == null) {
            throw new IOException("Not address found to invoke ");
        }
        final Connection connection = getConnection(randomAddress);
        send(invocation, (ClientConnection) connection);
    }

    @Override
    public void invokeOnTarget(ClientInvocation invocation, Address target)
            throws IOException {
        if (target == null) {
            throw new NullPointerException("Target can not be null");
        }
        if (!isMember(target)) {
            throw new TargetNotMemberException("Target :  " + target + " is not member. ");
        }
        final Connection connection = getConnection(target);
        invokeOnConnection(invocation, (ClientConnection) connection);
    }

    private Connection getConnection(Address target) throws IOException {
        ensureOwnerConnectionAvailable();
        Connection connection = connectionManager.getOrTriggerConnect(target, false);
        if (connection == null) {
            throw new IOException("No available connection to address " + target);
        }
        return connection;
    }

    private void ensureOwnerConnectionAvailable() throws IOException {
        ClientClusterService clientClusterService = client.getClientClusterService();
        Address ownerConnectionAddress = clientClusterService.getOwnerConnectionAddress();

        boolean isOwnerConnectionAvailable = ownerConnectionAddress != null
                && connectionManager.getConnection(ownerConnectionAddress) != null;

        if (!isOwnerConnectionAvailable) {
            if (isShutdown()) {
                throw new HazelcastException("ConnectionManager is not active!");
            }
            throw new IOException("Not able to setup owner connection!");
        }
    }

    @Override
    public void invokeOnConnection(ClientInvocation invocation, ClientConnection connection) throws IOException {
        send(invocation, connection);
    }

    private Address getRandomAddress() {
        Member member = loadBalancer.next();
        if (member != null) {
            return member.getAddress();
        }
        return null;
    }

    private boolean isMember(Address target) {
        final Member member = client.getClientClusterService().getMember(target);
        return member != null;
    }

}
