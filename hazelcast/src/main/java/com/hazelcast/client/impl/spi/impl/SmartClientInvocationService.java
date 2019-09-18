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

import com.hazelcast.client.LoadBalancer;
import com.hazelcast.client.impl.connection.nio.ClientConnection;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.cluster.Member;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.exception.TargetNotMemberException;

import java.io.IOException;

public class SmartClientInvocationService extends AbstractClientInvocationService {

    private final LoadBalancer loadBalancer;

    public SmartClientInvocationService(HazelcastClientInstanceImpl client, LoadBalancer loadBalancer) {
        super(client);
        this.loadBalancer = loadBalancer;
    }

    @Override
    public void invokeOnPartitionOwner(ClientInvocation invocation, int partitionId) throws IOException {
        final Address owner = partitionService.getPartitionOwner(partitionId);
        if (owner == null) {
            throw new IOException("Partition does not have an owner. partitionId: " + partitionId);
        }
        if (!isMember(owner)) {
            throw new TargetNotMemberException("Partition owner '" + owner + "' is not a member.");
        }
        invocation.getClientMessage().setPartitionId(partitionId);
        Connection connection = getOrTriggerConnect(owner, invocation.getClientMessage().acquiresResource());
        send(invocation, (ClientConnection) connection);
    }

    @Override
    public void invokeOnRandomTarget(ClientInvocation invocation) throws IOException {
        final Address randomAddress = getRandomAddress();
        if (randomAddress == null) {
            throw new IOException("No address found to invoke");
        }
        Connection connection = getOrTriggerConnect(randomAddress, invocation.getClientMessage().acquiresResource());
        send(invocation, (ClientConnection) connection);
    }

    @Override
    public void invokeOnTarget(ClientInvocation invocation, Address target) throws IOException {
        assert (target != null);
        if (!isMember(target)) {
            throw new TargetNotMemberException("Target '" + target + "' is not a member.");
        }
        Connection connection = getOrTriggerConnect(target, invocation.getClientMessage().acquiresResource());
        invokeOnConnection(invocation, (ClientConnection) connection);
    }

    private Connection getOrTriggerConnect(Address target, boolean acquiresResource) throws IOException {
        Connection connection = connectionManager.getOrTriggerConnect(target, acquiresResource);
        if (connection == null) {
            throw new IOException("No available connection to address " + target);
        }
        return connection;
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

    boolean isMember(Address target) {
        final Member member = client.getClientClusterService().getMember(target);
        return member != null;
    }
}
