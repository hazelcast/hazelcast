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

import com.hazelcast.client.LoadBalancer;
import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.impl.ClusterAuthenticator;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.security.Credentials;

import java.io.IOException;

public final class ClientSmartInvocationServiceImpl extends ClientInvocationServiceSupport {

    private final LoadBalancer loadBalancer;
    private final Credentials credentials;
    private final ClusterAuthenticator authenticator;

    public ClientSmartInvocationServiceImpl(HazelcastClientInstanceImpl client, LoadBalancer loadBalancer) {
        super(client);
        this.loadBalancer = loadBalancer;
        credentials = client.getCredentials();
        authenticator = new ClusterAuthenticator(client, credentials);
    }

    public void invokeOnPartitionOwner(ClientInvocation invocation, int partitionId) throws IOException {
        final Address owner = partitionService.getPartitionOwner(partitionId);
        if (owner == null) {
            throw new IOException("Partition does not have owner. partitionId : " + partitionId);
        }
        ClientConnection connection =
                (ClientConnection) connectionManager.getOrConnect(owner, authenticator);
        send(invocation, connection);
    }

    @Override
    public void invokeOnRandomTarget(ClientInvocation invocation) throws IOException {
        final Address randomAddress = getRandomAddress();
        if (randomAddress == null) {
            throw new IOException("Not address found to invoke ");
        }
        final Connection connection = connectionManager.getOrConnect(randomAddress, authenticator);
        send(invocation, (ClientConnection) connection);
    }

    @Override
    public void invokeOnTarget(ClientInvocation invocation, Address target)
            throws IOException {
        if (target == null) {
            throw new NullPointerException("Target can not be null");
        }
        if (!isMember(target)) {
            throw new IOException("Target :  " + target + " is not member. ");
        }
        final Connection connection = connectionManager.getOrConnect(target, authenticator);
        invokeOnConnection(invocation, (ClientConnection) connection);
    }


    @Override
    public void invokeOnConnection(ClientInvocation invocation, ClientConnection connection) throws IOException {
        send(invocation, connection);
    }

    private Address getRandomAddress() {
        MemberImpl member = (MemberImpl) loadBalancer.next();
        if (member != null) {
            return member.getAddress();
        }
        return null;
    }

    private boolean isMember(Address target) {
        final MemberImpl member = client.getClientClusterService().getMember(target);
        return member != null;
    }

}
