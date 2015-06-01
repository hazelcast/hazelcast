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

package com.hazelcast.client.impl.protocol.task;

import com.hazelcast.client.AuthenticationException;
import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.impl.ClientEndpointImpl;
import com.hazelcast.client.impl.client.ClientPrincipal;
import com.hazelcast.client.impl.operations.ClientReAuthOperation;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.security.Credentials;
import com.hazelcast.util.UuidUtil;

import java.security.Permission;
import java.util.Collection;
import java.util.Set;

/**
 * Base authentication task
 */
public abstract class AuthenticationBaseMessageTask<P>
        extends AbstractCallableMessageTask<P> {

    protected transient ClientPrincipal principal;
    protected transient Credentials credentials;

    public AuthenticationBaseMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected ClientEndpointImpl getEndpoint() {
        if (connection.isAlive()) {
            return new ClientEndpointImpl(clientEngine, connection);
        } else {
            handleEndpointNotCreatedConnectionNotAlive();
        }
        return null;
    }

    @Override
    protected boolean isAuthenticationMessage() {
        return true;
    }

    private void handleEndpointNotCreatedConnectionNotAlive() {
        logger.warning("Dropped: " + clientMessage + " -> endpoint not created for AuthenticationRequest, connection not alive");
    }

    @Override
    public Object call() {
        boolean authenticated = authenticate();

        if (authenticated) {
            return handleAuthenticated();
        } else {
            return handleUnauthenticated();
        }
    }

    protected abstract boolean authenticate();

    private Object handleUnauthenticated() {
        throw new AuthenticationException("Invalid credentials!");
    }

    private Object handleAuthenticated() {
        if (isOwnerConnection()) {
            final String uuid = getUuid();
            final String localMemberUUID = clientEngine.getLocalMember().getUuid();

            principal = new ClientPrincipal(uuid, localMemberUUID);
            reAuthLocal();
            Collection<MemberImpl> members = nodeEngine.getClusterService().getMemberList();
            for (MemberImpl member : members) {
                if (!member.localMember()) {
                    ClientReAuthOperation op = new ClientReAuthOperation(uuid);
                    op.setCallerUuid(localMemberUUID);
                    nodeEngine.getOperationService().send(op, member.getAddress());
                }
            }
        }

        boolean isMember = clientEngine.getClusterService().getMember(principal.getOwnerUuid()) == null;
        if (isMember) {
            throw new AuthenticationException("Invalid owner-uuid: " + principal.getOwnerUuid()
                    + ", it's not member of this cluster!");
        }

        endpoint.authenticated(principal, credentials, isOwnerConnection());
        endpointManager.registerEndpoint(endpoint);
        clientEngine.bind(endpoint);

        final Address thisAddress = clientEngine.getThisAddress();
        return encodeAuth(thisAddress, principal.getUuid(), principal.getOwnerUuid());
    }

    protected abstract ClientMessage encodeAuth(Address thisAddress, String uuid, String ownerUuid);

    protected abstract boolean isOwnerConnection();

    private String getUuid() {
        if (principal != null) {
            return principal.getUuid();
        }
        return UuidUtil.createClientUuid(endpoint.getConnection().getEndPoint());
    }

    private void reAuthLocal() {
        final Set<ClientEndpoint> endpoints = endpointManager.getEndpoints(principal.getUuid());
        for (ClientEndpoint endpoint : endpoints) {
            endpoint.authenticated(principal);
        }
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }
}
