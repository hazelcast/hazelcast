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

import com.hazelcast.client.impl.client.ClientPrincipal;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientAuthenticationCustomCodec;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;

import java.security.Permission;

/**
 * Custom Authentication with custom credential impl
 */
public class AuthenticationCustomCredentialsMessageTask
        extends AuthenticationBaseMessageTask<ClientAuthenticationCustomCodec.RequestParameters> {

    public AuthenticationCustomCredentialsMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected boolean isOwnerConnection() {
        return parameters.isOwnerConnection;
    }

    @Override
    protected String getClientType() {
        return parameters.clientType;
    }

    @Override
    protected ClientAuthenticationCustomCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        ClientAuthenticationCustomCodec.RequestParameters parameters =
                ClientAuthenticationCustomCodec.decodeRequest(clientMessage);
        String uuid = parameters.uuid;
        String ownerUuid = parameters.ownerUuid;
        if (uuid != null && uuid.length() > 0) {
            principal = new ClientPrincipal(uuid, ownerUuid);
        }
        credentials = serializationService.toObject(parameters.credentials);
        return parameters;
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return (ClientMessage) response;
    }


    @Override
    protected ClientMessage encodeAuth(Address thisAddress, String uuid, String ownerUuid) {
        return ClientAuthenticationCustomCodec.encodeResponse(thisAddress, uuid, ownerUuid);
    }

    @Override
    public String getServiceName() {
        return null;
    }

    @Override
    public String getDistributedObjectName() {
        return null;
    }

    @Override
    public String getMethodName() {
        return null;
    }

    @Override
    public Object[] getParameters() {
        return null;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }
}
