/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.sql.impl.client.SqlClientService;

import java.security.Permission;

public class NoSuchMessageTask extends AbstractMessageTask<ClientMessage> {

    public NoSuchMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected ClientMessage decodeClientMessage(ClientMessage clientMessage) {
        return clientMessage;
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return null;
    }

    @Override
    protected void processMessage() {
        String message = createMessage();
        logger.finest(message);
        throw new UnsupportedOperationException(message);
    }

    @Override
    protected boolean requiresAuthentication() {
        return false;
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

    private String createMessage() {
        int messageType = parameters.getMessageType();

        if (SqlClientService.isSqlMessage(messageType)) {
            // Special error message for the SQL beta service that do not maintain compatibility between versions.
            String memberVersion = nodeEngine.getVersion().toString();
            String clientVersion = endpoint.getClientVersion();

            return "Cannot process SQL client operation due to version mismatch "
                + "(please ensure that a client and a member have the same version) "
                + "[memberVersion=" + memberVersion + ", clientVersion=" + clientVersion + ']';
        }

        return "Unrecognized client message received with type: 0x" + Integer.toHexString(messageType);
    }
}
