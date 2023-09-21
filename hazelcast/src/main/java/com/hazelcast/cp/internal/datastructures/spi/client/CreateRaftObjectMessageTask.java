/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.datastructures.spi.client;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CPGroupCreateCPObjectCodec;
import com.hazelcast.cp.internal.client.AbstractCPMessageTask;
import com.hazelcast.cp.internal.datastructures.spi.operation.CreateOrGetRaftObjectOp;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;

import java.security.Permission;
import java.util.UUID;

/**
 * Client message task for Raft group creation
 */
public class CreateRaftObjectMessageTask extends AbstractCPMessageTask<CPGroupCreateCPObjectCodec.RequestParameters> {

    public CreateRaftObjectMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected void processMessage() {
        String clientVersion = endpoint.getClientVersion();
        // System.out.println("CreateRaftObjectMessageTask clientVersion: " + clientVersion);

        invoke(parameters.groupId, new CreateOrGetRaftObjectOp(parameters.serviceName, parameters.name));
    }

    @Override
    protected CPGroupCreateCPObjectCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return CPGroupCreateCPObjectCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return CPGroupCreateCPObjectCodec.encodeResponse((UUID) response);
    }

    @Override
    public String getServiceName() {
        return parameters.serviceName;
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    public String getMethodName() {
        return "createRaftObject";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters};
    }
}
