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

package com.hazelcast.cp.internal.session.client;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CPSessionHeartbeatSessionCodec;
import com.hazelcast.client.impl.protocol.codec.CPSessionHeartbeatSessionCodec.RequestParameters;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.client.AbstractCPMessageTask;
import com.hazelcast.cp.internal.session.operation.HeartbeatSessionOp;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;

import java.security.Permission;

/**
 * Client message task for {@link HeartbeatSessionOp}
 */
public class HeartbeatSessionMessageTask extends AbstractCPMessageTask<RequestParameters> {

    public HeartbeatSessionMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected void processMessage() {
        invoke(parameters.groupId, new HeartbeatSessionOp(parameters.sessionId));
    }

    @Override
    protected CPSessionHeartbeatSessionCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return CPSessionHeartbeatSessionCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return CPSessionHeartbeatSessionCodec.encodeResponse();
    }

    @Override
    public String getServiceName() {
        return RaftService.SERVICE_NAME;
    }

    @Override
    public String getDistributedObjectName() {
        return null;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    public String getMethodName() {
        return null;
    }

    @Override
    public Object[] getParameters() {
        return new Object[0];
    }
}
