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

package com.hazelcast.client.impl.protocol.task.management;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MCChangeWanReplicationStateCodec;
import com.hazelcast.client.impl.protocol.codec.MCChangeWanReplicationStateCodec.RequestParameters;
import com.hazelcast.client.impl.protocol.task.AbstractInvocationMessageTask;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.management.operation.ChangeWanStateOperation;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.spi.impl.operationservice.InvocationBuilder;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.wan.WanPublisherState;
import com.hazelcast.wan.impl.WanReplicationService;

import java.security.Permission;

public class ChangeWanReplicationStateMessageTask extends AbstractInvocationMessageTask<RequestParameters> {
    public ChangeWanReplicationStateMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected InvocationBuilder getInvocationBuilder(Operation op) {
        return nodeEngine.getOperationService().createInvocationBuilder(getServiceName(),
                op, nodeEngine.getThisAddress());
    }

    @Override
    protected Operation prepareOperation() {
        WanPublisherState state = WanPublisherState.getByType(parameters.newState);
        if (state == null) {
            throw new IllegalArgumentException("Unexpected WAN publisher state = [" + parameters.newState + "]");
        }
        return new ChangeWanStateOperation(
                parameters.wanReplicationName,
                parameters.wanPublisherId,
                state);
    }

    @Override
    protected RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MCChangeWanReplicationStateCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MCChangeWanReplicationStateCodec.encodeResponse();
    }

    @Override
    public String getServiceName() {
        return WanReplicationService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    public String getDistributedObjectName() {
        return null;
    }

    @Override
    public String getMethodName() {
        return "changeWanReplicationState";
    }

    @Override
    public Object[] getParameters() {
        return new Object[] {parameters.wanReplicationName, parameters.wanPublisherId, parameters.newState};
    }

    @Override
    public boolean isManagementTask() {
        return true;
    }
}
