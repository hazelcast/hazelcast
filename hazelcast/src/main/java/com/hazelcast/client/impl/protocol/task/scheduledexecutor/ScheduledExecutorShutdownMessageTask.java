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

package com.hazelcast.client.impl.protocol.task.scheduledexecutor;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorShutdownCodec;
import com.hazelcast.client.impl.protocol.task.AbstractInvocationMessageTask;
import com.hazelcast.cluster.Member;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.scheduledexecutor.impl.DistributedScheduledExecutorService;
import com.hazelcast.scheduledexecutor.impl.operations.ShutdownOperation;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.ScheduledExecutorPermission;
import com.hazelcast.spi.impl.operationservice.InvocationBuilder;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;

import java.security.Permission;

public class ScheduledExecutorShutdownMessageTask
        extends AbstractInvocationMessageTask<ScheduledExecutorShutdownCodec.RequestParameters> {

    public ScheduledExecutorShutdownMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected InvocationBuilder getInvocationBuilder(Operation op) {
        final OperationServiceImpl operationService = nodeEngine.getOperationService();
        Member member = nodeEngine.getClusterService().getMember(parameters.memberUuid);
        return operationService.createInvocationBuilder(getServiceName(), op, member.getAddress());
    }

    @Override
    protected Operation prepareOperation() {
        Operation op = new ShutdownOperation(parameters.schedulerName);
        op.setCallerUuid(endpoint.getUuid());
        return op;
    }

    @Override
    protected ScheduledExecutorShutdownCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return ScheduledExecutorShutdownCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return ScheduledExecutorShutdownCodec.encodeResponse();
    }

    @Override
    public String getServiceName() {
        return DistributedScheduledExecutorService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new ScheduledExecutorPermission(parameters.schedulerName, ActionConstants.ACTION_MODIFY);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.schedulerName;
    }

    @Override
    public String getMethodName() {
        return "shutdown";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.schedulerName};
    }
}
