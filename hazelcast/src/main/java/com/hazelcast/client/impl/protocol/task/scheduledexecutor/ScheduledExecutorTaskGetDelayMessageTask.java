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

package com.hazelcast.client.impl.protocol.task.scheduledexecutor;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorGetDelayCodec;
import com.hazelcast.client.impl.protocol.task.AbstractInvocationMessageTask;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.scheduledexecutor.impl.DistributedScheduledExecutorService;
import com.hazelcast.scheduledexecutor.impl.operations.GetDelayOperation;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.ScheduledExecutorPermission;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;

import java.security.Permission;

public class ScheduledExecutorTaskGetDelayMessageTask
        extends AbstractInvocationMessageTask<ScheduledExecutorGetDelayCodec.RequestParameters> {

    public ScheduledExecutorTaskGetDelayMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected InvocationBuilder getInvocationBuilder(Operation op) {
        final InternalOperationService operationService = nodeEngine.getOperationService();
        if (parameters.handler.getAddress() != null) {
            return operationService.createInvocationBuilder(getServiceName(), op, parameters.handler.getAddress());
        } else {
            return operationService.createInvocationBuilder(getServiceName(), op, parameters.handler.getPartitionId());
        }
    }

    @Override
    protected Operation prepareOperation() {
        Operation op = new GetDelayOperation(parameters.handler, parameters.unit);
        op.setPartitionId(getPartitionId());
        return op;
    }

    @Override
    protected ScheduledExecutorGetDelayCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return ScheduledExecutorGetDelayCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return ScheduledExecutorGetDelayCodec.encodeResponse((Long) response);
    }

    @Override
    public String getServiceName() {
        return DistributedScheduledExecutorService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new ScheduledExecutorPermission(parameters.handler.getSchedulerName(), ActionConstants.ACTION_MODIFY);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.handler.getTaskName();
    }

    @Override
    public String getMethodName() {
        return "getDelay";
    }

    @Override
    public Object[] getParameters() {
        return new Object[] { parameters.handler, parameters.unit };
    }
}
