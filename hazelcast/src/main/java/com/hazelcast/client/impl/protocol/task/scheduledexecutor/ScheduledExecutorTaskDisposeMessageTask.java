/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorDisposeCodec;
import com.hazelcast.client.impl.protocol.task.AbstractInvocationMessageTask;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.scheduledexecutor.ScheduledTaskHandler;
import com.hazelcast.scheduledexecutor.impl.DistributedScheduledExecutorService;
import com.hazelcast.scheduledexecutor.impl.operations.DisposeTaskOperation;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.ScheduledExecutorPermission;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;

import java.security.Permission;

public class ScheduledExecutorTaskDisposeMessageTask
        extends AbstractInvocationMessageTask<ScheduledExecutorDisposeCodec.RequestParameters> {

    public ScheduledExecutorTaskDisposeMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected InvocationBuilder getInvocationBuilder(Operation op) {
        final InternalOperationService operationService = nodeEngine.getOperationService();
        final ScheduledTaskHandler handler = ScheduledTaskHandler.of(parameters.handlerUrn);
        if (handler.getAddress() != null) {
            return operationService.createInvocationBuilder(getServiceName(), op, handler.getAddress());
        } else {
            return operationService.createInvocationBuilder(getServiceName(), op, handler.getPartitionId());
        }
    }

    @Override
    protected Operation prepareOperation() {
        Operation op = new DisposeTaskOperation(ScheduledTaskHandler.of(parameters.handlerUrn));
        op.setPartitionId(getPartitionId());
        return op;
    }

    @Override
    protected ScheduledExecutorDisposeCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return ScheduledExecutorDisposeCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return ScheduledExecutorDisposeCodec.encodeResponse();
    }

    @Override
    public String getServiceName() {
        return DistributedScheduledExecutorService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        final ScheduledTaskHandler handler = ScheduledTaskHandler.of(parameters.handlerUrn);
        return new ScheduledExecutorPermission(handler.getSchedulerName(), ActionConstants.ACTION_MODIFY);
    }

    @Override
    public String getDistributedObjectName() {
        ScheduledTaskHandler handler = ScheduledTaskHandler.of(parameters.handlerUrn);
        return handler.getTaskName();
    }

    @Override
    public String getMethodName() {
        return "dispose";
    }

    @Override
    public Object[] getParameters() {
        return new Object[] { parameters.handlerUrn };
    }
}
