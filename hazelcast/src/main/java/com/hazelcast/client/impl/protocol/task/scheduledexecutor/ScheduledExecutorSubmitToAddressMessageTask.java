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
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorSubmitToAddressCodec;
import com.hazelcast.client.impl.protocol.task.AbstractInvocationMessageTask;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.scheduledexecutor.impl.DistributedScheduledExecutorService;
import com.hazelcast.scheduledexecutor.impl.TaskDefinition;
import com.hazelcast.scheduledexecutor.impl.operations.ScheduleTaskOperation;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.ScheduledExecutorPermission;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;

import java.security.Permission;

public class ScheduledExecutorSubmitToAddressMessageTask
        extends AbstractInvocationMessageTask<ScheduledExecutorSubmitToAddressCodec.RequestParameters> {

    public ScheduledExecutorSubmitToAddressMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected InvocationBuilder getInvocationBuilder(Operation op) {
        final InternalOperationService operationService = nodeEngine.getOperationService();
        return operationService.createInvocationBuilder(getServiceName(), op, parameters.address);
    }

    @Override
    protected Operation prepareOperation() {
        TaskDefinition def = serializationService.toObject(parameters.taskDefinition);
        Operation op = new ScheduleTaskOperation(parameters.schedulerName, def);
        op.setPartitionId(-1);
        op.setCallerUuid(endpoint.getUuid());
        return op;
    }

    @Override
    protected ScheduledExecutorSubmitToAddressCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return ScheduledExecutorSubmitToAddressCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return ScheduledExecutorSubmitToAddressCodec.encodeResponse();
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
        return "submitToAddress";
    }

    @Override
    public Object[] getParameters() {
        return new Object[] { parameters.schedulerName, parameters.address, parameters.taskDefinition };
    }
}
