/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorSubmitToMemberCodec;
import com.hazelcast.client.impl.protocol.task.AbstractTargetMessageTask;
import com.hazelcast.cluster.Member;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.scheduledexecutor.impl.DistributedScheduledExecutorService;
import com.hazelcast.scheduledexecutor.impl.TaskDefinition;
import com.hazelcast.scheduledexecutor.impl.operations.ScheduleTaskOperation;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.ScheduledExecutorPermission;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.security.Permission;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class ScheduledExecutorSubmitToTargetMessageTask
        extends AbstractTargetMessageTask<ScheduledExecutorSubmitToMemberCodec.RequestParameters> {

    public ScheduledExecutorSubmitToTargetMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        Callable callable = serializationService.toObject(parameters.task);
        TaskDefinition def = new TaskDefinition(TaskDefinition.Type.getById(parameters.type),
                parameters.taskName, callable, parameters.initialDelayInMillis, parameters.periodInMillis,
                TimeUnit.MILLISECONDS, isAutoDisposable());
        return new ScheduleTaskOperation(parameters.schedulerName, def);
    }

    @Override
    protected UUID getTargetUuid() {
        return parameters.memberUuid;
    }

    @Override
    protected ScheduledExecutorSubmitToMemberCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return ScheduledExecutorSubmitToMemberCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return ScheduledExecutorSubmitToMemberCodec.encodeResponse();
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
        Callable callable = serializationService.toObject(parameters.task);
        TaskDefinition def = new TaskDefinition(TaskDefinition.Type.getById(parameters.type),
                parameters.taskName, callable, parameters.initialDelayInMillis, parameters.periodInMillis,
                TimeUnit.MILLISECONDS, isAutoDisposable());
        Member member = nodeEngine.getClusterService().getMember(parameters.memberUuid);
        return new Object[]{parameters.schedulerName, member == null ? null : member.getAddress(), def};
    }

    private boolean isAutoDisposable() {
        return parameters.isAutoDisposableExists && parameters.autoDisposable;
    }
}
