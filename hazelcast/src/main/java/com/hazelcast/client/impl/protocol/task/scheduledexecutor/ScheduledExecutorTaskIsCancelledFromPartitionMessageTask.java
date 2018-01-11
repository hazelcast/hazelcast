/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorIsCancelledFromPartitionCodec;
import com.hazelcast.client.impl.protocol.task.AbstractPartitionMessageTask;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.scheduledexecutor.ScheduledTaskHandler;
import com.hazelcast.scheduledexecutor.impl.DistributedScheduledExecutorService;
import com.hazelcast.scheduledexecutor.impl.ScheduledTaskHandlerImpl;
import com.hazelcast.scheduledexecutor.impl.operations.IsCanceledOperation;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.ScheduledExecutorPermission;
import com.hazelcast.spi.Operation;

import java.security.Permission;

public class ScheduledExecutorTaskIsCancelledFromPartitionMessageTask
        extends AbstractPartitionMessageTask<ScheduledExecutorIsCancelledFromPartitionCodec.RequestParameters> {

    public ScheduledExecutorTaskIsCancelledFromPartitionMessageTask(ClientMessage clientMessage, Node node,
                                                                    Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        ScheduledTaskHandler handler = ScheduledTaskHandlerImpl.of(getPartitionId(),
                parameters.schedulerName,
                parameters.taskName);
        return new IsCanceledOperation(handler);
    }

    @Override
    protected ScheduledExecutorIsCancelledFromPartitionCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return ScheduledExecutorIsCancelledFromPartitionCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return ScheduledExecutorIsCancelledFromPartitionCodec.encodeResponse((Boolean) response);
    }

    @Override
    public String getServiceName() {
        return DistributedScheduledExecutorService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new ScheduledExecutorPermission(parameters.schedulerName, ActionConstants.ACTION_READ);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.schedulerName;
    }


    @Override
    public String getMethodName() {
        return "isCancelled";
    }

    @Override
    public Object[] getParameters() {
        return null;
    }
}
