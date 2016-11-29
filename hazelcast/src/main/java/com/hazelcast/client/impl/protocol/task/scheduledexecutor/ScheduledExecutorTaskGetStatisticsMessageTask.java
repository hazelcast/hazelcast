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
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorGetStatsCodec;
import com.hazelcast.client.impl.protocol.task.AbstractInvocationMessageTask;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.scheduledexecutor.ScheduledTaskStatistics;
import com.hazelcast.scheduledexecutor.impl.DistributedScheduledExecutorService;
import com.hazelcast.scheduledexecutor.impl.operations.GetStatisticsOperation;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.ScheduledExecutorPermission;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;

import java.security.Permission;
import java.util.concurrent.TimeUnit;

public class ScheduledExecutorTaskGetStatisticsMessageTask
        extends AbstractInvocationMessageTask<ScheduledExecutorGetStatsCodec.RequestParameters> {

    public ScheduledExecutorTaskGetStatisticsMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
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
        Operation op = new GetStatisticsOperation(parameters.handler);
        op.setPartitionId(getPartitionId());
        return op;
    }

    @Override
    protected ScheduledExecutorGetStatsCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return ScheduledExecutorGetStatsCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        ScheduledTaskStatistics stats = (ScheduledTaskStatistics) response;
        return ScheduledExecutorGetStatsCodec.encodeResponse(stats.getCreatedAt(), stats.getFirstRunStart(),
                stats.getLastIdleTime(TimeUnit.NANOSECONDS), stats.getLastRunEnd(),
                stats.getLastRunStart(), stats.getTotalIdleTime(TimeUnit.NANOSECONDS), stats.getTotalRuns(),
                stats.getTotalRunTime(TimeUnit.NANOSECONDS));
    }

    @Override
    public String getServiceName() {
        return DistributedScheduledExecutorService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new ScheduledExecutorPermission(parameters.handler.getSchedulerName(), ActionConstants.ACTION_READ);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.handler.getTaskName();
    }

    @Override
    public String getMethodName() {
        return "getStatistics";
    }

    @Override
    public Object[] getParameters() {
        return new Object[] { parameters.handler };
    }
}
