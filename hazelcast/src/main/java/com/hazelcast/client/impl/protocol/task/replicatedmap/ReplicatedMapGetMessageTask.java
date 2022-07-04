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

package com.hazelcast.client.impl.protocol.task.replicatedmap;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ReplicatedMapGetCodec;
import com.hazelcast.client.impl.protocol.task.AbstractPartitionMessageTask;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.monitor.impl.LocalReplicatedMapStatsImpl;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.util.Timer;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.replicatedmap.impl.operation.GetOperation;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.ReplicatedMapPermission;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.security.Permission;

public class ReplicatedMapGetMessageTask
        extends AbstractPartitionMessageTask<ReplicatedMapGetCodec.RequestParameters> {

    private volatile long startTimeNanos;

    public ReplicatedMapGetMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        return new GetOperation(parameters.name, parameters.key);
    }

    @Override
    protected ReplicatedMapGetCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return ReplicatedMapGetCodec.decodeRequest(clientMessage);
    }

    @Override
    protected void beforeProcess() {
        startTimeNanos = Timer.nanos();
    }

    @Override
    protected Object processResponseBeforeSending(Object response) {
        ReplicatedMapService replicatedMapService = getService(ReplicatedMapService.SERVICE_NAME);
        if (replicatedMapService.getReplicatedMapConfig(parameters.name).isStatisticsEnabled()) {
            LocalReplicatedMapStatsImpl stats = replicatedMapService.getLocalReplicatedMapStatsImpl(parameters.name);
            stats.incrementGetsNanos(Timer.nanosElapsed(startTimeNanos));
        }
        return response;
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return ReplicatedMapGetCodec.encodeResponse(serializationService.toData(response));
    }

    @Override
    public String getServiceName() {
        return ReplicatedMapService.SERVICE_NAME;
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return "get";
    }

    @Override
    public Permission getRequiredPermission() {
        return new ReplicatedMapPermission(parameters.name, ActionConstants.ACTION_READ);
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.key};
    }
}
