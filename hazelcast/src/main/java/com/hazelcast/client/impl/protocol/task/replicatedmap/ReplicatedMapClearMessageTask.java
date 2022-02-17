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
import com.hazelcast.client.impl.protocol.codec.ReplicatedMapClearCodec;
import com.hazelcast.client.impl.protocol.task.AbstractAllPartitionsMessageTask;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.replicatedmap.impl.ReplicatedMapEventPublishingService;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.replicatedmap.impl.operation.ClearOperationFactory;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.ReplicatedMapPermission;
import com.hazelcast.spi.impl.operationservice.OperationFactory;

import java.security.Permission;
import java.util.Map;

public class ReplicatedMapClearMessageTask
        extends AbstractAllPartitionsMessageTask<String> {

    public ReplicatedMapClearMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected OperationFactory createOperationFactory() {
        return new ClearOperationFactory(parameters);
    }

    @Override
    protected Object reduce(Map<Integer, Object> map) {
        int deletedEntrySize = 0;
        for (Object deletedEntryPerPartition : map.values()) {
            deletedEntrySize += (Integer) deletedEntryPerPartition;
        }
        ReplicatedMapService service = getService(getServiceName());
        ReplicatedMapEventPublishingService eventPublishingService = service.getEventPublishingService();
        eventPublishingService.fireMapClearedEvent(deletedEntrySize, getDistributedObjectName());
        return null;
    }

    @Override
    protected String decodeClientMessage(ClientMessage clientMessage) {
        return ReplicatedMapClearCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return ReplicatedMapClearCodec.encodeResponse();
    }

    @Override
    public String getServiceName() {
        return ReplicatedMapService.SERVICE_NAME;
    }

    @Override
    public String getDistributedObjectName() {
        return parameters;
    }

    @Override
    public String getMethodName() {
        return "clear";
    }

    @Override
    public Permission getRequiredPermission() {
        return new ReplicatedMapPermission(parameters, ActionConstants.ACTION_REMOVE);
    }

    @Override
    public Object[] getParameters() {
        return null;
    }
}

