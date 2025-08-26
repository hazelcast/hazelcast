/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.task.multimap;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MultiMapGetCodec;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.util.Timer;
import com.hazelcast.multimap.impl.MultiMapRecord;
import com.hazelcast.multimap.impl.operations.GetAllOperation;
import com.hazelcast.multimap.impl.operations.MultiMapResponse;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.security.SecurityInterceptorConstants;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MultiMapPermission;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.security.Permission;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Client Protocol Task for handling messages with type ID:
 * {@link com.hazelcast.client.impl.protocol.codec.MultiMapGetCodec#REQUEST_MESSAGE_TYPE}
 */
public class MultiMapGetMessageTask
        extends AbstractMultiMapPartitionMessageTask<MultiMapGetCodec.RequestParameters> {

    private transient long startTimeNanos;

    public MultiMapGetMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected void beforeProcess() {
        if (getContainer().getConfig().isStatisticsEnabled()) {
            startTimeNanos = Timer.nanos();
        }
    }

    @Override
    protected Object processResponseBeforeSending(Object response) {
        updateStats(stats -> stats.incrementGetLatencyNanos(Timer.nanosElapsed(startTimeNanos)));
        return response;
    }

    @Override
    protected Operation prepareOperation() {
        GetAllOperation operation = new GetAllOperation(parameters.name, parameters.key);
        operation.setThreadId(parameters.threadId);
        return operation;
    }

    @Override
    protected MultiMapGetCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MultiMapGetCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        List<Data> collection = new ArrayList<>();
        Collection<MultiMapRecord> responseCollection = ((MultiMapResponse) response).getCollection();
        if (responseCollection != null) {
            for (MultiMapRecord record : responseCollection) {
                collection.add(serializationService.toData(record.getObject()));
            }
        }
        return MultiMapGetCodec.encodeResponse(collection);
    }

    @Override
    public Permission getRequiredPermission() {
        return new MultiMapPermission(parameters.name, ActionConstants.ACTION_READ);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.GET;
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.key};
    }

}
