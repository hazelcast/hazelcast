/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector.impl.protocol.task;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.VectorCollectionPutAllCodec;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.security.SecurityInterceptorConstants;
import com.hazelcast.security.permission.VectorCollectionPermission;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.vector.impl.DataVectorDocument;
import com.hazelcast.vector.impl.ops.PutAllOperation;
import com.hazelcast.vector.impl.ops.VectorEntries;

import java.security.Permission;
import java.util.Map;

import static com.hazelcast.internal.util.MapUtil.createHashMap;
import static com.hazelcast.security.permission.ActionConstants.ACTION_PUT;

/**
 * PutAll message task for single partition
 */
public class VectorCollectionPutAllMessageTask
        extends AbstractVectorCollectionPartitionMessageTask<VectorCollectionPutAllCodec.RequestParameters> {

    public VectorCollectionPutAllMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        VectorEntries mapEntries = new VectorEntries(parameters.entries);
        return new PutAllOperation(parameters.name, mapEntries);
    }

    @Override
    protected VectorCollectionPutAllCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return VectorCollectionPutAllCodec.decodeRequest(clientMessage);
    }

    @Override
    protected Object processResponseBeforeSending(Object response) {
        recordStats((stats, nanos) -> stats.incrementPutAllLatencyNanos(parameters.entries.size(), nanos));
        return response;
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return VectorCollectionPutAllCodec.encodeResponse();
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.PUT_ALL;
    }

    @Override
    public Object[] getParameters() {
        Map<Data, DataVectorDocument> map = createHashMap(parameters.entries.size());
        for (Map.Entry<Data, DataVectorDocument> entry : parameters.entries) {
            map.put(entry.getKey(), entry.getValue());
        }
        return new Object[]{map};
    }

    @Override
    public Permission getRequiredPermission() {
        return new VectorCollectionPermission(getDistributedObjectName(), ACTION_PUT);
    }
}
