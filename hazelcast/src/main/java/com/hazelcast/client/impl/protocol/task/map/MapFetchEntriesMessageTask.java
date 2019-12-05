/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.task.map;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MapFetchEntriesCodec;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.iterator.MapEntriesWithCursor;
import com.hazelcast.map.impl.operation.MapOperationProvider;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.security.Permission;
import java.util.Collections;
import java.util.Map;

public class MapFetchEntriesMessageTask extends AbstractMapPartitionMessageTask<MapFetchEntriesCodec.RequestParameters> {
    public MapFetchEntriesMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        MapOperationProvider operationProvider = getMapOperationProvider(parameters.name);
        return operationProvider.createFetchEntriesOperation(parameters.name, parameters.tableIndex, parameters.batch);
    }

    @Override
    protected MapFetchEntriesCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MapFetchEntriesCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        if (response == null) {
            return MapFetchEntriesCodec.encodeResponse(0, Collections.<Map.Entry<Data, Data>>emptyList());
        }
        MapEntriesWithCursor mapEntriesWithCursor = (MapEntriesWithCursor) response;
        return MapFetchEntriesCodec.encodeResponse(mapEntriesWithCursor.getNextTableIndexToReadFrom(),
                mapEntriesWithCursor.getBatch());
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return "iterator";
    }

    @Override
    public Object[] getParameters() {
        return new Object[0];
    }
}
