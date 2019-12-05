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
import com.hazelcast.client.impl.protocol.codec.MapFetchKeysCodec;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.iterator.MapKeysWithCursor;
import com.hazelcast.map.impl.operation.MapOperationProvider;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.security.Permission;
import java.util.Collections;

public class MapFetchKeysMessageTask extends AbstractMapPartitionMessageTask<MapFetchKeysCodec.RequestParameters> {
    public MapFetchKeysMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        MapOperationProvider operationProvider = getMapOperationProvider(parameters.name);
        return operationProvider.createFetchKeysOperation(parameters.name, parameters.tableIndex, parameters.batch);
    }

    @Override
    protected MapFetchKeysCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MapFetchKeysCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        if (response == null) {
            return MapFetchKeysCodec.encodeResponse(0, Collections.<Data>emptyList());
        }
        MapKeysWithCursor mapKeysWithCursor = (MapKeysWithCursor) response;
        return MapFetchKeysCodec.encodeResponse(mapKeysWithCursor.getNextTableIndexToReadFrom(), mapKeysWithCursor.getBatch());
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
