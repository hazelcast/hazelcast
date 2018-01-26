/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.impl.protocol.codec.MapPutAllCodec;
import com.hazelcast.instance.Node;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.operation.MapOperationProvider;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.spi.Operation;

import java.security.Permission;
import java.util.Map;

import static com.hazelcast.util.MapUtil.createHashMap;

public class MapPutAllMessageTask
        extends AbstractMapPartitionMessageTask<MapPutAllCodec.RequestParameters> {

    public MapPutAllMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        MapEntries mapEntries = new MapEntries(parameters.entries);
        MapOperationProvider operationProvider = getMapOperationProvider(parameters.name);
        return operationProvider.createPutAllOperation(parameters.name, mapEntries);
    }

    @Override
    protected MapPutAllCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MapPutAllCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MapPutAllCodec.encodeResponse();
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new MapPermission(parameters.name, ActionConstants.ACTION_PUT);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return "putAll";
    }

    @Override
    public Object[] getParameters() {
        Map<Data, Data> map = createHashMap(parameters.entries.size());
        for (Map.Entry<Data, Data> entry : parameters.entries) {
            map.put(entry.getKey(), entry.getValue());
        }
        return new Object[]{map};
    }
}
