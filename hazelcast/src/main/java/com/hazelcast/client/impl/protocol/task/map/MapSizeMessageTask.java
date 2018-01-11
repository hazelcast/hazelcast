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

package com.hazelcast.client.impl.protocol.task.map;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MapSizeCodec;
import com.hazelcast.instance.Node;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.nio.Connection;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.spi.OperationFactory;

import java.security.Permission;
import java.util.Map;

public class MapSizeMessageTask
        extends AbstractMapAllPartitionsMessageTask<MapSizeCodec.RequestParameters> {

    public MapSizeMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected OperationFactory createOperationFactory() {
        String mapName = parameters.name;
        return getOperationProvider(mapName).createMapSizeOperationFactory(mapName);
    }

    @Override
    protected Object reduce(Map<Integer, Object> map) {
        int total = 0;
        MapService mapService = getService(MapService.SERVICE_NAME);
        for (Object result : map.values()) {
            Integer size = (Integer) mapService.getMapServiceContext().toObject(result);
            total += size;
        }
        return total;
    }

    @Override
    protected MapSizeCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MapSizeCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MapSizeCodec.encodeResponse((Integer) response);
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    public Permission getRequiredPermission() {
        return new MapPermission(parameters.name, ActionConstants.ACTION_READ);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return "size";
    }

    @Override
    public Object[] getParameters() {
        return null;
    }
}
