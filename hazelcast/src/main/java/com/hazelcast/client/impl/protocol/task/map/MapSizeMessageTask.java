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

package com.hazelcast.client.impl.protocol.task.map;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MapSizeCodec;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.spi.impl.operationservice.OperationFactory;

import java.security.Permission;
import java.util.Map;

import static com.hazelcast.internal.util.MapUtil.toIntSize;
import static com.hazelcast.map.impl.LocalMapStatsUtil.incrementOtherOperationsCount;

public class MapSizeMessageTask
        extends AbstractMapAllPartitionsMessageTask<String> {

    public MapSizeMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected OperationFactory createOperationFactory() {
        String mapName = parameters;
        return getOperationProvider(mapName).createMapSizeOperationFactory(mapName);
    }

    @Override
    protected Object reduce(Map<Integer, Object> map) {
        long total = 0;
        MapService mapService = getService(MapService.SERVICE_NAME);
        for (Object result : map.values()) {
            Integer size = (Integer) mapService.getMapServiceContext().toObject(result);
            total += size;
        }
        incrementOtherOperationsCount(mapService, parameters);
        return toIntSize(total);
    }

    @Override
    protected String decodeClientMessage(ClientMessage clientMessage) {
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
        return new MapPermission(parameters, ActionConstants.ACTION_READ);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters;
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
