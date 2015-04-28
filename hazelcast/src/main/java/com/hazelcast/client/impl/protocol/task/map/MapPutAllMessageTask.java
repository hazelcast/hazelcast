/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.impl.protocol.parameters.MapPutAllParameters;
import com.hazelcast.client.impl.protocol.parameters.VoidResultParameters;
import com.hazelcast.client.impl.protocol.task.AbstractAllPartitionsMessageTask;
import com.hazelcast.instance.Node;
import com.hazelcast.map.impl.MapEntrySet;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.operation.MapPutAllOperationFactory;
import com.hazelcast.nio.Connection;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.util.ExceptionUtil;

import java.security.Permission;
import java.util.HashMap;
import java.util.Map;

public class MapPutAllMessageTask extends AbstractAllPartitionsMessageTask<MapPutAllParameters> {

    public MapPutAllMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected OperationFactory createOperationFactory() {
        int length = parameters.keys.size();
        final MapEntrySet mapEntrySet = new MapEntrySet();
        for (int i = 0; i < length; i++) {
            mapEntrySet.add(parameters.keys.get(i), parameters.values.get(i));
        }
        return new MapPutAllOperationFactory(parameters.name, mapEntrySet);
    }

    @Override
    protected ClientMessage reduce(Map<Integer, Object> map) {
        MapService mapService = getService(MapService.SERVICE_NAME);
        for (Map.Entry<Integer, Object> entry : map.entrySet()) {
            Object result = mapService.getMapServiceContext().toObject(entry.getValue());
            if (result instanceof Throwable) {
                throw ExceptionUtil.rethrow((Throwable) result);
            }
        }
        return VoidResultParameters.encode();
    }

    @Override
    protected MapPutAllParameters decodeClientMessage(ClientMessage clientMessage) {
        return MapPutAllParameters.decode(clientMessage);
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
        final HashMap map = new HashMap();
        final int length = parameters.keys.size();
        for (int i = 0; i < length; i++) {
            map.put(parameters.keys.get(i), parameters.values.get(i));
        }
        return new Object[]{map};
    }
}
