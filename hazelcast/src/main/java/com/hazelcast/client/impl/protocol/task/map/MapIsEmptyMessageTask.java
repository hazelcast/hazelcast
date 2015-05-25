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
import com.hazelcast.client.impl.protocol.parameters.BooleanResultParameters;
import com.hazelcast.client.impl.protocol.codec.MapIsEmptyCodec;
import com.hazelcast.client.impl.protocol.task.AbstractAllPartitionsMessageTask;
import com.hazelcast.instance.Node;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.operation.IsEmptyOperationFactory;
import com.hazelcast.nio.Connection;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.spi.OperationFactory;

import java.security.Permission;
import java.util.Map;

public class MapIsEmptyMessageTask extends AbstractAllPartitionsMessageTask<MapIsEmptyCodec.RequestParameters> {

    public MapIsEmptyMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected OperationFactory createOperationFactory() {
        return new IsEmptyOperationFactory(parameters.name);
    }

    @Override
    protected ClientMessage reduce(Map<Integer, Object> map) {
        MapService mapService = getService(MapService.SERVICE_NAME);
        boolean response = true;
        for (Object result : map.values()) {
            boolean isEmpty = (Boolean) mapService.getMapServiceContext().toObject(result);
            if (!isEmpty) {
                response = false;
            }
        }

        return BooleanResultParameters.encode(response);
    }

    @Override
    protected MapIsEmptyCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MapIsEmptyCodec.decodeRequest(clientMessage);
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new MapPermission(parameters.name, ActionConstants.ACTION_READ);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return "isEmpty";
    }

    @Override
    public Object[] getParameters() {
        return null;
    }

}
