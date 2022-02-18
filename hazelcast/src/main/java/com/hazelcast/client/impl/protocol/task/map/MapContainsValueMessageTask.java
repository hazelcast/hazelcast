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
import com.hazelcast.client.impl.protocol.codec.MapContainsValueCodec;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.operation.MapOperationProvider;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.spi.impl.operationservice.OperationFactory;

import java.security.Permission;
import java.util.Map;

import static com.hazelcast.map.impl.LocalMapStatsUtil.incrementOtherOperationsCount;

public class MapContainsValueMessageTask
        extends AbstractMapAllPartitionsMessageTask<MapContainsValueCodec.RequestParameters> {

    public MapContainsValueMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected OperationFactory createOperationFactory() {
        MapOperationProvider operationProvider = getOperationProvider(parameters.name);
        return operationProvider.createContainsValueOperationFactory(parameters.name, parameters.value);
    }

    @Override
    protected Object reduce(Map<Integer, Object> map) {
        boolean result = false;
        for (Object contains : map.values()) {
            if (Boolean.TRUE.equals(contains)) {
                result = true;
                break;
            }
        }
        incrementOtherOperationsCount((MapService) getService(MapService.SERVICE_NAME), parameters.name);
        return result;
    }

    @Override
    protected MapContainsValueCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MapContainsValueCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MapContainsValueCodec.encodeResponse((Boolean) response);
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
        return "containsValue";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.value};
    }


}
