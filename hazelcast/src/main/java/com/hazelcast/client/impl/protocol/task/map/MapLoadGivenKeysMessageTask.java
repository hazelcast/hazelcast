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
import com.hazelcast.client.impl.protocol.codec.MapLoadGivenKeysCodec;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.operation.MapOperationProvider;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.spi.impl.operationservice.OperationFactory;

import java.security.Permission;
import java.util.Arrays;
import java.util.Map;

public class MapLoadGivenKeysMessageTask
        extends AbstractMapAllPartitionsMessageTask<MapLoadGivenKeysCodec.RequestParameters> {

    public MapLoadGivenKeysMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected OperationFactory createOperationFactory() {
        Data[] keys = parameters.keys.toArray(new Data[0]);
        MapOperationProvider operationProvider = getOperationProvider(parameters.name);
        return operationProvider.createLoadAllOperationFactory(parameters.name,
                Arrays.asList(keys), parameters.replaceExistingValues);
    }

    @Override
    protected Object reduce(Map<Integer, Object> map) {
        return MapLoadGivenKeysCodec.encodeResponse();
    }

    @Override
    protected MapLoadGivenKeysCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MapLoadGivenKeysCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MapLoadGivenKeysCodec.encodeResponse();
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new MapPermission(parameters.name, ActionConstants.ACTION_REMOVE);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return "loadAll";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.keys, parameters.replaceExistingValues};
    }
}
