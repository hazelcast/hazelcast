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
import com.hazelcast.client.impl.protocol.parameters.DataEntryListResultParameters;
import com.hazelcast.client.impl.protocol.codec.MapEntrySetCodec;
import com.hazelcast.client.impl.protocol.task.AbstractAllPartitionsMessageTask;
import com.hazelcast.instance.Node;
import com.hazelcast.map.impl.MapEntrySet;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.operation.EntrySetOperationFactory;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.spi.OperationFactory;

import java.security.Permission;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MapEntrySetMessageTask extends AbstractAllPartitionsMessageTask<MapEntrySetCodec.RequestParameters> {

    public MapEntrySetMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected OperationFactory createOperationFactory() {
        return new EntrySetOperationFactory(parameters.name);
    }

    @Override
    protected ClientMessage reduce(Map<Integer, Object> map) {
        List<Data> keys = new ArrayList<Data>();
        List<Data> values = new ArrayList<Data>();
        MapService service = getService(MapService.SERVICE_NAME);
        for (Object result : map.values()) {
            Set<Map.Entry<Data, Data>> entries = ((MapEntrySet) service.getMapServiceContext().toObject(result)).getEntrySet();
            for (Map.Entry<Data, Data> entry : entries) {
                keys.add(entry.getKey());
                values.add(entry.getValue());
            }
        }
        return DataEntryListResultParameters.encode(keys, values);
    }

    @Override
    protected MapEntrySetCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MapEntrySetCodec.decodeRequest(clientMessage);
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
        return "entrySet";
    }

    @Override
    public Object[] getParameters() {
        return null;
    }
}
