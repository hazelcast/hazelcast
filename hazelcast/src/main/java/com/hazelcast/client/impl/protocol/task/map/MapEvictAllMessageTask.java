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
import com.hazelcast.client.impl.protocol.parameters.IntResultParameters;
import com.hazelcast.client.impl.protocol.parameters.MapEvictAllParameters;
import com.hazelcast.client.impl.protocol.task.AbstractAllPartitionsMessageTask;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.instance.Node;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.operation.EvictAllOperationFactory;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.spi.OperationFactory;

import java.security.Permission;
import java.util.Map;

public class MapEvictAllMessageTask extends AbstractAllPartitionsMessageTask<MapEvictAllParameters> {

    public MapEvictAllMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected OperationFactory createOperationFactory() {
        return new EvictAllOperationFactory(parameters.name);
    }

    @Override
    protected ClientMessage reduce(Map<Integer, Object> map) {
        int total = 0;
        MapService mapService = getService(MapService.SERVICE_NAME);
        final MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        for (Object result : map.values()) {
            Integer size = (Integer) mapServiceContext.toObject(result);
            total += size;
        }
        final Address thisAddress = mapServiceContext.getNodeEngine().getThisAddress();
        if (total > 0) {
            mapServiceContext.getMapEventPublisher().
                    publishMapEvent(thisAddress, parameters.name, EntryEventType.EVICT_ALL, total);
        }
        return IntResultParameters.encode(total);
    }

    @Override
    protected MapEvictAllParameters decodeClientMessage(ClientMessage clientMessage) {
        return null;
    }

    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    public Permission getRequiredPermission() {
        return new MapPermission(parameters.name, ActionConstants.ACTION_REMOVE);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return "evictAll";
    }

    @Override
    public Object[] getParameters() {
        return null;
    }
}
