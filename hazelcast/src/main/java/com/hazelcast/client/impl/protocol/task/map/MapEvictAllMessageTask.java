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
import com.hazelcast.client.impl.protocol.codec.MapEvictAllCodec;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.event.MapEventPublisher;
import com.hazelcast.map.impl.operation.MapOperationProvider;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.spi.impl.operationservice.OperationFactory;

import java.security.Permission;
import java.util.Map;

import static com.hazelcast.core.EntryEventType.EVICT_ALL;

public class MapEvictAllMessageTask
        extends AbstractMapAllPartitionsMessageTask<String> {

    public MapEvictAllMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected OperationFactory createOperationFactory() {
        MapOperationProvider operationProvider = getOperationProvider(parameters);
        return operationProvider.createEvictAllOperationFactory(parameters);
    }

    @Override
    protected Object reduce(Map<Integer, Object> map) {
        MapService mapService = getService(MapService.SERVICE_NAME);
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();

        int evictedTotal = 0;
        for (Object result : map.values()) {
            Integer size = (Integer) mapServiceContext.toObject(result);
            evictedTotal += size;
        }

        if (evictedTotal > 0) {
            Address thisAddress = mapServiceContext.getNodeEngine().getThisAddress();
            MapEventPublisher mapEventPublisher = mapServiceContext.getMapEventPublisher();
            mapEventPublisher.publishMapEvent(thisAddress, parameters, EVICT_ALL, evictedTotal);
        }

        return null;
    }

    @Override
    protected String decodeClientMessage(ClientMessage clientMessage) {
        return MapEvictAllCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MapEvictAllCodec.encodeResponse();
    }

    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    public Permission getRequiredPermission() {
        return new MapPermission(parameters, ActionConstants.ACTION_REMOVE);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters;
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
