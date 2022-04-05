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
import com.hazelcast.client.impl.protocol.codec.MapGetAllCodec;
import com.hazelcast.client.impl.protocol.task.AbstractPartitionMessageTask;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.util.Timer;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.operation.GetAllOperation;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.security.Permission;

public class MapGetAllMessageTask
        extends AbstractPartitionMessageTask<MapGetAllCodec.RequestParameters> {

    private volatile long startTimeNanos;

    public MapGetAllMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        return new GetAllOperation(parameters.name, parameters.keys);
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    protected MapGetAllCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MapGetAllCodec.decodeRequest(clientMessage);
    }

    @Override
    protected void beforeProcess() {
        startTimeNanos = Timer.nanos();
    }

    @Override
    protected Object processResponseBeforeSending(Object response) {
        MapService mapService = getService(MapService.SERVICE_NAME);
        MapContainer mapContainer = mapService.getMapServiceContext().getMapContainer(parameters.name);
        if (mapContainer.getMapConfig().isStatisticsEnabled()) {
            mapService.getMapServiceContext().getLocalMapStatsProvider().getLocalMapStatsImpl(parameters.name)
                    .incrementGetLatencyNanos(parameters.keys.size(), Timer.nanosElapsed(startTimeNanos));
        }
        return response;
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MapGetAllCodec.encodeResponse(((MapEntries) response).entries());
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
        return "getAll";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.keys};
    }
}
