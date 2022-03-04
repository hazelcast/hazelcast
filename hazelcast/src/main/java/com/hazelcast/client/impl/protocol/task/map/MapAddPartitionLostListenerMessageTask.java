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
import com.hazelcast.client.impl.protocol.codec.MapAddPartitionLostListenerCodec;
import com.hazelcast.client.impl.protocol.task.AbstractAddListenerMessageTask;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.listener.MapPartitionLostListener;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;

import java.security.Permission;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.spi.impl.InternalCompletableFuture.newCompletedFuture;

public class MapAddPartitionLostListenerMessageTask
        extends AbstractAddListenerMessageTask<MapAddPartitionLostListenerCodec.RequestParameters> {

    public MapAddPartitionLostListenerMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected CompletableFuture<UUID> processInternal() {
        final MapService mapService = getService(MapService.SERVICE_NAME);

        final MapPartitionLostListener listener = event -> {
            if (endpoint.isAlive()) {
                ClientMessage eventMessage = MapAddPartitionLostListenerCodec
                        .encodeMapPartitionLostEvent(event.getPartitionId(), event.getMember().getUuid());
                sendClientMessage(null, eventMessage);
            }
        };

        MapServiceContext mapServiceContext = mapService.getMapServiceContext();

        if (parameters.localOnly) {
            return newCompletedFuture(mapServiceContext.addLocalPartitionLostListener(listener, parameters.name));
        }

        return mapServiceContext.addPartitionLostListenerAsync(listener, parameters.name);
    }

    @Override
    protected MapAddPartitionLostListenerCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MapAddPartitionLostListenerCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MapAddPartitionLostListenerCodec.encodeResponse((UUID) response);
    }


    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public String getMethodName() {
        return "addPartitionLostListener";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{null};
    }

    @Override
    public Permission getRequiredPermission() {
        return new MapPermission(parameters.name, ActionConstants.ACTION_LISTEN);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }
}
