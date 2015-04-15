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

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.parameters.AddListenerResultParameters;
import com.hazelcast.client.impl.protocol.parameters.MapAddPartitionLostListenerParameters;
import com.hazelcast.client.impl.protocol.task.AbstractCallableMessageTask;
import com.hazelcast.instance.Node;
import com.hazelcast.map.MapPartitionLostEvent;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.listener.MapPartitionLostListener;
import com.hazelcast.nio.Connection;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.spi.impl.PortableMapPartitionLostEvent;

import java.security.Permission;

public class MapAddPartitionLostListenerMessageTask
        extends AbstractCallableMessageTask<MapAddPartitionLostListenerParameters> {


    public MapAddPartitionLostListenerMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected ClientMessage call() {
        final ClientEndpoint endpoint = getEndpoint();
        final MapService mapService = getService(MapService.SERVICE_NAME);

        final MapPartitionLostListener listener = new MapPartitionLostListener() {
            @Override
            public void partitionLost(MapPartitionLostEvent event) {
                if (endpoint.isAlive()) {
                    final PortableMapPartitionLostEvent portableEvent =
                            new PortableMapPartitionLostEvent(event.getPartitionId(), event.getMember().getUuid());
                    endpoint.sendEvent(null, portableEvent, clientMessage.getCorrelationId());
                }
            }
        };

        String registrationId = mapService.getMapServiceContext().addPartitionLostListener(listener, parameters.name);
        endpoint.setListenerRegistration(MapService.SERVICE_NAME, parameters.name, registrationId);
        return AddListenerResultParameters.encode(registrationId);
    }

    @Override
    protected MapAddPartitionLostListenerParameters decodeClientMessage(ClientMessage clientMessage) {
        return MapAddPartitionLostListenerParameters.decode(clientMessage);
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
        return null;
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
