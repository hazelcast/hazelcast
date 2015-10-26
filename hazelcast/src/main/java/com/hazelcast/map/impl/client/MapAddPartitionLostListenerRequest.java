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

package com.hazelcast.map.impl.client;

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.impl.client.BaseClientAddListenerRequest;
import com.hazelcast.map.MapPartitionLostEvent;
import com.hazelcast.map.impl.MapPortableHook;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.listener.MapPartitionLostListener;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.spi.impl.PortableMapPartitionLostEvent;

import java.io.IOException;
import java.security.Permission;

public class MapAddPartitionLostListenerRequest extends BaseClientAddListenerRequest {

    private String name;

    public MapAddPartitionLostListenerRequest() {
    }

    public MapAddPartitionLostListenerRequest(String name) {
        this.name = name;
    }

    @Override
    public Object call() {
        final ClientEndpoint endpoint = getEndpoint();

        final MapPartitionLostListener listener = new MapPartitionLostListener() {
            @Override
            public void partitionLost(MapPartitionLostEvent event) {
                if (endpoint.isAlive()) {
                    final PortableMapPartitionLostEvent portableEvent =
                            new PortableMapPartitionLostEvent(event.getPartitionId(), event.getMember().getUuid());
                    endpoint.sendEvent(null, portableEvent, getCallId());
                }
            }
        };

        MapService mapService = getService();
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        String registrationId;
        if (localOnly) {
            registrationId = mapServiceContext.addLocalPartitionLostListener(listener, name);
        } else {
            registrationId = mapServiceContext.addPartitionLostListener(listener, name);
        }

        endpoint.addListenerDestroyAction(MapService.SERVICE_NAME, name, registrationId);
        return registrationId;
    }


    @Override
    public void write(PortableWriter writer) throws IOException {
        super.write(writer);
        writer.writeUTF("name", name);
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        super.read(reader);
        name = reader.readUTF("name");
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
    public int getFactoryId() {
        return MapPortableHook.F_ID;
    }

    @Override
    public int getClassId() {
        return MapPortableHook.ADD_MAP_PARTITION_LOST_LISTENER;
    }

    @Override
    public Permission getRequiredPermission() {
        return new MapPermission(name, ActionConstants.ACTION_LISTEN);
    }

    @Override
    public String getDistributedObjectName() {
        return name;
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{null};
    }
}
