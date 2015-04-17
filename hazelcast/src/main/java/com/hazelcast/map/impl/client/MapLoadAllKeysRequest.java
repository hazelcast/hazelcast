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
import com.hazelcast.client.impl.client.InvocationClientRequest;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.map.impl.MapPortableHook;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;

import java.io.IOException;
import java.security.Permission;

/**
 * Triggers the load of all keys from defined map store.
 */
public class MapLoadAllKeysRequest extends InvocationClientRequest {

    protected String name;

    private boolean replaceExistingValues;

    public MapLoadAllKeysRequest() {
    }

    public MapLoadAllKeysRequest(String name, boolean replaceExistingValues) {
        this.name = name;
        this.replaceExistingValues = replaceExistingValues;
    }

    @Override
    public void invoke() {
        final MapService mapService = getService();
        final DistributedObject distributedObject
                = mapService.getMapServiceContext().getNodeEngine().getProxyService()
                .getDistributedObject(MapService.SERVICE_NAME, name);
        final MapProxyImpl mapProxy = (MapProxyImpl) distributedObject;
        mapProxy.loadAll(replaceExistingValues);
        final ClientEndpoint endpoint = getEndpoint();
        endpoint.sendResponse(Boolean.TRUE, getCallId());
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return MapPortableHook.F_ID;
    }

    @Override
    public int getClassId() {
        return MapPortableHook.LOAD_ALL_KEYS;
    }

    @Override
    public Permission getRequiredPermission() {
        return new MapPermission(name, ActionConstants.ACTION_REMOVE);
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
        writer.writeBoolean("r", replaceExistingValues);
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
        replaceExistingValues = reader.readBoolean("r");
    }

    @Override
    public String getDistributedObjectName() {
        return name;
    }

    @Override
    public String getMethodName() {
        return "loadAll";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{replaceExistingValues};
    }
}
