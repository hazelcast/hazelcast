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

import com.hazelcast.client.impl.client.RetryableRequest;
import com.hazelcast.client.impl.client.SecureRequest;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.map.impl.MapPortableHook;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.spi.OperationFactory;

import java.io.IOException;
import java.security.Permission;
import java.util.Map;

/**
 * Evict all entries request used by clients.
 */
public class MapEvictAllRequest extends MapAllPartitionsClientRequest implements Portable, RetryableRequest, SecureRequest {


    public MapEvictAllRequest() {
    }

    public MapEvictAllRequest(String name) {
        super(name);
    }

    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    public int getFactoryId() {
        return MapPortableHook.F_ID;
    }

    public int getClassId() {
        return MapPortableHook.EVICT_ALL;
    }

    public void write(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
    }

    public void read(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
    }

    @Override
    protected OperationFactory createOperationFactory() {
        return getOperationProvider().createEvictAllOperationFactory(name);
    }

    @Override
    protected Object reduce(Map<Integer, Object> map) {
        int total = 0;
        MapService mapService = getService();
        final MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        for (Object result : map.values()) {
            Integer size = (Integer) mapServiceContext.toObject(result);
            total += size;
        }
        final Address thisAddress = mapServiceContext.getNodeEngine().getThisAddress();
        if (total > 0) {
            mapServiceContext.getMapEventPublisher().publishMapEvent(thisAddress, name, EntryEventType.EVICT_ALL, total);
        }
        return total;
    }

    public Permission getRequiredPermission() {
        return new MapPermission(name, ActionConstants.ACTION_REMOVE);
    }

    @Override
    public String getDistributedObjectName() {
        return name;
    }

    @Override
    public String getMethodName() {
        return "evictAll";
    }
}
