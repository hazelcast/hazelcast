/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.client;

import com.hazelcast.client.client.AllPartitionsClientRequest;
import com.hazelcast.client.client.RetryableRequest;
import com.hazelcast.client.client.SecureRequest;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.map.MapPortableHook;
import com.hazelcast.map.MapService;
import com.hazelcast.map.operation.ClearOperationFactory;
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

public class MapClearRequest extends AllPartitionsClientRequest implements Portable, RetryableRequest, SecureRequest {

    private String name;

    public MapClearRequest() {
    }

    public MapClearRequest(String name) {
        this.name = name;
    }

    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    public int getFactoryId() {
        return MapPortableHook.F_ID;
    }

    public int getClassId() {
        return MapPortableHook.CLEAR;
    }

    public void write(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
    }

    public void read(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
    }

    @Override
    protected OperationFactory createOperationFactory() {
        return new ClearOperationFactory(name);
    }

    @Override
    protected Object reduce(Map<Integer, Object> map) {
        int totalAffectedEntries = 0;
        for (Object affectedEntries : map.values()) {
            totalAffectedEntries += (Integer) affectedEntries;
        }
        final MapService service = getService();
        final Address thisAddress = service.getMapServiceContext().getNodeEngine().getThisAddress();
        if (totalAffectedEntries > 0) {
            service.getMapServiceContext().getMapEventPublisher()
                    .publishMapEvent(thisAddress, name, EntryEventType.CLEAR_ALL, totalAffectedEntries);
        }
        return null;
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
        return "clear";
    }
}
