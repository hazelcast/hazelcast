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

import com.hazelcast.client.impl.client.SecureRequest;
import com.hazelcast.client.impl.client.TargetClientRequest;
import com.hazelcast.map.impl.MapPortableHook;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.operation.ClearNearCacheOperation;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.security.Permission;

public class MapClearNearCacheRequest extends TargetClientRequest implements Portable, SecureRequest {

    private String name;

    private Address target;

    public MapClearNearCacheRequest() {
    }

    public MapClearNearCacheRequest(String name, Address target) {
        this.name = name;
        this.target = target;
    }

    @Override
    public int getFactoryId() {
        return MapPortableHook.F_ID;
    }

    public int getClassId() {
        return MapPortableHook.CLEAR_NEAR_CACHE;
    }

    @Override
    public String getDistributedObjectName() {
        return name;
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    protected Operation prepareOperation() {
        return new ClearNearCacheOperation(name);
    }

    @Override
    protected InvocationBuilder getInvocationBuilder(Operation op) {
        return operationService.createInvocationBuilder(getServiceName(), op, target);
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
        target.writeData(writer.getRawDataOutput());
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
        target = new Address();
        target.readData(reader.getRawDataInput());
    }

    @Override
    public Permission getRequiredPermission() {
        return new MapPermission(name, ActionConstants.ACTION_REMOVE);
    }

}
