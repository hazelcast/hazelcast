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
import com.hazelcast.map.MapPortableHook;
import com.hazelcast.map.MapService;
import com.hazelcast.map.operation.ContainsValueOperationFactory;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.spi.OperationFactory;
import java.io.IOException;
import java.security.Permission;
import java.util.Map;

public class MapContainsValueRequest extends AllPartitionsClientRequest implements Portable, RetryableRequest, SecureRequest {

    private String name;
    private Data value;

    public MapContainsValueRequest() {
    }

    public MapContainsValueRequest(String name, Data value) {
        this.name = name;
        this.value = value;
    }

    @Override
    protected OperationFactory createOperationFactory() {
        return new ContainsValueOperationFactory(name, value);
    }

    @Override
    protected Object reduce(Map<Integer, Object> map) {
        for (Object contains : map.values()) {
            if (Boolean.TRUE.equals(contains)) {
                return true;
            }
        }
        return false;
    }

    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return MapPortableHook.F_ID;
    }

    public int getClassId() {
        return MapPortableHook.CONTAINS_VALUE;
    }

    public void write(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
        final ObjectDataOutput out = writer.getRawDataOutput();
        value.writeData(out);
    }

    public void read(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
        final ObjectDataInput in = reader.getRawDataInput();
        value = new Data();
        value.readData(in);
    }

    public Permission getRequiredPermission() {
        return new MapPermission(name, ActionConstants.ACTION_READ);
    }

    @Override
    public String getDistributedObjectName() {
        return name;
    }

    @Override
    public String getMethodName() {
        return "containsValue";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{value};
    }
}
