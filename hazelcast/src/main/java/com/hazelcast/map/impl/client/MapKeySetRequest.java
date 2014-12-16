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

package com.hazelcast.map.impl.client;

import com.hazelcast.client.impl.client.AllPartitionsClientRequest;
import com.hazelcast.client.impl.client.RetryableRequest;
import com.hazelcast.client.impl.client.SecureRequest;
import com.hazelcast.map.impl.MapKeySet;
import com.hazelcast.map.impl.MapPortableHook;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.operation.MapKeySetOperationFactory;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.spi.OperationFactory;
import java.io.IOException;
import java.security.Permission;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class MapKeySetRequest extends AllPartitionsClientRequest implements Portable, RetryableRequest, SecureRequest {

    private String name;

    public MapKeySetRequest() {
    }

    public MapKeySetRequest(String name) {
        this.name = name;
    }

    @Override
    protected OperationFactory createOperationFactory() {
        return new MapKeySetOperationFactory(name);
    }

    @Override
    protected Object reduce(Map<Integer, Object> map) {
        Set res = new HashSet();
        MapService service = getService();
        for (Object o : map.values()) {
            Set keys = ((MapKeySet) service.getMapServiceContext().toObject(o)).getKeySet();
            res.addAll(keys);
        }
        return new MapKeySet(res);
    }

    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return MapPortableHook.F_ID;
    }

    public int getClassId() {
        return MapPortableHook.KEY_SET;
    }

    public void write(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
    }

    public void read(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
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
        return "keySet";
    }
}
