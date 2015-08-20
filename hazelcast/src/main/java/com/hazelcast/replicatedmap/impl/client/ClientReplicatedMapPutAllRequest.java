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

package com.hazelcast.replicatedmap.impl.client;

import com.hazelcast.client.impl.client.AllPartitionsClientRequest;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.replicatedmap.impl.operation.PutAllOperationFactory;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.ReplicatedMapPermission;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.util.ExceptionUtil;
import java.io.IOException;
import java.security.Permission;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Client request class for {@link Map#putAll(java.util.Map)} implementation
 */
public class ClientReplicatedMapPutAllRequest extends AllPartitionsClientRequest {

    private String name;
    private ReplicatedMapEntrySet entrySet;

    public ClientReplicatedMapPutAllRequest() {
    }

    public ClientReplicatedMapPutAllRequest(String name, ReplicatedMapEntrySet entrySet) {
        this.name = name;
        this.entrySet = entrySet;
    }

    @Override
    protected OperationFactory createOperationFactory() {
        return new PutAllOperationFactory(name, entrySet);
    }

    @Override
    protected Object reduce(Map<Integer, Object> map) {
        for (Map.Entry<Integer, Object> entry : map.entrySet()) {
            Object result = serializationService.toObject(entry.getValue());
            if (result instanceof Throwable) {
                throw ExceptionUtil.rethrow((Throwable) result);
            }
        }
        return null;
    }

    @Override
    public String getServiceName() {
        return ReplicatedMapService.SERVICE_NAME;
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
        entrySet.writePortable(writer);
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
        entrySet = new ReplicatedMapEntrySet();
        entrySet.readPortable(reader);
    }

    @Override
    public int getFactoryId() {
        return ReplicatedMapPortableHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ReplicatedMapPortableHook.PUT_ALL;
    }

    @Override
    public String getDistributedObjectName() {
        return name;
    }

    @Override
    public Permission getRequiredPermission() {
        return new ReplicatedMapPermission(name, ActionConstants.ACTION_PUT);
    }

    @Override
    public String getMethodName() {
        return "putAll";
    }

    @Override
    public Object[] getParameters() {
        final Set<Map.Entry<Data, Data>> set = entrySet.getEntrySet();
        final HashMap map = new HashMap();
        for (Map.Entry entry : set) {
            map.put(entry.getKey(), entry.getValue());
        }
        return new Object[]{map};
    }
}
