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

import com.hazelcast.client.impl.client.PartitionClientRequest;
import com.hazelcast.client.impl.client.SecureRequest;
import com.hazelcast.map.impl.MapEntrySet;
import com.hazelcast.map.impl.MapPortableHook;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.operation.PutAllOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.spi.Operation;
import java.io.IOException;
import java.security.Permission;
import java.util.HashMap;
import java.util.Map;

public class MapPutAllRequest extends PartitionClientRequest implements Portable, SecureRequest {

    protected String name;
    private MapEntrySet entrySet;
    private int partitionId;

    public MapPutAllRequest() {
    }

    public MapPutAllRequest(String name, MapEntrySet entrySet, int partitionId) {
        this.name = name;
        this.entrySet = entrySet;
        this.partitionId = partitionId;
    }

    public int getFactoryId() {
        return MapPortableHook.F_ID;
    }

    public int getClassId() {
        return MapPortableHook.PUT_ALL;
    }

    @Override
    protected Operation prepareOperation() {
        PutAllOperation operation = new PutAllOperation(name, entrySet);
        operation.setPartitionId(partitionId);
        return operation;
    }

    @Override
    protected int getPartition() {
        return partitionId;
    }

    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    public void write(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
        writer.writeInt("p", partitionId);
        ObjectDataOutput output = writer.getRawDataOutput();
        entrySet.writeData(output);
    }

    public void read(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
        partitionId = reader.readInt("p");
        ObjectDataInput input = reader.getRawDataInput();
        entrySet = new MapEntrySet();
        entrySet.readData(input);
    }

    public Permission getRequiredPermission() {
        return new MapPermission(name, ActionConstants.ACTION_PUT);
    }

    @Override
    public String getDistributedObjectName() {
        return name;
    }

    @Override
    public String getMethodName() {
        return "putAll";
    }

    @Override
    public Object[] getParameters() {
        final HashMap map = new HashMap();
        for (Map.Entry<Data, Data> entry : entrySet.getEntrySet()) {
            map.put(entry.getKey(), entry.getValue());
        }
        return new Object[]{map};
    }
}
