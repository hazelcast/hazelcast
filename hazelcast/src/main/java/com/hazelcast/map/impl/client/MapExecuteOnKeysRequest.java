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

import com.hazelcast.client.impl.client.MultiPartitionClientRequest;
import com.hazelcast.client.impl.client.SecureRequest;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.MapEntrySet;
import com.hazelcast.map.impl.MapPortableHook;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.operation.MultipleEntryOperationFactory;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.spi.OperationFactory;
import java.io.IOException;
import java.security.Permission;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class MapExecuteOnKeysRequest extends MultiPartitionClientRequest implements Portable, SecureRequest {

    private String name;
    private EntryProcessor processor;
    private Set<Data> keys;

    public MapExecuteOnKeysRequest() {
    }

    public MapExecuteOnKeysRequest(String name, EntryProcessor processor, Set<Data> keys) {
        this.name = name;
        this.processor = processor;
        this.keys = keys;
    }

    @Override
    protected OperationFactory createOperationFactory() {
        return new MultipleEntryOperationFactory(name, keys, processor);
    }

    @Override
    protected Object reduce(Map<Integer, Object> map) {
        MapEntrySet result = new MapEntrySet();
        MapService mapService = getService();
        for (Object o : map.values()) {
            if (o != null) {
                MapEntrySet entrySet = (MapEntrySet) mapService.getMapServiceContext().toObject(o);
                Set<Map.Entry<Data, Data>> entries = entrySet.getEntrySet();
                for (Map.Entry<Data, Data> entry : entries) {
                    result.add(entry);
                }
            }
        }
        return result;
    }

    @Override
    public Collection<Integer> getPartitions() {
        InternalPartitionService partitionService = getClientEngine().getPartitionService();
        int partitions = partitionService.getPartitionCount();
        int capacity = Math.min(partitions, keys.size());
        Set<Integer> partitionIds = new HashSet<Integer>(capacity);
        Iterator<Data> iterator = keys.iterator();
        while (iterator.hasNext() && partitionIds.size() < partitions) {
            Data key = iterator.next();
            partitionIds.add(partitionService.getPartitionId(key));
        }
        return partitionIds;
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
        return MapPortableHook.EXECUTE_ON_KEYS;
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
        writer.writeInt("size", keys.size());
        ObjectDataOutput output = writer.getRawDataOutput();
        for (Data key : keys) {
            output.writeData(key);
        }
        output.writeObject(processor);
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
        int size = reader.readInt("size");
        keys = new HashSet<Data>();
        ObjectDataInput input = reader.getRawDataInput();
        for (int i = 0; i < size; i++) {
            Data key = input.readData();
            keys.add(key);
        }
        processor = input.readObject();
    }

    @Override
    public Permission getRequiredPermission() {
        return new MapPermission(name, ActionConstants.ACTION_PUT, ActionConstants.ACTION_REMOVE);
    }

    @Override
    public String getDistributedObjectName() {
        return name;
    }

    @Override
    public String getMethodName() {
        return "executeOnKeys";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{keys, processor};
    }
}
