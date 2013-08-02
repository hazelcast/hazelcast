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

import com.hazelcast.client.AllPartitionsClientRequest;
import com.hazelcast.client.InitializingObjectRequest;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.MapEntrySet;
import com.hazelcast.map.MapPortableHook;
import com.hazelcast.map.MapService;
import com.hazelcast.map.operation.PartitionWideEntryOperationFactory;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.OperationFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class MapExecuteOnAllKeysRequest extends AllPartitionsClientRequest implements Portable, InitializingObjectRequest {

    private String name;
    private EntryProcessor processor;

    public MapExecuteOnAllKeysRequest() {
    }

    public MapExecuteOnAllKeysRequest(String name, EntryProcessor processor) {
        this.name = name;
        this.processor = processor;
    }

    @Override
    protected OperationFactory createOperationFactory() {
        return new PartitionWideEntryOperationFactory(name, processor);
    }

    @Override
    protected Object reduce(Map<Integer, Object> map) {
        MapEntrySet result = new MapEntrySet();
        MapService mapService = getService();
        for (Object o : map.values()) {
            if (o != null) {
                MapEntrySet entrySet = (MapEntrySet)mapService.toObject(o);
                Set<Map.Entry<Data,Data>> entries = entrySet.getEntrySet();
                for (Map.Entry<Data, Data> entry : entries) {
                    result.add(entry);
                }
            }
        }
        return result;
    }

    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public Object getObjectId() {
        return name;
    }

    @Override
    public int getFactoryId() {
        return MapPortableHook.F_ID;
    }

    public int getClassId() {
        return MapPortableHook.EXECUTE_ON_ALL_KEYS;
    }

    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
        final ObjectDataOutput out = writer.getRawDataOutput();
        out.writeObject(processor);
    }

    public void readPortable(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
        final ObjectDataInput in = reader.getRawDataInput();
        processor = in.readObject();
    }
}
