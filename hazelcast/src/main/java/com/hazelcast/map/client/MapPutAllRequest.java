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
import com.hazelcast.map.MapEntrySet;
import com.hazelcast.map.MapPortableHook;
import com.hazelcast.map.operation.MapPutAllOperationFactory;
import com.hazelcast.map.MapService;
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

public class MapPutAllRequest extends AllPartitionsClientRequest implements Portable, InitializingObjectRequest {

    protected String name;
    private MapEntrySet entrySet;

    public MapPutAllRequest() {
    }

    public MapPutAllRequest(String name, MapEntrySet entrySet) {
        this.name = name;
        this.entrySet = entrySet;
    }

    public int getFactoryId() {
        return MapPortableHook.F_ID;
    }

    public int getClassId() {
        return MapPortableHook.PUT_ALL;
    }

    @Override
    protected OperationFactory createOperationFactory() {
        return new MapPutAllOperationFactory(name, entrySet);
    }

    @Override
    protected Object reduce(Map<Integer, Object> map) {
        MapEntrySet resultSet = new MapEntrySet();
        MapService mapService = getService();
        for (Map.Entry<Integer, Object> entry : map.entrySet()) {
            MapEntrySet mapEntrySet = (MapEntrySet) mapService.toObject(entry.getValue());
            Set<Map.Entry<Data, Data>> set = mapEntrySet.getEntrySet();
            for (Map.Entry<Data, Data> dataEntry : set) {
                resultSet.add(dataEntry);
            }
        }
        return resultSet;
    }

    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public Object getObjectId() {
        return name;
    }

    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
        ObjectDataOutput output = writer.getRawDataOutput();
        entrySet.writeData(output);
    }

    public void readPortable(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
        ObjectDataInput input = reader.getRawDataInput();
        entrySet = new MapEntrySet();
        entrySet.readData(input);
    }
}
