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
import com.hazelcast.map.operation.AddIndexOperationFactory;
import com.hazelcast.map.MapPortableHook;
import com.hazelcast.map.MapService;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.OperationFactory;

import java.io.IOException;
import java.util.Map;

public class MapAddIndexRequest extends AllPartitionsClientRequest implements Portable, InitializingObjectRequest {

    private String name;
    private String attribute;
    private boolean ordered;

    public MapAddIndexRequest() {
    }

    public MapAddIndexRequest(String name, String attribute, boolean ordered) {
        this.name = name;
        this.attribute = attribute;
        this.ordered = ordered;
    }


    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return MapPortableHook.F_ID;
    }

    public int getClassId() {
        return MapPortableHook.ADD_INDEX;
    }

    @Override
    public Object getObjectId() {
        return name;
    }

    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
        writer.writeUTF("a", attribute);
        writer.writeBoolean("o", ordered);
    }

    public void readPortable(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
        attribute = reader.readUTF("a");
        ordered = reader.readBoolean("o");
    }

    @Override
    protected OperationFactory createOperationFactory() {
        return new AddIndexOperationFactory(name, attribute, ordered);
    }

    @Override
    protected Object reduce(Map<Integer, Object> map) {
        return null;
    }


}
