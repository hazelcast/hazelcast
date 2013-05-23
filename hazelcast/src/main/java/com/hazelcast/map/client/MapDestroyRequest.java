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

import com.hazelcast.client.CallableClientRequest;
import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.ClientEngine;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.map.EntryEventFilter;
import com.hazelcast.map.MapPortableHook;
import com.hazelcast.map.MapService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.EventFilter;
import com.hazelcast.util.MutableString;

import java.io.IOException;

public class MapDestroyRequest extends CallableClientRequest implements Portable {

    private String name;

    public MapDestroyRequest() {
    }

    public MapDestroyRequest(String name) {
        this.name = name;
    }

    @Override
    public Object call() {
        final MapService mapService = getService();
        mapService.destroyDistributedObject(name);
        return true;
    }

    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return MapPortableHook.F_ID;
    }

    public int getClassId() {
        return MapPortableHook.DESTROY;
    }

    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeUTF("name", name);
    }

    public void readPortable(PortableReader reader) throws IOException {
        name = reader.readUTF("name");
    }
}
