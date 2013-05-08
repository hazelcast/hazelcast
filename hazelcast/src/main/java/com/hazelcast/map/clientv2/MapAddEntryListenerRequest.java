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

package com.hazelcast.map.clientv2;

import com.hazelcast.clientv2.ClientEndpoint;
import com.hazelcast.clientv2.ClientEngine;
import com.hazelcast.clientv2.RunnableClientRequest;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.map.MapPortableHook;
import com.hazelcast.map.MapService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

public class MapAddEntryListenerRequest extends RunnableClientRequest {

    private String name;
    private Data key;
    private boolean includeValue;

    public MapAddEntryListenerRequest() {
    }

    public MapAddEntryListenerRequest(String name) {
        this.name = name;
    }

    public MapAddEntryListenerRequest(String name, Data key, boolean includeValue) {
        this.name = name;
        this.includeValue = includeValue;
        this.key = key;
    }

    @Override
    public void run() {
        final ClientEndpoint endpoint = getEndpoint();
        final ClientEngine clientEngine = getClientEngine();
        final MapService mapService = getService();
        EntryListener<Object, Object> listener = new EntryAdapter<Object, Object>() {
            public void entryAdded(EntryEvent<Object, Object> event) {
                if (endpoint.getConn().live()) {
                    clientEngine.sendResponse(endpoint, event.toString());
                } else {
                    System.err.println("De-registering listener for " + name);
                    mapService.removeEventListener(this, name, null);
                }
            }
        };
        mapService.addLocalEventListener(listener, name);

    }

    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return MapPortableHook.F_ID;
    }

    public int getClassId() {
        return MapPortableHook.ADD_ENTRY_LISTENER;
    }

    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeUTF("name", name);
        writer.writeBoolean("i", includeValue);
        final boolean hasKey = key != null;
        writer.writeBoolean("key", hasKey);

        if (hasKey) {
            final ObjectDataOutput out = writer.getRawDataOutput();
            key.writeData(out);
        }
    }

    public void readPortable(PortableReader reader) throws IOException {
        name = reader.readUTF("name");
        includeValue = reader.readBoolean("i");
        boolean hasKey = reader.readBoolean("key");

        if (hasKey) {
            final ObjectDataInput in = reader.getRawDataInput();
            key = new Data();
            key.readData(in);
        }
    }
}
