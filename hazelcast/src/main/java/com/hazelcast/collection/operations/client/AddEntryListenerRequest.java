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

package com.hazelcast.collection.operations.client;

import com.hazelcast.client.CallableClientRequest;
import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.ClientEngine;
import com.hazelcast.client.InitializingObjectRequest;
import com.hazelcast.collection.CollectionPortableHook;
import com.hazelcast.collection.CollectionProxyId;
import com.hazelcast.collection.CollectionService;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.impl.PortableEntryEvent;

import java.io.IOException;

/**
 * @author ali 5/10/13
 */
public class AddEntryListenerRequest extends CallableClientRequest implements Portable, InitializingObjectRequest {

    CollectionProxyId proxyId;
    Data key;
    boolean includeValue;

    public AddEntryListenerRequest() {
    }

    public AddEntryListenerRequest(CollectionProxyId proxyId, Data key, boolean includeValue) {
        this.proxyId = proxyId;
        this.key = key;
        this.includeValue = includeValue;
    }

    public Object call() throws Exception {
        final ClientEndpoint endpoint = getEndpoint();
        final ClientEngine clientEngine = getClientEngine();
        final CollectionService service = getService();
        EntryListener listener = new EntryListener() {

            public void entryAdded(EntryEvent event) {
                send(event);
            }

            public void entryRemoved(EntryEvent event) {
                send(event);
            }

            public void entryUpdated(EntryEvent event) {
                send(event);
            }

            public void entryEvicted(EntryEvent event) {
                send(event);
            }

            private void send(EntryEvent event) {
                if (endpoint.live()) {
                    Data key = clientEngine.toData(event.getKey());
                    Data value = clientEngine.toData(event.getValue());
                    Data oldValue = clientEngine.toData(event.getOldValue());
                    PortableEntryEvent portableEntryEvent = new PortableEntryEvent(key, value, oldValue, event.getEventType(), event.getMember().getUuid());
                    clientEngine.sendResponse(endpoint, portableEntryEvent);
                }
            }
        };
        String registrationId = service.addListener(proxyId.getName(), listener, key, includeValue, false);
        endpoint.setListenerRegistration(CollectionService.SERVICE_NAME, proxyId.getName(), registrationId);
        return null;
    }

    public String getServiceName() {
        return CollectionService.SERVICE_NAME;
    }

    public Object getObjectId() {
        return proxyId;
    }

    public int getFactoryId() {
        return CollectionPortableHook.F_ID;
    }

    public int getClassId() {
        return CollectionPortableHook.ADD_ENTRY_LISTENER;
    }

    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeBoolean("i", includeValue);
        final ObjectDataOutput out = writer.getRawDataOutput();
        proxyId.writeData(out);
        IOUtil.writeNullableData(out, key);
    }

    public void readPortable(PortableReader reader) throws IOException {
        includeValue = reader.readBoolean("i");
        final ObjectDataInput in = reader.getRawDataInput();
        proxyId = new CollectionProxyId();
        proxyId.readData(in);
        key = IOUtil.readNullableData(in);

    }
}
