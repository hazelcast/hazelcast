/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

import java.io.IOException;

/**
 * @ali 5/10/13
 */
public class AddListenerRequest extends CallableClientRequest implements Portable {

    CollectionProxyId proxyId;

    Data key;

    boolean includeValue;

    boolean local;

    public AddListenerRequest() {
    }

    public AddListenerRequest(CollectionProxyId proxyId, Data key, boolean includeValue, boolean local) {
        this.proxyId = proxyId;
        this.key = key;
        this.includeValue = includeValue;
        this.local = local;
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

            private void send(EntryEvent event){
                if (endpoint.live()){
                    clientEngine.sendResponse(endpoint, event.toString());
                }
                else {
                    System.err.println("De-registering listener for " + proxyId);
//                    service.removeListener(proxyId.getName(), this, key);
                }
            }
        };
        service.addListener(proxyId.getName(), listener, key, includeValue, local);
        return null;
    }

    public String getServiceName() {
        return CollectionService.SERVICE_NAME;
    }

    public int getFactoryId() {
        return CollectionPortableHook.F_ID;
    }

    public int getClassId() {
        return CollectionPortableHook.ADD_LISTENER;
    }

    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeBoolean("i",includeValue);
        writer.writeBoolean("l",local);
        final ObjectDataOutput out = writer.getRawDataOutput();
        proxyId.writeData(out);
        IOUtil.writeNullableData(out, key);
    }

    public void readPortable(PortableReader reader) throws IOException {
        includeValue = reader.readBoolean("i");
        local = reader.readBoolean("l");
        final ObjectDataInput in = reader.getRawDataInput();
        proxyId = new CollectionProxyId();
        proxyId.readData(in);
        key = IOUtil.readNullableData(in);

    }
}
