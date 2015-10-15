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

package com.hazelcast.multimap.impl.client;

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.impl.client.BaseClientAddListenerRequest;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.MapEvent;
import com.hazelcast.map.impl.DataAwareEntryEvent;
import com.hazelcast.multimap.impl.MultiMapPortableHook;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MultiMapPermission;
import com.hazelcast.spi.impl.PortableEntryEvent;

import java.io.IOException;
import java.security.Permission;

public class AddEntryListenerRequest extends BaseClientAddListenerRequest {

    String name;
    Data key;
    boolean includeValue;

    public AddEntryListenerRequest() {
    }

    public AddEntryListenerRequest(String name, Data key, boolean includeValue) {
        this.name = name;
        this.key = key;
        this.includeValue = includeValue;
    }

    @Override
    public Object call() throws Exception {
        final ClientEndpoint endpoint = getEndpoint();
        final MultiMapService service = getService();
        EntryAdapter listener = new EntryAdapter() {
            @Override
            public void onEntryEvent(EntryEvent event) {
                if (endpoint.isAlive()) {
                    if (!(event instanceof DataAwareEntryEvent)) {
                        throw new IllegalArgumentException("Expecting: DataAwareEntryEvent, Found: "
                                + event.getClass().getSimpleName());
                    }
                    DataAwareEntryEvent dataAwareEntryEvent = (DataAwareEntryEvent) event;
                    Data key = dataAwareEntryEvent.getKeyData();
                    Data value = dataAwareEntryEvent.getNewValueData();
                    Data oldValue = dataAwareEntryEvent.getOldValueData();
                    Data mergingValue = dataAwareEntryEvent.getMergingValueData();
                    final EntryEventType type = event.getEventType();
                    final String uuid = event.getMember().getUuid();
                    PortableEntryEvent portableEntryEvent = new PortableEntryEvent(key, value, oldValue, mergingValue
                            , type, uuid);
                    endpoint.sendEvent(key, portableEntryEvent, getCallId());
                }
            }

            @Override
            public void onMapEvent(MapEvent event) {
                if (endpoint.isAlive()) {
                    PortableEntryEvent portableEntryEvent = toPortableEntryEvent(event);
                    endpoint.sendEvent(null, portableEntryEvent, getCallId());
                }
            }
        };
        String registrationId = service.addListener(name, listener, key, includeValue, localOnly);
        endpoint.addListenerDestroyAction(MultiMapService.SERVICE_NAME, name, registrationId);
        return registrationId;
    }

    private PortableEntryEvent toPortableEntryEvent(MapEvent event) {
        final EntryEventType type = event.getEventType();
        final String uuid = event.getMember().getUuid();
        return new PortableEntryEvent(type, uuid, event.getNumberOfEntriesAffected());
    }

    @Override
    public String getServiceName() {
        return MultiMapService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return MultiMapPortableHook.F_ID;
    }

    @Override
    public int getClassId() {
        return MultiMapPortableHook.ADD_ENTRY_LISTENER;
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        super.write(writer);
        writer.writeBoolean("i", includeValue);
        writer.writeUTF("n", name);
        final ObjectDataOutput out = writer.getRawDataOutput();
        out.writeData(key);
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        super.read(reader);
        includeValue = reader.readBoolean("i");
        name = reader.readUTF("n");
        final ObjectDataInput in = reader.getRawDataInput();
        key = in.readData();
    }

    @Override
    public Permission getRequiredPermission() {
        return new MultiMapPermission(name, ActionConstants.ACTION_LISTEN);
    }

    @Override
    public String getDistributedObjectName() {
        return name;
    }

    @Override
    public String getMethodName() {
        return "addEntryListener";
    }

    @Override
    public Object[] getParameters() {
        if (key != null) {
            return new Object[]{null, key, includeValue};
        }
        return new Object[]{null, includeValue};
    }
}
