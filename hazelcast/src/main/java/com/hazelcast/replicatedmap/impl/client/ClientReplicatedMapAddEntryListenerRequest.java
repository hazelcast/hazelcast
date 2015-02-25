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

package com.hazelcast.replicatedmap.impl.client;

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.MapEvent;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.query.Predicate;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.ReplicatedMapPermission;

import java.io.IOException;
import java.security.Permission;

/**
 * Client request class for {@link com.hazelcast.core.ReplicatedMap#addEntryListener(com.hazelcast.core.EntryListener)}
 * implementation
 */
public class ClientReplicatedMapAddEntryListenerRequest
        extends AbstractReplicatedMapClientRequest {

    private Predicate predicate;
    private Object key;

    ClientReplicatedMapAddEntryListenerRequest() {
        super(null);
    }

    public ClientReplicatedMapAddEntryListenerRequest(String mapName, Predicate predicate, Object key) {
        super(mapName);
        this.predicate = predicate;
        this.key = key;
    }

    @Override
    public Object call()
            throws Exception {
        final ClientEndpoint endpoint = getEndpoint();
        final ReplicatedRecordStore replicatedRecordStore = getReplicatedRecordStore();
        final EntryListener<Object, Object> listener = new ClientReplicatedMapEntryListener();
        String registrationId;
        if (predicate == null) {
            registrationId = replicatedRecordStore.addEntryListener(listener, key);
        } else {
            registrationId = replicatedRecordStore.addEntryListener(listener, predicate, key);
        }
        endpoint.setListenerRegistration(ReplicatedMapService.SERVICE_NAME, getMapName(), registrationId);
        return registrationId;
    }

    /**
     * Client replicated map entry listener.
     */
    private class ClientReplicatedMapEntryListener implements EntryListener<Object, Object> {

        private void handleEvent(EntryEvent<Object, Object> event) {
            if (endpoint.isAlive()) {
                Object key = event.getKey();
                Object value = event.getValue();
                Object oldValue = event.getOldValue();
                EntryEventType eventType = event.getEventType();
                String uuid = event.getMember().getUuid();
                Portable portableEntryEvent = new ReplicatedMapPortableEntryEvent(key, value, oldValue, eventType, uuid);
                Data partitionKey = serializationService.toData(key);
                endpoint.sendEvent(partitionKey, portableEntryEvent, getCallId());
            }
        }

        @Override
        public void entryAdded(EntryEvent<Object, Object> event) {
            handleEvent(event);
        }

        @Override
        public void entryRemoved(EntryEvent<Object, Object> event) {
            handleEvent(event);
        }

        @Override
        public void entryUpdated(EntryEvent<Object, Object> event) {
            handleEvent(event);
        }

        @Override
        public void entryEvicted(EntryEvent<Object, Object> event) {
            handleEvent(event);
        }

        @Override
        public void mapEvicted(MapEvent event) {
            // TODO handle this event
        }

        @Override
        public void mapCleared(MapEvent event) {
            // TODO handle this event
        }
    }

    @Override
    public void write(PortableWriter writer)
            throws IOException {
        super.write(writer);
        ObjectDataOutput out = writer.getRawDataOutput();
        out.writeObject(key);
        out.writeObject(predicate);
    }

    @Override
    public void read(PortableReader reader)
            throws IOException {
        super.read(reader);
        ObjectDataInput in = reader.getRawDataInput();
        key = in.readObject();
        predicate = in.readObject();
    }

    @Override
    public int getClassId() {
        return ReplicatedMapPortableHook.ADD_LISTENER;
    }

    @Override
    public Permission getRequiredPermission() {
        return new ReplicatedMapPermission(getMapName(), ActionConstants.ACTION_LISTEN);
    }

    @Override
    public String getMethodName() {
        return "addEntryListener";
    }

    @Override
    public Object[] getParameters() {
        if (key == null && predicate == null) {
            return new Object[]{null};
        } else if (key == null && predicate != null) {
            return new Object[]{null, predicate};
        } else if (key != null && predicate == null) {
            return new Object[]{null, key};
        }
        return new Object[]{null, predicate, key};
    }
}
