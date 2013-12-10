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

package com.hazelcast.replicatedmap.client;

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.ClientEngine;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.query.Predicate;
import com.hazelcast.replicatedmap.ReplicatedMapService;
import com.hazelcast.replicatedmap.record.ReplicatedRecordStore;

import java.io.IOException;

public class ClientReplicatedMapAddEntryListenerRequest extends AbstractReplicatedMapClientRequest {

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
    public Object call() throws Exception {
        final ClientEndpoint endpoint = getEndpoint();
        final ClientEngine clientEngine = getClientEngine();
        final ReplicatedRecordStore replicatedRecordStore = getReplicatedRecordStore();

        EntryListener<Object, Object> listener = new EntryListener<Object, Object>() {

            private void handleEvent(EntryEvent<Object, Object> event) {
                if (endpoint.live()) {
                    Object key = event.getKey();
                    Object value = event.getValue();
                    Object oldValue = event.getOldValue();
                    ReplicatedMapPortableEntryEvent portableEntryEvent =
                            new ReplicatedMapPortableEntryEvent(key, value, oldValue,
                                    event.getEventType(), event.getMember().getUuid());
                    clientEngine.sendResponse(endpoint, portableEntryEvent);
                }
            }

            public void entryAdded(EntryEvent<Object, Object> event) {
                handleEvent(event);
            }

            public void entryRemoved(EntryEvent<Object, Object> event) {
                handleEvent(event);
            }

            public void entryUpdated(EntryEvent<Object, Object> event) {
                handleEvent(event);
            }

            public void entryEvicted(EntryEvent<Object, Object> event) {
                handleEvent(event);
            }
        };

        String registrationId;
        if (predicate == null) {
            registrationId = replicatedRecordStore.addEntryListener(listener, key);
        } else {
            registrationId = replicatedRecordStore.addEntryListener(listener, predicate, key);
        }
        endpoint.setListenerRegistration(ReplicatedMapService.SERVICE_NAME, getMapName(), registrationId);
        return true;
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        super.writePortable(writer);
        ObjectDataOutput out = writer.getRawDataOutput();
        out.writeObject(key);
        out.writeObject(predicate);
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        super.readPortable(reader);
        ObjectDataInput in = reader.getRawDataInput();
        key = in.readObject();
        predicate = in.readObject();
    }

    @Override
    public int getClassId() {
        return ReplicatedMapPortableHook.ADD_LISTENER;
    }
}
