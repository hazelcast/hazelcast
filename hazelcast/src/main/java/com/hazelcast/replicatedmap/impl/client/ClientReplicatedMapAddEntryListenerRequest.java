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
import com.hazelcast.client.impl.client.BaseClientAddListenerRequest;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.IMapEvent;
import com.hazelcast.core.MapEvent;
import com.hazelcast.core.Member;
import com.hazelcast.map.impl.DataAwareEntryEvent;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.query.Predicate;
import com.hazelcast.replicatedmap.ReplicatedMapCantBeCreatedOnLiteMemberException;
import com.hazelcast.replicatedmap.impl.ReplicatedMapEventPublishingService;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.replicatedmap.impl.record.ReplicatedEntryEventFilter;
import com.hazelcast.replicatedmap.impl.record.ReplicatedQueryEventFilter;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.ReplicatedMapPermission;

import java.io.IOException;
import java.security.Permission;

/**
 * Client request class for {@link com.hazelcast.core.ReplicatedMap#addEntryListener(com.hazelcast.core.EntryListener)}
 * implementation
 */
public class ClientReplicatedMapAddEntryListenerRequest extends BaseClientAddListenerRequest {

    private String mapName;
    private Predicate predicate;
    private Data key;

    ClientReplicatedMapAddEntryListenerRequest() {
    }

    public ClientReplicatedMapAddEntryListenerRequest(String mapName, Predicate predicate, Data key) {
        this.mapName = mapName;
        this.predicate = predicate;
        this.key = key;
    }

    @Override
    public Object call() throws Exception {
        if (getClientEngine().getConfig().isLiteMember()) {
            throw new ReplicatedMapCantBeCreatedOnLiteMemberException("Listener cannot be registered on lite members!");
        }
        final ClientEndpoint endpoint = getEndpoint();
        ReplicatedMapService service = getService();
        ReplicatedMapEventPublishingService eventPublishingService = service.getEventPublishingService();
        final EntryListener listener = new ClientReplicatedMapEntryListener();
        String registrationId;
        if (predicate == null) {
            registrationId = eventPublishingService.addEventListener(listener,
                    new ReplicatedEntryEventFilter(key), mapName);
        } else {
            registrationId = eventPublishingService.addEventListener(listener,
                    new ReplicatedQueryEventFilter(key, predicate), mapName);
        }
        endpoint.addListenerDestroyAction(ReplicatedMapService.SERVICE_NAME, mapName, registrationId);
        return registrationId;
    }

    /**
     * Client replicated map entry listener.
     */
    private class ClientReplicatedMapEntryListener implements EntryListener {

        private void handleEvent(EntryEvent event) {
            if (!shouldSendEvent(event)) {
                return;
            }

            if (!(event instanceof DataAwareEntryEvent)) {
                throw new IllegalArgumentException("Expecting: DataAwareEntryEvent, Found: "
                        + event.getClass().getSimpleName());
            }
            DataAwareEntryEvent dataAwareEntryEvent = (DataAwareEntryEvent) event;
            Data key = dataAwareEntryEvent.getKeyData();
            Data value = dataAwareEntryEvent.getNewValueData();
            Data oldValue = dataAwareEntryEvent.getOldValueData();
            EntryEventType eventType = event.getEventType();
            String uuid = event.getMember().getUuid();
            Portable portableEntryEvent = new ReplicatedMapPortableEntryEvent(key, value, oldValue, eventType, uuid);
            Data partitionKey = serializationService.toData(key);
            endpoint.sendEvent(partitionKey, portableEntryEvent, getCallId());
        }

        private void handleMapEvent(MapEvent event) {
            if (!shouldSendEvent(event)) {
                return;
            }

            final EntryEventType type = event.getEventType();
            final String uuid = event.getMember().getUuid();
            ReplicatedMapPortableEntryEvent portableEntryEvent = new ReplicatedMapPortableEntryEvent(null,
                    null, null, type, uuid, event.getNumberOfEntriesAffected());
            endpoint.sendEvent(null, portableEntryEvent, getCallId());
        }

        private boolean shouldSendEvent(IMapEvent event) {
            if (!endpoint.isAlive()) {
                return false;
            }

            Member originatedMember = event.getMember();
            if (localOnly && !getClientEngine().getLocalMember().equals(originatedMember)) {
                //if listener is registered local only, do not let the events originated from other members pass through
                return false;
            }
            return true;
        }

        @Override
        public void entryAdded(EntryEvent event) {
            handleEvent(event);
        }

        @Override
        public void entryRemoved(EntryEvent event) {
            handleEvent(event);
        }

        @Override
        public void entryUpdated(EntryEvent event) {
            handleEvent(event);
        }

        @Override
        public void entryEvicted(EntryEvent event) {
            handleEvent(event);
        }

        @Override
        public void mapEvicted(MapEvent event) {
            // TODO handle this event
        }

        @Override
        public void mapCleared(MapEvent event) {
            handleMapEvent(event);
        }
    }

    @Override
    public String getServiceName() {
        return ReplicatedMapService.SERVICE_NAME;
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        super.write(writer);
        writer.writeUTF("n", mapName);
        ObjectDataOutput out = writer.getRawDataOutput();
        out.writeData(key);
        out.writeObject(predicate);
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        super.read(reader);
        mapName = reader.readUTF("n");
        ObjectDataInput in = reader.getRawDataInput();
        key = in.readData();
        predicate = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return ReplicatedMapPortableHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ReplicatedMapPortableHook.ADD_LISTENER;
    }

    @Override
    public Permission getRequiredPermission() {
        return new ReplicatedMapPermission(mapName, ActionConstants.ACTION_LISTEN);
    }

    @Override
    public String getDistributedObjectName() {
        return mapName;
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
