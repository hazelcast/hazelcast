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

package com.hazelcast.collection.operations;

import com.hazelcast.collection.*;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.*;

import java.io.IOException;
import java.util.Collection;

/**
 * @ali 1/8/13
 */
public abstract class CollectionOperation extends AbstractOperation implements PartitionAwareOperation {

    private transient CollectionContainer container;

    transient Object response;

    CollectionProxyId proxyId;


    protected CollectionOperation() {
    }

    protected CollectionOperation(CollectionProxyId proxyId) {
        this.proxyId = proxyId;
    }

    public Object getResponse() {
        return response;
    }

    public String getServiceName() {
        return CollectionService.SERVICE_NAME;
    }

    public boolean hasListener() {
        EventService eventService = getNodeEngine().getEventService();
        Collection<EventRegistration> registrations = eventService.getRegistrations(getServiceName(), proxyId.getName());
        return registrations.size() > 0;
    }

    public void publishEvent(EntryEventType eventType, Data key, Object value) {
        NodeEngine engine = getNodeEngine();
        EventService eventService = engine.getEventService();
        Collection<EventRegistration> registrations = eventService.getRegistrations(CollectionService.SERVICE_NAME, proxyId.getName());
        for (EventRegistration registration : registrations) {
            CollectionEventFilter filter = (CollectionEventFilter) registration.getFilter();
            if (filter.getKey() != null && filter.getKey().equals(key)) {
                Data dataValue = filter.isIncludeValue() ? engine.toData(value) : null;
                CollectionEvent event = new CollectionEvent(proxyId.getName(), key, dataValue, eventType, engine.getThisAddress());
                eventService.publishEvent(CollectionService.SERVICE_NAME, registration, event);
            }
        }
    }

    public Object toObject(Object obj) {
        return getNodeEngine().toObject(obj);
    }

    public Data toData(Object obj) {
        return getNodeEngine().toData(obj);
    }

    public CollectionContainer getOrCreateContainer() {
        if (container == null) {
            CollectionService service = getService();
            container = service.getOrCreateCollectionContainer(getPartitionId(), proxyId);
        }
        return container;
    }

    public boolean isBinary() {
        return getOrCreateContainer().getConfig().isBinary();
    }

    public int getSyncBackupCount() {
        return getOrCreateContainer().getConfig().getSyncBackupCount();
    }

    public int getAsyncBackupCount() {
        return getOrCreateContainer().getConfig().getAsyncBackupCount();
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        proxyId.writeData(out);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        proxyId = new CollectionProxyId();
        proxyId.readData(in);
    }
}
