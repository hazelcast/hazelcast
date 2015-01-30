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

package com.hazelcast.collection;

import com.hazelcast.collection.list.ListContainer;
import com.hazelcast.collection.list.ListService;
import com.hazelcast.core.ItemEventType;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import java.io.IOException;
import java.util.Collection;

public abstract class CollectionOperation extends Operation
        implements PartitionAwareOperation, IdentifiedDataSerializable {

    protected String name;
    protected transient Object response;

    private transient CollectionContainer container;
    private CollectionType collectionType;

    protected CollectionOperation() {
    }

    protected CollectionOperation(CollectionType collectionType, String name) {
        this.name = name;
        this.collectionType = collectionType;
    }

    public CollectionType getCollectionType() {
        return collectionType;
    }

    @Override
    public String getServiceName() {
       return collectionType.getServiceName();
    }

    protected final ListContainer getOrCreateListContainer() {
        if (container == null) {
            ListService service = getService();
            try {
                container = service.getOrCreateContainer(name, this instanceof BackupOperation);
            } catch (Exception e) {
                throw new RetryableHazelcastException(e);
            }
        }
        return (ListContainer) container;
    }

    protected final CollectionContainer getOrCreateContainer() {
        if (container == null) {
            CollectionService service = getService();
            try {
                container = service.getOrCreateContainer(name, this instanceof BackupOperation);
            } catch (Exception e) {
                throw new RetryableHazelcastException(e);
            }
        }
        return container;
    }

    protected void publishEvent(ItemEventType eventType, Data data) {
        EventService eventService = getNodeEngine().getEventService();
        String serviceName = getServiceName();
        Collection<EventRegistration> registrations = eventService.getRegistrations(serviceName, name);
        final Address address = getNodeEngine().getThisAddress();
        for (EventRegistration registration : registrations) {
            CollectionEventFilter filter = (CollectionEventFilter) registration.getFilter();
            final boolean includeValue = filter.isIncludeValue();
            CollectionEvent event = new CollectionEvent(name, includeValue ? data : null, eventType, address);
            eventService.publishEvent(serviceName, registration, event, name.hashCode());
        }
    }

    public boolean hasEnoughCapacity(int delta) {
        return getOrCreateContainer().hasEnoughCapacity(delta);
    }

    @Override
    public int getFactoryId() {
        return CollectionDataSerializerHook.F_ID;
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeByte(collectionType.asByte());
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        collectionType = CollectionType.fromByte(in.readByte());
    }
}
