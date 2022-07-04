/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.collection.impl.collection.operations;

import java.io.IOException;
import java.util.Collection;

import com.hazelcast.cluster.Address;
import com.hazelcast.collection.impl.collection.CollectionContainer;
import com.hazelcast.collection.impl.collection.CollectionDataSerializerHook;
import com.hazelcast.collection.impl.collection.CollectionEvent;
import com.hazelcast.collection.impl.collection.CollectionEventFilter;
import com.hazelcast.collection.impl.collection.CollectionService;
import com.hazelcast.collection.impl.list.ListContainer;
import com.hazelcast.collection.impl.list.ListService;
import com.hazelcast.core.ItemEventType;
import com.hazelcast.internal.monitor.impl.AbstractLocalCollectionStats;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.impl.eventservice.EventRegistration;
import com.hazelcast.spi.impl.eventservice.EventService;
import com.hazelcast.spi.impl.operationservice.BackupOperation;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;
import com.hazelcast.spi.impl.operationservice.NamedOperation;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.PartitionAwareOperation;
import com.hazelcast.spi.impl.operationservice.ReadonlyOperation;
import com.hazelcast.spi.tenantcontrol.TenantControl;

public abstract class CollectionOperation extends Operation
        implements NamedOperation, PartitionAwareOperation, IdentifiedDataSerializable {

    protected String name;

    protected transient Object response;

    private transient CollectionContainer container;

    protected CollectionOperation() {
    }

    protected CollectionOperation(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
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
        Collection<EventRegistration> registrations = eventService.getRegistrations(getServiceName(), name);
        final Address address = getNodeEngine().getThisAddress();
        for (EventRegistration registration : registrations) {
            CollectionEventFilter filter = (CollectionEventFilter) registration.getFilter();
            final boolean includeValue = filter.isIncludeValue();
            CollectionEvent event = new CollectionEvent(name, includeValue ? data : null, eventType, address);
            eventService.publishEvent(getServiceName(), registration, event, name.hashCode());
        }
    }

    public boolean hasEnoughCapacity(int delta) {
        return getOrCreateContainer().hasEnoughCapacity(delta);
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    public int getFactoryId() {
        return CollectionDataSerializerHook.F_ID;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeString(name);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        name = in.readString();
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);

        sb.append(", name=").append(name);
    }

    @Override
    public void afterRun() throws Exception {
        if (this instanceof ReadonlyOperation) {
            updateStatsForReadOnlyOperation();
        } else if (this instanceof MutatingOperation) {
            updateStatsForMutableOperation();
        }
        super.afterRun();
    }

    protected void updateStatsForReadOnlyOperation() {
        getLocalCollectionStats().setLastAccessTime(Clock.currentTimeMillis());
    }

    protected void updateStatsForMutableOperation() {
        long now = Clock.currentTimeMillis();
        getLocalCollectionStats().setLastAccessTime(now);
        getLocalCollectionStats().setLastUpdateTime(now);
    }

    private AbstractLocalCollectionStats getLocalCollectionStats() {
        CollectionService collectionService = getService();
        return collectionService.getLocalCollectionStats(name);
    }

    @Override
    public TenantControl getTenantControl() {
        CollectionService collectionService = getService();
        return getNodeEngine().getTenantControlService()
                .getTenantControl(collectionService.getServiceName(), name);
    }

    @Override
    public boolean requiresTenantContext() {
        return true;
    }
}
