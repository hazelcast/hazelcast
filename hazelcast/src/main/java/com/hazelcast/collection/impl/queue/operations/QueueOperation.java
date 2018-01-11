/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.collection.impl.queue.operations;

import com.hazelcast.collection.impl.queue.QueueContainer;
import com.hazelcast.collection.impl.queue.QueueDataSerializerHook;
import com.hazelcast.collection.impl.queue.QueueEvent;
import com.hazelcast.collection.impl.queue.QueueEventFilter;
import com.hazelcast.collection.impl.queue.QueueService;
import com.hazelcast.core.ItemEventType;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.NamedOperation;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.impl.AbstractNamedOperation;

import java.util.Collection;

/**
 * This class contains methods for Queue operations
 * such as {@link com.hazelcast.collection.impl.queue.operations.AddAllOperation}.
 */
public abstract class QueueOperation extends AbstractNamedOperation
        implements PartitionAwareOperation, IdentifiedDataSerializable, NamedOperation {

    protected transient Object response;

    private transient QueueContainer container;

    protected QueueOperation() {
    }

    protected QueueOperation(String name) {
        super(name);
    }

    protected QueueOperation(String name, long timeoutMillis) {
        this(name);
        setWaitTimeout(timeoutMillis);
    }

    protected final QueueContainer getContainer() {
        return container;
    }

    private void initializeContainer() {
        QueueService queueService = getService();
        try {
            container = queueService.getOrCreateContainer(name, this instanceof BackupOperation);
        } catch (Exception e) {
            throw new RetryableHazelcastException(e);
        }
    }

    @Override
    public final Object getResponse() {
        return response;
    }

    @Override
    public final String getServiceName() {
        return QueueService.SERVICE_NAME;
    }

    @Override
    public void afterRun() throws Exception {
    }

    @Override
    public void beforeRun() throws Exception {
        initializeContainer();
    }

    public boolean hasListener() {
        EventService eventService = getNodeEngine().getEventService();
        Collection<EventRegistration> registrations = eventService.getRegistrations(getServiceName(), name);
        return registrations.size() > 0;
    }

    public void publishEvent(ItemEventType eventType, Data data) {
        EventService eventService = getNodeEngine().getEventService();
        Collection<EventRegistration> registrations = eventService.getRegistrations(getServiceName(), name);
        Address thisAddress = getNodeEngine().getThisAddress();
        for (EventRegistration registration : registrations) {
            QueueEventFilter filter = (QueueEventFilter) registration.getFilter();
            QueueEvent event = new QueueEvent(name, filter.isIncludeValue() ? data : null, eventType, thisAddress);
            eventService.publishEvent(getServiceName(), registration, event, name.hashCode());
        }
    }

    protected QueueService getQueueService() {
        return getService();
    }

    @Override
    public int getFactoryId() {
        return QueueDataSerializerHook.F_ID;
    }
}
