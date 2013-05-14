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

package com.hazelcast.queue;

import com.hazelcast.core.ItemEventType;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.*;
import com.hazelcast.spi.exception.RetryableHazelcastException;

import java.io.IOException;
import java.util.Collection;

/**
 * @ali 12/6/12
 */
public abstract class QueueOperation extends Operation implements PartitionAwareOperation, IdentifiedDataSerializable {

    protected String name;

    protected long timeoutMillis;

    protected transient Object response;

    private transient QueueContainer container;

    protected QueueOperation() {
    }

    protected QueueOperation(String name) {
        this.name = name;
    }

    protected QueueOperation(String name, long timeoutMillis) {
        this.name = name;
        this.timeoutMillis = timeoutMillis;
    }

    protected final QueueContainer getOrCreateContainer() {
        if (container == null) {
            QueueService queueService = getService();
            try {
                container = queueService.getOrCreateContainer(name, this instanceof BackupOperation);
            } catch (Exception e) {
                throw new RetryableHazelcastException(e);
            }
        }
        return container;
    }

    public final Object getResponse() {
        return response;
    }

    public final String getServiceName() {
        return QueueService.SERVICE_NAME;
    }

    public final String getName() {
        return name;
    }

    public void afterRun() throws Exception {
    }

    public void beforeRun() throws Exception {
    }

    public final boolean returnsResponse() {
        return true;
    }

    public final long getWaitTimeoutMillis() {
        return timeoutMillis;
    }

    public boolean hasListener() {
        EventService eventService = getNodeEngine().getEventService();
        Collection<EventRegistration> registrations = eventService.getRegistrations(getServiceName(), name);
        return registrations.size() > 0;
    }

    public void publishEvent(ItemEventType eventType, Data data) {
        EventService eventService = getNodeEngine().getEventService();
        Collection<EventRegistration> registrations = eventService.getRegistrations(getServiceName(), name);
        for (EventRegistration registration : registrations) {
            QueueEventFilter filter = (QueueEventFilter) registration.getFilter();
            QueueEvent event = new QueueEvent(name, filter.isIncludeValue() ? data : null, eventType, getNodeEngine().getThisAddress());
            eventService.publishEvent(getServiceName(), registration, event);
        }
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeLong(timeoutMillis);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        timeoutMillis = in.readLong();
    }

    protected QueueService getQueueService(){
        return getService();
    }

    public int getFactoryId() {
        return QueueDataSerializerHook.F_ID;
    }
}
