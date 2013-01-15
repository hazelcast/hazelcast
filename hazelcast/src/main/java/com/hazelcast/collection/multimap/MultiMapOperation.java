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

package com.hazelcast.collection.multimap;

import com.hazelcast.collection.CollectionContainer;
import com.hazelcast.collection.CollectionEvent;
import com.hazelcast.collection.CollectionEventFilter;
import com.hazelcast.collection.CollectionService;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.*;
import com.hazelcast.spi.impl.AbstractNamedOperation;

import java.util.Collection;

/**
 * @ali 1/8/13
 */
public abstract class MultiMapOperation extends AbstractNamedOperation implements PartitionAwareOperation {

    transient Object response;

    protected MultiMapOperation() {
    }

    protected MultiMapOperation(String name) {
        super(name);
    }

    public CollectionContainer getContainer(){
        CollectionService service = getService();
        return service.getCollectionContainer(getPartitionId(), name);
    }

    public Object getResponse() {
        return response;
    }

    public String getServiceName() {
        return CollectionService.COLLECTION_SERVICE_NAME;
    }

    public boolean hasListener(){
        EventService eventService = getNodeEngine().getEventService();
        Collection<EventRegistration> registrations = eventService.getRegistrations(getServiceName(), name);
        return registrations.size() > 0;
    }

    public void publishEvent(EntryEventType eventType, Data key, Object value){
        NodeEngine engine = getNodeEngine();
        EventService eventService = engine.getEventService();
        Collection<EventRegistration> registrations = eventService.getRegistrations(CollectionService.COLLECTION_SERVICE_NAME, name);
        for (EventRegistration registration: registrations){
            CollectionEventFilter filter = (CollectionEventFilter)registration.getFilter();
            if (filter.getKey() != null && filter.getKey().equals(key)){
                Data dataValue = filter.isIncludeValue() ? engine.toData(value) : null;
                CollectionEvent event = new CollectionEvent(name,  key, dataValue, eventType, engine.getThisAddress());
                eventService.publishEvent(CollectionService.COLLECTION_SERVICE_NAME, registration, event);
            }
        }
    }

}
