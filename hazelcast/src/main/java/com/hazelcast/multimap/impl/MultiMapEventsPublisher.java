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

package com.hazelcast.multimap.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.map.impl.event.EntryEventData;
import com.hazelcast.map.impl.event.MapEventData;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.eventservice.EventRegistration;
import com.hazelcast.spi.impl.eventservice.EventService;

import java.util.Collection;

/**
 * Publishes multimap entry and map events through eventService
 */
public class MultiMapEventsPublisher {

    private final NodeEngine nodeEngine;

    public MultiMapEventsPublisher(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    public void publishMultiMapEvent(String mapName, EntryEventType eventType, int numberOfEntriesAffected) {
        EventService eventService = nodeEngine.getEventService();
        Collection<EventRegistration> registrations = eventService.getRegistrations(MultiMapService.SERVICE_NAME, mapName);
        if (registrations.isEmpty()) {
            return;
        }
        Address caller = nodeEngine.getThisAddress();
        String source = caller.toString();
        MapEventData mapEventData = new MapEventData(source, mapName, caller, eventType.getType(), numberOfEntriesAffected);
        eventService.publishEvent(MultiMapService.SERVICE_NAME, registrations, mapEventData, mapName.hashCode());
    }

    public final void publishEntryEvent(String multiMapName, EntryEventType eventType, Data key, Object newValue,
                                        Object oldValue) {
        EventService eventService = nodeEngine.getEventService();
        Collection<EventRegistration> registrations = eventService.getRegistrations(MultiMapService.SERVICE_NAME, multiMapName);
        for (EventRegistration registration : registrations) {
            MultiMapEventFilter filter = (MultiMapEventFilter) registration.getFilter();
            if (filter.getKey() == null || filter.getKey().equals(key)) {
                Data dataNewValue = filter.isIncludeValue() ? nodeEngine.toData(newValue) : null;
                Data dataOldValue = filter.isIncludeValue() ? nodeEngine.toData(oldValue) : null;
                Address caller = nodeEngine.getThisAddress();
                String source = caller.toString();
                EntryEventData event = new EntryEventData(source, multiMapName, caller, key, dataNewValue, dataOldValue,
                        eventType.getType());
                eventService.publishEvent(MultiMapService.SERVICE_NAME, registration, event, multiMapName.hashCode());
            }
        }
    }
}
