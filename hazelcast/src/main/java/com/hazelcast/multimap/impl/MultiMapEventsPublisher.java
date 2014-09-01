package com.hazelcast.multimap.impl;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.map.impl.EntryEventData;
import com.hazelcast.map.impl.MapEventData;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.NodeEngine;

import java.util.Collection;

/**
 * Publishes multimap entry and map events through eventService
 */
public class MultiMapEventsPublisher {

    private final NodeEngine nodeEngine;

    public MultiMapEventsPublisher(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    public void publishMultiMapEvent(String mapName, EntryEventType eventType,
                                     int numberOfEntriesAffected) {
        final EventService eventService = nodeEngine.getEventService();
        final Collection<EventRegistration> registrations =
                eventService.getRegistrations(MultiMapService.SERVICE_NAME, mapName);
        if (registrations.isEmpty()) {
            return;
        }
        final Address caller = nodeEngine.getThisAddress();
        final String source = caller.toString();
        final MapEventData mapEventData =
                new MapEventData(source, mapName, caller, eventType.getType(), numberOfEntriesAffected);
        eventService.publishEvent(MultiMapService.SERVICE_NAME, registrations, mapEventData, mapName.hashCode());

    }

    public final void publishEntryEvent(String multiMapName, EntryEventType eventType, Data key, Object value) {
        EventService eventService = nodeEngine.getEventService();
        Collection<EventRegistration> registrations =
                eventService.getRegistrations(MultiMapService.SERVICE_NAME, multiMapName);
        for (EventRegistration registration : registrations) {
            MultiMapEventFilter filter = (MultiMapEventFilter) registration.getFilter();
            if (filter.getKey() == null || filter.getKey().equals(key)) {
                Data dataValue = filter.isIncludeValue() ? nodeEngine.toData(value) : null;
                final Address caller = nodeEngine.getThisAddress();
                final String source = caller.toString();
                EntryEventData event =
                        new EntryEventData(source, multiMapName, caller, key, dataValue, null, eventType.getType());
                eventService.publishEvent(MultiMapService.SERVICE_NAME, registration, event, multiMapName.hashCode());
            }
        }
    }
}
