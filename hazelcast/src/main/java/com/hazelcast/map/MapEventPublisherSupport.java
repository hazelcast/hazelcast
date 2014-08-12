package com.hazelcast.map;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.EntryView;
import com.hazelcast.map.wan.MapReplicationRemove;
import com.hazelcast.map.wan.MapReplicationUpdate;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.query.impl.QueryEntry;
import com.hazelcast.spi.EventFilter;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.EventServiceImpl;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

class MapEventPublisherSupport implements MapEventPublisher {

    private MapServiceContext mapServiceContext;

    protected MapEventPublisherSupport(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
    }

    public void publishWanReplicationUpdate(String mapName, EntryView entryView) {
        MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        MapReplicationUpdate replicationEvent = new MapReplicationUpdate(mapName, mapContainer.getWanMergePolicy(),
                entryView);
        mapContainer.getWanReplicationPublisher().publishReplicationEvent(mapServiceContext.serviceName(), replicationEvent);
    }

    public void publishWanReplicationRemove(String mapName, Data key, long removeTime) {
        MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        MapReplicationRemove replicationEvent = new MapReplicationRemove(mapName, key, removeTime);
        mapContainer.getWanReplicationPublisher().publishReplicationEvent(mapServiceContext.serviceName(), replicationEvent);
    }

    public void publishMapEvent(Address caller, String mapName, EntryEventType eventType, int numberOfEntriesAffected) {
        final NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        final Collection<EventRegistration> registrations = nodeEngine.getEventService()
                .getRegistrations(mapServiceContext.serviceName(), mapName);
        if (registrations.isEmpty()) {
            return;
        }
        final String source = nodeEngine.getThisAddress().toString();
        final MapEventData mapEventData = new MapEventData(source, mapName, caller,
                eventType.getType(), numberOfEntriesAffected);
        nodeEngine.getEventService().publishEvent(mapServiceContext.serviceName(),
                registrations, mapEventData, mapName.hashCode());

    }

    public void publishEvent(Address caller, String mapName, EntryEventType eventType,
                             final Data dataKey, Data dataOldValue, Data dataValue) {
        final Collection<EventRegistration> candidates = getCandidates(mapName);
        if (candidates.isEmpty()) {
            return;
        }
        final Set<EventRegistration> registrationsWithValue = new HashSet<EventRegistration>();
        final Set<EventRegistration> registrationsWithoutValue = new HashSet<EventRegistration>();
        // iterate on candidates.
        for (final EventRegistration candidate : candidates) {
            Result result = Result.NONE;
            final EventFilter filter = candidate.getFilter();
            if (emptyFilter(filter)) {
                result = processEmptyFilter();
            } else if (queryEventFilter(filter)) {
                result = processQueryEventFilter(filter, eventType, dataKey, dataOldValue, dataValue);
            } else if (filter.eval(dataKey)) {
                result = processEntryEventFilter(filter);
            }
            registerCandidate(result, candidate, registrationsWithValue, registrationsWithoutValue);
        }
        if (registrationsWithValue.isEmpty() && registrationsWithoutValue.isEmpty()) {
            return;
        }
        final EntryEventData eventData = createEntryEventData(mapName, caller,
                dataKey, dataValue, dataOldValue, eventType.getType());
        final int orderKey = pickOrderKey(dataKey);
        publishWithValue(registrationsWithValue, eventData, orderKey);
        publishWithoutValue(registrationsWithoutValue, eventData, orderKey);
    }

    private boolean emptyFilter(EventFilter filter) {
        return filter instanceof EventServiceImpl.EmptyFilter;
    }

    private boolean queryEventFilter(EventFilter filter) {
        return filter instanceof QueryEventFilter;
    }

    private Collection<EventRegistration> getCandidates(String mapName) {
        final NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        return nodeEngine.getEventService().getRegistrations(mapServiceContext.serviceName(), mapName);
    }

    private int pickOrderKey(Data key) {
        return key == null ? -1 : key.hashCode();
    }


    private void registerCandidate(Result result, EventRegistration candidate, Set<EventRegistration> registrationsWithValue,
                                   Set<EventRegistration> registrationsWithoutValue) {
        switch (result) {
            case VALUE_INCLUDED:
                registrationsWithValue.add(candidate);
                break;
            case NO_VALUE_INCLUDED:
                registrationsWithoutValue.add(candidate);
                break;
            case NONE:
                break;
            default:
                throw new IllegalArgumentException("Not a known result type [" + result + "]");
        }
    }

    private void publishWithValue(Set<EventRegistration> registrationsWithValue, EntryEventData event, int orderKey) {
        final NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        nodeEngine.getEventService().publishEvent(mapServiceContext.serviceName(),
                registrationsWithValue, event, orderKey);
    }

    private void publishWithoutValue(Set<EventRegistration> registrationsWithoutValue, EntryEventData event, int orderKey) {
        final NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        nodeEngine.getEventService().publishEvent(mapServiceContext.serviceName(),
                registrationsWithoutValue, event.cloneWithoutValues(), orderKey);
    }

    private Result processEmptyFilter() {
        return Result.VALUE_INCLUDED;
    }

    private Result processQueryEventFilter(EventFilter filter, EntryEventType eventType,
                                           final Data dataKey, Data dataOldValue, Data dataValue) {
        final NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        final SerializationService serializationService = nodeEngine.getSerializationService();
        Object testValue;
        if (eventType == EntryEventType.REMOVED || eventType == EntryEventType.EVICTED) {
            testValue = serializationService.toObject(dataOldValue);
        } else {
            testValue = serializationService.toObject(dataValue);
        }
        Object key = serializationService.toObject(dataKey);
        QueryEventFilter queryEventFilter = (QueryEventFilter) filter;
        QueryEntry entry = new QueryEntry(serializationService, dataKey, key, testValue);
        if (queryEventFilter.eval(entry)) {
            if (queryEventFilter.isIncludeValue()) {
                return Result.VALUE_INCLUDED;
            } else {
                return Result.NO_VALUE_INCLUDED;
            }
        }
        return Result.NONE;
    }

    private Result processEntryEventFilter(EventFilter filter) {
        EntryEventFilter eventFilter = (EntryEventFilter) filter;
        if (eventFilter.isIncludeValue()) {
            return Result.VALUE_INCLUDED;
        } else {
            return Result.NO_VALUE_INCLUDED;
        }
    }


    private EntryEventData createEntryEventData(String mapName, Address caller,
                                                Data dataKey, Data dataNewValue, Data dataOldValue, int eventType) {
        final NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        return new EntryEventData(nodeEngine.getThisAddress().toString(), mapName, caller,
                dataKey, dataNewValue, dataOldValue, eventType);
    }

    private static enum Result {
        VALUE_INCLUDED,
        NO_VALUE_INCLUDED,
        NONE
    }
}
