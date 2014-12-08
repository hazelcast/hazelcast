package com.hazelcast.map.impl;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.EntryView;
import com.hazelcast.map.impl.wan.MapReplicationRemove;
import com.hazelcast.map.impl.wan.MapReplicationUpdate;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.query.impl.QueryEntry;
import com.hazelcast.spi.EventFilter;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.EventServiceImpl;
import com.hazelcast.wan.ReplicationEventObject;
import com.hazelcast.wan.WanReplicationPublisher;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

class MapEventPublisherSupport implements MapEventPublisher {

    private final MapServiceContext mapServiceContext;

    protected MapEventPublisherSupport(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
    }

    @Override
    public void publishWanReplicationUpdate(String mapName, EntryView entryView) {
        MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        MapReplicationUpdate replicationEvent = new MapReplicationUpdate(mapName, mapContainer.getWanMergePolicy(),
                entryView);
        mapContainer.getWanReplicationPublisher().publishReplicationEvent(mapServiceContext.serviceName(), replicationEvent);
    }

    @Override
    public void publishWanReplicationRemove(String mapName, Data key, long removeTime) {
        final MapReplicationRemove event = new MapReplicationRemove(mapName, key, removeTime);

        publishWanReplicationEventInternal(mapName, event);
    }

    @Override
    public void publishMapEvent(Address caller, String mapName, EntryEventType eventType,
                                int numberOfEntriesAffected) {

        final Collection<EventRegistration> registrations = getRegistrations(mapName);
        if (registrations.isEmpty()) {
            return;
        }
        final String source = getThisNodesAddress();
        final MapEventData mapEventData = new MapEventData(source, mapName, caller,
                eventType.getType(), numberOfEntriesAffected);

        publishEventInternal(registrations, mapEventData, mapName.hashCode());
    }

    @Override
    public void publishEvent(Address caller, String mapName, EntryEventType eventType,
                             Data dataKey, Data dataOldValue, Data dataValue) {
        publishEvent(caller, mapName, eventType, false, dataKey, dataOldValue, dataValue);
    }

    @Override
    public void publishEvent(Address caller, String mapName, EntryEventType eventType, boolean syntheticEvent,
                             final Data dataKey, Data dataOldValue, Data dataValue) {
        final Collection<EventRegistration> registrations = getRegistrations(mapName);
        if (registrations.isEmpty()) {
            return;
        }

        List<EventRegistration> registrationsWithValue = null;
        List<EventRegistration> registrationsWithoutValue = null;

        for (final EventRegistration candidate : registrations) {
            final EventFilter filter = candidate.getFilter();
            final Result result = applyEventFilter(filter, syntheticEvent, dataKey, dataOldValue, dataValue, eventType);

            registrationsWithValue = initRegistrationsWithValue(registrationsWithValue, result);
            registrationsWithoutValue = initRegistrationsWithoutValue(registrationsWithoutValue, result);

            registerCandidate(result, candidate, registrationsWithValue, registrationsWithoutValue);
        }

        final boolean withValueRegistrationExists = isNotEmpty(registrationsWithValue);
        final boolean withoutValueRegistrationExists = isNotEmpty(registrationsWithoutValue);

        if (!withValueRegistrationExists && !withoutValueRegistrationExists) {
            return;
        }

        final EntryEventData eventData = createEntryEventData(mapName, caller,
                dataKey, dataValue, dataOldValue, eventType.getType());
        final int orderKey = pickOrderKey(dataKey);

        if (withValueRegistrationExists) {
            publishEventInternal(registrationsWithValue, eventData, orderKey);
        }
        if (withoutValueRegistrationExists) {
            publishEventInternal(registrationsWithoutValue, eventData.cloneWithoutValues(), orderKey);
        }
    }

    private List<EventRegistration> initRegistrationsWithoutValue(List<EventRegistration> registrationsWithoutValue,
                                                                  Result result) {
        if (registrationsWithoutValue != null) {
            return registrationsWithoutValue;
        }

        if (Result.NO_VALUE_INCLUDED.equals(result)) {
            registrationsWithoutValue = new ArrayList<EventRegistration>();
        }
        return registrationsWithoutValue;
    }

    private List<EventRegistration> initRegistrationsWithValue(List<EventRegistration> registrationsWithValue,
                                                               Result result) {
        if (registrationsWithValue != null) {
            return registrationsWithValue;
        }

        if (Result.VALUE_INCLUDED.equals(result)) {
            registrationsWithValue = new ArrayList<EventRegistration>();
        }

        return registrationsWithValue;
    }

    private static <T> boolean isNotEmpty(Collection<T> collection) {
        return !(collection == null || collection.isEmpty());
    }

    private Result applyEventFilter(EventFilter filter, boolean syntheticEvent, Data dataKey,
                                    Data dataOldValue, Data dataValue, EntryEventType eventType) {

        // below, order of ifs are important.
        // QueryEventFilter is instance of EntryEventFilter.
        // SyntheticEventFilter wraps an event filter.
        if (filter instanceof SyntheticEventFilter) {
            if (syntheticEvent) {
                return Result.NONE;
            }
            final SyntheticEventFilter syntheticEventFilter = (SyntheticEventFilter) filter;
            filter = syntheticEventFilter.getFilter();
        }

        if (filter instanceof EventServiceImpl.EmptyFilter) {
            return Result.VALUE_INCLUDED;
        }


        if (filter instanceof QueryEventFilter) {
            return processQueryEventFilter(filter, eventType, dataKey, dataOldValue, dataValue);
        }

        if (filter instanceof EntryEventFilter) {
            return processEntryEventFilter(filter, dataKey);
        }


        throw new IllegalArgumentException("Unknown EventFilter type = [" + filter.getClass().getCanonicalName() + "]");
    }

    private Collection<EventRegistration> getRegistrations(String mapName) {
        final MapServiceContext mapServiceContext = this.mapServiceContext;
        final NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        final EventService eventService = nodeEngine.getEventService();
        final String serviceName = mapServiceContext.serviceName();

        return eventService.getRegistrations(serviceName, mapName);
    }

    private int pickOrderKey(Data key) {
        return key == null ? -1 : key.hashCode();
    }

    private void registerCandidate(Result result, EventRegistration candidate,
                                   Collection<EventRegistration> registrationsWithValue,
                                   Collection<EventRegistration> registrationsWithoutValue) {
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

    private void publishEventInternal(Collection<EventRegistration> registrations, Object eventData, int orderKey) {
        final MapServiceContext mapServiceContext = this.mapServiceContext;
        final NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        final EventService eventService = nodeEngine.getEventService();
        final String serviceName = mapServiceContext.serviceName();

        eventService.publishEvent(serviceName, registrations, eventData, orderKey);
    }

    private String getThisNodesAddress() {
        final NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        final Address thisAddress = nodeEngine.getThisAddress();
        return thisAddress.toString();
    }

    private Result processEntryEventFilter(EventFilter filter, Data dataKey) {
        final EntryEventFilter eventFilter = (EntryEventFilter) filter;

        if (!eventFilter.eval(dataKey)) {
            return Result.NONE;
        }

        if (eventFilter.isIncludeValue()) {
            return Result.VALUE_INCLUDED;
        }

        return Result.NO_VALUE_INCLUDED;
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
        final Object key = serializationService.toObject(dataKey);
        final QueryEventFilter queryEventFilter = (QueryEventFilter) filter;
        final QueryEntry entry = new QueryEntry(serializationService, dataKey, key, testValue);
        if (queryEventFilter.eval(entry)) {
            return queryEventFilter.isIncludeValue() ? Result.VALUE_INCLUDED : Result.NO_VALUE_INCLUDED;
        }
        return Result.NONE;
    }

    private void publishWanReplicationEventInternal(String mapName, ReplicationEventObject event) {
        final MapServiceContext mapServiceContext = this.mapServiceContext;
        final MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        final String serviceName = mapServiceContext.serviceName();
        final WanReplicationPublisher wanReplicationPublisher = mapContainer.getWanReplicationPublisher();
        wanReplicationPublisher.publishReplicationEvent(serviceName, event);
    }

    private EntryEventData createEntryEventData(String mapName, Address caller,
                                                Data dataKey, Data dataNewValue, Data dataOldValue,
                                                int eventType) {
        final String thisNodesAddress = getThisNodesAddress();
        return new EntryEventData(thisNodesAddress, mapName, caller,
                dataKey, dataNewValue, dataOldValue, eventType);
    }

    private static enum Result {
        VALUE_INCLUDED,
        NO_VALUE_INCLUDED,
        NONE
    }
}
