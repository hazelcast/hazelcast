package com.hazelcast.map;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.IMapEvent;
import com.hazelcast.core.MapEvent;
import com.hazelcast.core.Member;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.query.impl.QueryEntry;
import com.hazelcast.spi.EventFilter;
import com.hazelcast.spi.EventPublishingService;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.EventServiceImpl;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;

/**
 * Contains map service event publisher functionality.
 */
abstract class MapEventPublisherSupport extends MapMigrationSupport
        implements EventPublishingService<EventData, EntryListener> {

    protected MapEventPublisherSupport(NodeEngine nodeEngine) {
        super(nodeEngine);
    }

    private void incrementEventStats(IMapEvent event) {
        final String mapName = event.getName();
        MapContainer mapContainer = getMapContainer(mapName);
        if (mapContainer.getMapConfig().isStatisticsEnabled()) {
            getLocalMapStatsImpl(mapName).incrementReceivedEvents();
        }
    }

    @Override
    public void dispatchEvent(EventData eventData, EntryListener listener) {
        if (eventData instanceof EntryEventData) {
            dispatchEntryEventData(eventData, listener);
        } else if (eventData instanceof MapEventData) {
            dispatchMapEventData(eventData, listener);
        } else {
            throw new IllegalArgumentException("Unknown map event data");
        }
    }

    private void dispatchMapEventData(EventData eventData, EntryListener listener) {
        final MapEventData mapEventData = (MapEventData) eventData;
        final Member member = getMemberOrNull(eventData);
        if (member == null) {
            return;
        }
        final MapEvent event = createMapEvent(mapEventData, member);
        dispatch0(event, listener);
        incrementEventStats(event);
    }

    private MapEvent createMapEvent(MapEventData mapEventData, Member member) {
        return new MapEvent(mapEventData.getMapName(), member,
                mapEventData.getEventType(), mapEventData.getNumberOfEntries());
    }

    private void dispatchEntryEventData(EventData eventData, EntryListener listener) {
        final EntryEventData entryEventData = (EntryEventData) eventData;
        final Member member = getMemberOrNull(eventData);

        final EntryEvent event = createDataAwareEntryEvent(entryEventData, member);
        dispatch0(event, listener);
        incrementEventStats(event);
    }

    private Member getMemberOrNull(EventData eventData) {
        final Member member = nodeEngine.getClusterService().getMember(eventData.getCaller());
        if (member == null) {
            if (logger.isLoggable(Level.INFO)) {
                logger.info("Dropping event " + eventData + " from unknown address:" + eventData.getCaller());
            }
        }
        return member;
    }

    private DataAwareEntryEvent createDataAwareEntryEvent(EntryEventData entryEventData, Member member) {
        return new DataAwareEntryEvent(member, entryEventData.getEventType(), entryEventData.getMapName(),
                entryEventData.getDataKey(), entryEventData.getDataNewValue(), entryEventData.getDataOldValue(),
                nodeEngine.getSerializationService());
    }

    private void dispatch0(IMapEvent event, EntryListener listener) {
        switch (event.getEventType()) {
            case ADDED:
                listener.entryAdded((EntryEvent) event);
                break;
            case EVICTED:
                listener.entryEvicted((EntryEvent) event);
                break;
            case UPDATED:
                listener.entryUpdated((EntryEvent) event);
                break;
            case REMOVED:
                listener.entryRemoved((EntryEvent) event);
                break;
            case EVICT_ALL:
                listener.mapEvicted((MapEvent) event);
                break;
            case CLEAR_ALL:
                listener.mapCleared((MapEvent) event);
                break;
            default:
                throw new IllegalArgumentException("Invalid event type: " + event.getEventType());
        }
    }


    public void publishMapEvent(Address caller, String mapName, EntryEventType eventType, int numberOfEntriesAffected) {
        final Collection<EventRegistration> registrations = nodeEngine.getEventService()
                .getRegistrations(getServiceName(), mapName);
        if (registrations.isEmpty()) {
            return;
        }
        final String source = nodeEngine.getThisAddress().toString();
        final MapEventData mapEventData = new MapEventData(source, mapName, caller, eventType.getType(), numberOfEntriesAffected);
        nodeEngine.getEventService().publishEvent(getServiceName(), registrations, mapEventData, mapName.hashCode());

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
            EventFilter filter = candidate.getFilter();
            if (filter instanceof EventServiceImpl.EmptyFilter) {
                result = processEmptyFilter();
            } else if (filter instanceof QueryEventFilter) {
                result = processQueryEventFilter(filter, eventType, dataKey, dataOldValue, dataValue);
            } else if (filter.eval(dataKey)) {
                result = processEntryEventFilter(filter);
            }
            registerCandidate(result, candidate, registrationsWithValue, registrationsWithoutValue);
        }
        if (registrationsWithValue.isEmpty() && registrationsWithoutValue.isEmpty()) {
            return;
        }
        dataValue = pickDataValue(eventType, dataOldValue, dataValue);
        final EntryEventData eventData = createEntryEventData(mapName, caller,
                dataKey, dataValue, dataOldValue, eventType.getType());
        final int orderKey = pickOrderKey(dataKey);
        publishWithValue(registrationsWithValue, eventData, orderKey);
        publishWithoutValue(registrationsWithoutValue, eventData, orderKey);
    }

    private Collection<EventRegistration> getCandidates(String mapName) {
        return nodeEngine.getEventService().getRegistrations(getServiceName(), mapName);
    }

    private int pickOrderKey(Data key) {
        return key == null ? -1 : key.hashCode();
    }

    private Data pickDataValue(EntryEventType eventType, Data dataOldValue, Data dataValue) {
        if (eventType == EntryEventType.REMOVED || eventType == EntryEventType.EVICTED) {
            return dataValue != null ? dataValue : dataOldValue;
        }
        return dataValue;
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
        nodeEngine.getEventService().publishEvent(getServiceName(),
                registrationsWithValue, event, orderKey);
    }

    private void publishWithoutValue(Set<EventRegistration> registrationsWithoutValue, EntryEventData event, int orderKey) {
        nodeEngine.getEventService().publishEvent(getServiceName(),
                registrationsWithoutValue, event.cloneWithoutValues(), orderKey);
    }

    private Result processEmptyFilter() {
        return Result.VALUE_INCLUDED;
    }

    private Result processQueryEventFilter(EventFilter filter, EntryEventType eventType,
                                           final Data dataKey, Data dataOldValue, Data dataValue) {
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
        return new EntryEventData(nodeEngine.getThisAddress().toString(), mapName, caller,
                dataKey, dataNewValue, dataOldValue, eventType);
    }

    private static enum Result {
        VALUE_INCLUDED,
        NO_VALUE_INCLUDED,
        NONE
    }
}
