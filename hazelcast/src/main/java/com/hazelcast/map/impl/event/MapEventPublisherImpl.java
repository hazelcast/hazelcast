/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.event;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.EntryView;
import com.hazelcast.map.impl.EntryEventFilter;
import com.hazelcast.map.impl.EventListenerFilter;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapPartitionLostEventFilter;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.query.QueryEventFilter;
import com.hazelcast.map.impl.wan.MapReplicationRemove;
import com.hazelcast.map.impl.wan.MapReplicationUpdate;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.EventFilter;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.eventservice.impl.TrueEventFilter;
import com.hazelcast.spi.properties.HazelcastProperty;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.collection.Int2ObjectHashMap;
import com.hazelcast.wan.ReplicationEventObject;
import com.hazelcast.wan.WanReplicationPublisher;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.map.impl.event.AbstractFilteringStrategy.FILTER_DOES_NOT_MATCH;
import static com.hazelcast.util.CollectionUtil.isEmpty;

public class MapEventPublisherImpl implements MapEventPublisher {

    /**
     * When {@code true}, enables processing of entry events for listeners with predicates to fit with "query cache" concept:
     * for example when the original event indicates an update from an old value that does not match the predicate to a new value
     * that does match, then the entry listener will be notified with an ADDED event.
     * This affects only map listeners with predicates and the way entry updates are handled. Put/remove operations are not
     * affected, neither are listeners without predicates.
     * Default value is {@code false}, to maintain compatible behavior with previous Hazelcast versions.
     */
    public static final HazelcastProperty LISTENER_WITH_PREDICATE_PRODUCES_NATURAL_EVENT_TYPES = new HazelcastProperty(
            "hazelcast.map.entry.filtering.natural.event.types", false);

    // Default capacity of event-type > EventData map. Since we expect at most 2 entries, 4 should never cause rehashing.
    public static final int EVENT_DATA_MAP_CAPACITY = 4;

    protected final MapServiceContext mapServiceContext;
    protected final NodeEngine nodeEngine;
    protected final SerializationService serializationService;
    protected final EventService eventService;
    protected final FilteringStrategy filteringStrategy;

    public MapEventPublisherImpl(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
        this.nodeEngine = mapServiceContext.getNodeEngine();
        this.serializationService = nodeEngine.getSerializationService();
        this.eventService = nodeEngine.getEventService();
        if (this.nodeEngine.getProperties().
                getBoolean(LISTENER_WITH_PREDICATE_PRODUCES_NATURAL_EVENT_TYPES)) {
            this.filteringStrategy = new QueryCacheNaturalFilteringStrategy(serializationService, mapServiceContext);
        } else {
            this.filteringStrategy = new DefaultEntryEventFilteringStrategy(serializationService, mapServiceContext);
        }
    }

    @Override
    public void publishWanReplicationUpdate(String mapName, EntryView entryView) {
        MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        MapReplicationUpdate replicationEvent
                = new MapReplicationUpdate(mapName, mapContainer.getWanMergePolicy(), entryView);
        mapContainer.getWanReplicationPublisher().publishReplicationEvent(SERVICE_NAME, replicationEvent);
    }

    @Override
    public void publishWanReplicationRemove(String mapName, Data key, long removeTime) {
        MapReplicationRemove event = new MapReplicationRemove(mapName, key, removeTime);
        publishWanReplicationEventInternal(mapName, event);
    }

    @Override
    public void publishWanReplicationUpdateBackup(String mapName, EntryView entryView) {
        // NOP
    }

    @Override
    public void publishWanReplicationRemoveBackup(String mapName, Data key, long removeTime) {
        // NOP
    }

    @Override
    public void publishMapEvent(Address caller, String mapName, EntryEventType eventType,
                                int numberOfEntriesAffected) {
        Collection<EventRegistration> mapsListenerRegistrations = getRegistrations(mapName);
        if (isEmpty(mapsListenerRegistrations)) {
            return;
        }

        Collection<EventRegistration> registrations = null;
        for (EventRegistration registration : mapsListenerRegistrations) {
            EventFilter filter = registration.getFilter();

            if (filter instanceof EventListenerFilter) {
                if (!filter.eval(eventType.getType())) {
                    continue;
                }
            }

            if (!(filter instanceof MapPartitionLostEventFilter)) {
                if (registrations == null) {
                    registrations = new ArrayList<EventRegistration>();
                }
                registrations.add(registration);
            }
        }

        if (isEmpty(registrations)) {
            return;
        }

        String source = getThisNodesAddress();
        MapEventData mapEventData = new MapEventData(source, mapName, caller, eventType.getType(), numberOfEntriesAffected);
        publishEventInternal(registrations, mapEventData, mapName.hashCode());
    }

    @Override
    public void publishEvent(Address caller, String mapName, EntryEventType eventType,
                             Data dataKey, Object dataOldValue, Object dataValue) {
        publishEvent(caller, mapName, eventType, dataKey, dataOldValue, dataValue, null);
    }


    @Override
    public void publishEvent(Address caller, String mapName, EntryEventType eventType,
                             Data dataKey, Object oldValue, Object value, Object mergingValue) {
        Collection<EventRegistration> registrations = getRegistrations(mapName);
        if (isEmpty(registrations)) {
            return;
        }

        publishEvent(registrations, caller, mapName, eventType, dataKey, oldValue, value, mergingValue);
    }

    private void publishEvent(Collection<EventRegistration> registrations, Address caller, String mapName,
                                     EntryEventType eventType, Data dataKey, Object oldValue, Object value,
                                     Object mergingValue) {

        Map<Integer, EntryEventData> eventDataIncludingValues = null;
        Map<Integer, EntryEventData> eventDataExcludingValues = null;
        int orderKey = pickOrderKey(dataKey);

        for (EventRegistration registration : registrations) {
            EventFilter filter = registration.getFilter();
            // a filtering strategy determines whether the event must be published on the specific
            // event registration and may alter the type of event to be published
            int eventTypeForPublishing = filteringStrategy.doFilter(filter, dataKey, oldValue, value, eventType, mapName);
            if (eventTypeForPublishing == FILTER_DOES_NOT_MATCH) {
                continue;
            }

            EntryEventData eventDataToBePublished;
            if (isIncludeValue(filter)) {
                if (eventDataIncludingValues == null) {
                    eventDataIncludingValues = new Int2ObjectHashMap<EntryEventData>(EVENT_DATA_MAP_CAPACITY);
                }
                eventDataToBePublished = getOrCreateEventData(eventDataIncludingValues, mapName, caller, dataKey,
                        value, oldValue, mergingValue, eventTypeForPublishing);
            } else {
                if (eventDataExcludingValues == null) {
                    eventDataExcludingValues = new Int2ObjectHashMap<EntryEventData>(EVENT_DATA_MAP_CAPACITY);
                }
                eventDataToBePublished = getOrCreateEventData(eventDataExcludingValues, mapName, caller, dataKey,
                        null, null, null, eventTypeForPublishing);
            }
            eventService.publishEvent(SERVICE_NAME, registration, eventDataToBePublished, orderKey);
        }

        // if events were generated, execute the post-publish hook on each one
        if (eventDataIncludingValues != null) {
            for (EntryEventData entryEventData : eventDataIncludingValues.values()) {
                postPublishEvent(entryEventData);
            }
        }
    }

    /**
     * Hook for actions to perform after any of {@code publishEvent} methods is executed.
     * This method will be invoked once per unique EntryEventData generated by {@code publishEvent},
     * regardless of the number of registrations on which the event is published.
     * @param eventData
     */
    protected void postPublishEvent(EntryEventData eventData) {
        // nop
    }

    /**
     * If an {@code EntryEventData} is already mapped to the given {@code eventType} in {@code Map eventDataPerEventType},
     * then return the mapped value, otherwise create the {@code EntryEventData}, put it in {@code Map eventDataPerEventType}
     * and return it.
     * @param eventDataPerEventType
     * @param mapName
     * @param caller
     * @param dataKey
     * @param newValue
     * @param oldValue
     * @param mergingValue
     * @param eventType
     * @return {@code EntryEventData} already cached in {@code Map eventDataPerEventType} for the given {@code eventType} or
     *          if not already cached, a new {@code EntryEventData} object.
     */
    private EntryEventData getOrCreateEventData(Map<Integer, EntryEventData> eventDataPerEventType, String mapName,
                                                Address caller, Data dataKey, Object newValue, Object oldValue,
                                                Object mergingValue, int eventType) {

        if (eventDataPerEventType.containsKey(eventType)) {
            return eventDataPerEventType.get(eventType);
        } else {
            Data dataOldValue = mapServiceContext.toData(oldValue);
            Data dataNewValue = mapServiceContext.toData(newValue);
            Data dataMergingValue = mapServiceContext.toData(mergingValue);

            EntryEventData entryEventData = createEntryEventData(mapName, caller, dataKey,
                    dataNewValue, dataOldValue, dataMergingValue, eventType);
            eventDataPerEventType.put(eventType, entryEventData);
            return entryEventData;
        }
    }

    protected Map<Integer, List<EventRegistration>> getOrCreateMap(Map<Integer, List<EventRegistration>> registrations) {
        return registrations == null ? new Int2ObjectHashMap<List<EventRegistration>>() : registrations;
    }

    /**
     * Add an {@code EventRegistration} to the batch of {@code EventRegistration}s associated with the given event type.
     * If a mapping {@code eventType} => {@code List&lt;EventRegistration&gt;} does not exist, it will be created.
     * @param registrations
     * @param registration
     * @param eventType
     */
    private void addRegistration(Map<Integer, List<EventRegistration>> registrations, EventRegistration registration,
                                        int eventType) {
        if (registrations.containsKey(eventType)) {
            registrations.get(eventType).add(registration);
        } else {
            LinkedList<EventRegistration> list = new LinkedList<EventRegistration>();
            list.add(registration);
            registrations.put(eventType, list);
        }
    }

    protected boolean isIncludeValue(EventFilter filter) {
        // the order of the following ifs is important!
        // QueryEventFilter is instance of EntryEventFilter
        // SyntheticEventFilter wraps an event filter
        if (filter instanceof EventListenerFilter) {
            filter = ((EventListenerFilter) filter).getEventFilter();
        }
        if (filter instanceof TrueEventFilter) {
            return true;
        }
        if (filter instanceof QueryEventFilter) {
            return ((QueryEventFilter) filter).isIncludeValue();
        }
        if (filter instanceof EntryEventFilter) {
            return ((EntryEventFilter) filter).isIncludeValue();
        }
        throw new IllegalArgumentException("Unknown EventFilter type = [" + filter.getClass().getCanonicalName() + "]");
    }

    @Override
    public void publishMapPartitionLostEvent(Address caller, String mapName, int partitionId) {
        Collection<EventRegistration> registrations = new LinkedList<EventRegistration>();
        for (EventRegistration registration : getRegistrations(mapName)) {
            if (registration.getFilter() instanceof MapPartitionLostEventFilter) {
                registrations.add(registration);
            }
        }

        if (registrations.isEmpty()) {
            return;
        }

        String thisNodesAddress = getThisNodesAddress();
        MapPartitionEventData eventData = new MapPartitionEventData(thisNodesAddress, mapName, caller, partitionId);
        publishEventInternal(registrations, eventData, partitionId);
    }

    @Override
    public void hintMapEvent(Address caller, String mapName, EntryEventType eventType,
                             int numberOfEntriesAffected, int partitionId) {
        // NOP
    }

    @Override
    public boolean hasEventListener(String mapName) {
        return eventService.hasEventRegistration(SERVICE_NAME, mapName);
    }

    protected Collection<EventRegistration> getRegistrations(String mapName) {
        return eventService.getRegistrations(SERVICE_NAME, mapName);
    }

    private int pickOrderKey(Data key) {
        return key == null ? -1 : key.hashCode();
    }

    protected void publishEventInternal(Collection<EventRegistration> registrations, Object eventData, int orderKey) {
        eventService.publishEvent(SERVICE_NAME, registrations, eventData, orderKey);
    }

    private String getThisNodesAddress() {
        Address thisAddress = nodeEngine.getThisAddress();
        return thisAddress.toString();
    }

    protected void publishWanReplicationEventInternal(String mapName, ReplicationEventObject event) {
        MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        WanReplicationPublisher wanReplicationPublisher = mapContainer.getWanReplicationPublisher();
        wanReplicationPublisher.publishReplicationEvent(SERVICE_NAME, event);
    }

    private EntryEventData createEntryEventData(String mapName, Address caller,
                                                Data dataKey, Data dataNewValue, Data dataOldValue,
                                                Data dataMergingValue, int eventType) {
        String thisNodesAddress = getThisNodesAddress();
        return new EntryEventData(thisNodesAddress, mapName, caller,
                dataKey, dataNewValue, dataOldValue, dataMergingValue, eventType);
    }
}
