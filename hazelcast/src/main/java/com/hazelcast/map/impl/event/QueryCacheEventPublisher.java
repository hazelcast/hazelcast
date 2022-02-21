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

package com.hazelcast.map.impl.event;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.accumulator.Accumulator;
import com.hazelcast.map.impl.querycache.event.DefaultQueryCacheEventData;
import com.hazelcast.map.impl.querycache.event.QueryCacheEventData;
import com.hazelcast.map.impl.querycache.publisher.MapPublisherRegistry;
import com.hazelcast.map.impl.querycache.publisher.PartitionAccumulatorRegistry;
import com.hazelcast.map.impl.querycache.publisher.PublisherContext;
import com.hazelcast.map.impl.querycache.publisher.PublisherRegistry;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.eventservice.EventFilter;

import java.util.Collection;
import java.util.Collections;

import static com.hazelcast.core.EntryEventType.ADDED;
import static com.hazelcast.core.EntryEventType.REMOVED;
import static com.hazelcast.core.EntryEventType.UPDATED;
import static com.hazelcast.map.impl.event.MapEventPublisherImpl.isIncludeValue;
import static com.hazelcast.map.impl.querycache.event.QueryCacheEventDataBuilder.newQueryCacheEventDataBuilder;
import static com.hazelcast.internal.util.CollectionUtil.isEmpty;
import static com.hazelcast.internal.util.Preconditions.checkInstanceOf;

/**
 * Handles publishing of map events to continuous query caches
 *
 * @see com.hazelcast.map.QueryCache
 */
public class QueryCacheEventPublisher {

    private final FilteringStrategy filteringStrategy;
    private final QueryCacheContext queryCacheContext;
    private final InternalSerializationService serializationService;

    public QueryCacheEventPublisher(FilteringStrategy filteringStrategy, QueryCacheContext queryCacheContext,
                                    InternalSerializationService serializationService) {
        this.filteringStrategy = filteringStrategy;
        this.queryCacheContext = queryCacheContext;
        this.serializationService = serializationService;
    }

    public void addEventToQueryCache(Object eventData) {
        checkInstanceOf(EventData.class, eventData, "eventData");

        String mapName = ((EventData) eventData).getMapName();
        int eventType = ((EventData) eventData).getEventType();

        // this collection contains all defined query-caches on an IMap
        Collection<PartitionAccumulatorRegistry> partitionAccumulatorRegistries = getPartitionAccumulatorRegistries(mapName);
        if (isEmpty(partitionAccumulatorRegistries)) {
            return;
        }

        if (!(eventData instanceof EntryEventData)) {
            return;
        }

        EntryEventData entryEvenData = (EntryEventData) eventData;
        Data dataKey = entryEvenData.getDataKey();
        Data dataNewValue = entryEvenData.getDataNewValue();
        Data dataOldValue = entryEvenData.getDataOldValue();
        int partitionId = queryCacheContext.getPartitionId(entryEvenData.dataKey);

        for (PartitionAccumulatorRegistry registry : partitionAccumulatorRegistries) {
            DefaultQueryCacheEventData singleEventData = (DefaultQueryCacheEventData) convertQueryCacheEventDataOrNull(registry,
                    dataKey, dataNewValue, dataOldValue, eventType, partitionId, mapName);

            if (singleEventData == null) {
                continue;
            }

            Accumulator accumulator = registry.getOrCreate(partitionId);
            accumulator.accumulate(singleEventData);
        }
    }

    // TODO known issue: Locked keys will also be cleared from the query-cache after calling a map-wide event like clear/evictAll
    public void hintMapEvent(Address caller, String mapName, EntryEventType eventType, int numberOfEntriesAffected,
                             int partitionId) {

        // this collection contains all defined query-caches on this map.
        Collection<PartitionAccumulatorRegistry> partitionAccumulatorRegistries = getPartitionAccumulatorRegistries(mapName);
        for (PartitionAccumulatorRegistry accumulatorRegistry : partitionAccumulatorRegistries) {
            Accumulator accumulator = accumulatorRegistry.getOrCreate(partitionId);

            QueryCacheEventData singleEventData = newQueryCacheEventDataBuilder(false).withPartitionId(partitionId)
                    .withEventType(eventType.getType()).build();

            accumulator.accumulate(singleEventData);
        }
    }

    private QueryCacheEventData convertQueryCacheEventDataOrNull(PartitionAccumulatorRegistry registry, Data dataKey,
                                                                 Data dataNewValue, Data dataOldValue, int eventTypeId,
                                                                 int partitionId, String mapName) {
        EventFilter eventFilter = registry.getEventFilter();
        EntryEventType eventType = EntryEventType.getByType(eventTypeId);
        // when using Hazelcast default event filtering strategy, then let the CQC workaround kick-in
        // otherwise, just deliver the event if it matches the registry's predicate according to the configured
        // filtering strategy
        if (filteringStrategy instanceof DefaultEntryEventFilteringStrategy) {
            eventType = getCQCEventTypeOrNull(eventType, eventFilter, dataKey, dataNewValue, dataOldValue, mapName);
        } else {
            int producedEventTypeId = filteringStrategy.doFilter(eventFilter, dataKey, dataOldValue, dataNewValue,
                    eventType, mapName);
            if (producedEventTypeId == FilteringStrategy.FILTER_DOES_NOT_MATCH) {
                eventType = null;
            } else {
                eventType = EntryEventType.getByType(producedEventTypeId);
            }
        }
        if (eventType == null) {
            return null;
        }

        boolean includeValue = isIncludeValue(eventFilter);
        return newQueryCacheEventDataBuilder(includeValue)
                .withPartitionId(partitionId)
                .withDataKey(dataKey)
                .withDataNewValue(dataNewValue)
                .withEventType(eventType.getType())
                .withDataOldValue(dataOldValue)
                .withSerializationService((serializationService)).build();
    }

    // this method processes UPDATED events and may morph them into ADDED/REMOVED events
    // depending on old/new value matching the EventFilter. Fixes an issue that prevents proper CQC
    // implementation when DefaultEntryEventFilteringStrategy is in use. It is not used when any
    // other filtering strategy is in place
    private EntryEventType getCQCEventTypeOrNull(EntryEventType eventType, EventFilter eventFilter,
                                                 Data dataKey, Data dataNewValue, Data dataOldValue, String mapName) {
        boolean newValueMatching = filteringStrategy.doFilter(eventFilter, dataKey, dataOldValue, dataNewValue,
                eventType, mapName) != FilteringStrategy.FILTER_DOES_NOT_MATCH;
        if (eventType == UPDATED) {
            // UPDATED event has a special handling as it might result in either ADDING or REMOVING an entry to/from CQC
            // depending on a predicate
            boolean oldValueMatching = filteringStrategy.doFilter(eventFilter, dataKey, null, dataOldValue,
                    EntryEventType.ADDED, mapName) != FilteringStrategy.FILTER_DOES_NOT_MATCH;
            if (oldValueMatching) {
                if (!newValueMatching) {
                    eventType = REMOVED;
                }
            } else {
                if (newValueMatching) {
                    eventType = ADDED;
                } else {
                    //neither old value nor new value is matching -> it's a non-event for the CQC
                    return null;
                }
            }
        } else if (!newValueMatching) {
            return null;
        }
        return eventType;
    }

    private Collection<PartitionAccumulatorRegistry> getPartitionAccumulatorRegistries(String mapName) {
        PublisherContext publisherContext = queryCacheContext.getPublisherContext();
        MapPublisherRegistry mapPublisherRegistry = publisherContext.getMapPublisherRegistry();
        PublisherRegistry publisherRegistry = mapPublisherRegistry.getOrNull(mapName);
        if (publisherRegistry == null) {
            return Collections.emptySet();
        }
        // this collection contains all query-caches for this map
        return publisherRegistry.getAll().values();
    }
}
