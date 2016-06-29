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
import com.hazelcast.map.impl.EntryEventFilter;
import com.hazelcast.map.impl.EventListenerFilter;
import com.hazelcast.map.impl.MapPartitionLostEventFilter;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.query.QueryEventFilter;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.EventFilter;
import com.hazelcast.spi.impl.eventservice.impl.TrueEventFilter;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.collection.Int2ObjectHashMap;

import java.util.Collection;
import java.util.Map;

import static com.hazelcast.core.EntryEventType.ADDED;
import static com.hazelcast.core.EntryEventType.EVICTED;
import static com.hazelcast.core.EntryEventType.EXPIRED;
import static com.hazelcast.core.EntryEventType.REMOVED;
import static com.hazelcast.core.EntryEventType.UPDATED;

/**
 * A filtering strategy that preserves the default behavior in most cases, but processes entry events for listeners with
 * predicates to fit with "query cache" concept. For example when the original event indicates an update from an old value that
 * does not match the predicate to a new value that does match, then the entry listener will be notified with an ADDED event.
 * This affects only map listeners with predicates and the way entry updates are handled. Put/remove operations are not
 * affected, neither are listeners without predicates.
 * This filtering strategy is used when Hazelcast property {@code hazelcast.map.entry.filtering.natural.event.types} is set
 * to {@code true}. The complete decision matrix for event types published with this filtering strategy.
 * <p>
 * <table>
 *     <tr>
 *         <td>Old value</td>
 *         <td>New value</td>
 *         <td>Published event type</td>
 *     </tr>
 *     <tr>
 *         <td>Match</td>
 *         <td>Mismatch</td>
 *         <td>REMOVED</td>
 *     </tr>
 *     <tr>
 *         <td>Match</td>
 *         <td>Match</td>
 *         <td>UPDATED</td>
 *     </tr>
 *     <tr>
 *         <td>Mismatch</td>
 *         <td>Mismatch</td>
 *         <td>NO MATCH (no event is triggered)</td>
 *     </tr>
 *     <tr>
 *         <td>Mismatch</td>
 *         <td>Match</td>
 *         <td>ADDED</td>
 *     </tr>
 * </table>
 */
public class QueryCacheNaturalFilteringStrategy extends AbstractFilteringStrategy {

    // Default capacity of event-type > EventData map.
    private static final int EVENT_DATA_MAP_CAPACITY = 4;

    public QueryCacheNaturalFilteringStrategy(SerializationService serializationService, MapServiceContext mapServiceContext) {
        super(serializationService, mapServiceContext);
    }

    @SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:cyclomaticcomplexity"})
    @Override
    public int doFilter(EventFilter filter, Data dataKey, Object dataOldValue, Object dataValue, EntryEventType eventType,
                        String mapNameOrNull) {
        if (filter instanceof MapPartitionLostEventFilter) {
            return FILTER_DOES_NOT_MATCH;
        }

        // Since the event type may change if we have a QueryEventFilter to evaluate,
        // the effective event type may change after execution of QueryEventFilter.eval
        EventListenerFilter filterAsEventListenerFilter = null;
        boolean originalFilterEventTypeMatches = true;
        if (filter instanceof EventListenerFilter) {
            // evaluate whether the filter matches the original event type
            originalFilterEventTypeMatches = filter.eval(eventType.getType());
            // hold a reference to the original event filter; this may be used later, in case there is a query event filter
            // and it alters the event type to be published
            filterAsEventListenerFilter = ((EventListenerFilter) filter);
            filter = ((EventListenerFilter) filter).getEventFilter();
        }
        if (originalFilterEventTypeMatches && filter instanceof TrueEventFilter) {
            return eventType.getType();
        }
        if (filter instanceof QueryEventFilter) {
            int effectiveEventType = processQueryEventFilterWithAlternativeEventType(filter, eventType, dataKey, dataOldValue,
                    dataValue, mapNameOrNull);
            if (effectiveEventType == FILTER_DOES_NOT_MATCH) {
                return FILTER_DOES_NOT_MATCH;
            } else {
                // query matches, also need to verify that the effective event type is one that the EventListenerFilter
                // wants to listen for (if effective event type != original event type
                if (filterAsEventListenerFilter != null && effectiveEventType != eventType.getType()) {
                    return filterAsEventListenerFilter.eval(effectiveEventType) ? effectiveEventType : FILTER_DOES_NOT_MATCH;
                } else {
                    return effectiveEventType;
                }
            }
        }
        if (filter instanceof EntryEventFilter) {
            return (originalFilterEventTypeMatches && processEntryEventFilter(filter, dataKey))
                    ? eventType.getType() : FILTER_DOES_NOT_MATCH;
        }
        throw new IllegalArgumentException("Unknown EventFilter type = [" + filter.getClass().getCanonicalName() + "]");
    }

    @Override
    public EntryEventDataCache getEntryEventDataCache() {
        return new EntryEventDataPerEventTypeCache();
    }

    @Override
    public String toString() {
        return "QueryCacheNaturalFilteringStrategy";
    }

    private int processQueryEventFilterWithAlternativeEventType(EventFilter filter, EntryEventType eventType,
                                                Data dataKey, Object dataOldValue, Object dataValue, String mapNameOrNull) {
        if (eventType == UPDATED) {
            // need to evaluate the filter on both old and new value and morph accordingly the event type
            boolean newValueMatches = evaluateQueryEventFilter(filter, dataKey, dataValue, mapNameOrNull);
            boolean oldValueMatches = evaluateQueryEventFilter(filter, dataKey, dataOldValue, mapNameOrNull);

            // decision matrix:
            // +-----------+-----------+---------+
            // | Old value | New value | Result  |
            // +-----------+-----------+---------+
            // |  Match    | Mismatch  | REMOVED |
            // |  Match    | Match     | UPDATED |
            // | Mismatch  | Mismatch  | NO MATCH|
            // | Mismatch  | Match     |  ADDED  |
            // +-----------+-----------+---------+
            if (oldValueMatches) {
                return newValueMatches ? UPDATED.getType() : REMOVED.getType();
            } else {
                return newValueMatches ? ADDED.getType() : FILTER_DOES_NOT_MATCH;
            }
        } else {
            Object testValue;
            if (eventType == REMOVED || eventType == EVICTED || eventType == EXPIRED) {
                testValue = dataOldValue;
            } else {
                testValue = dataValue;
            }
            return evaluateQueryEventFilter(filter, dataKey, testValue, mapNameOrNull)
                    ? eventType.getType() : FILTER_DOES_NOT_MATCH;
        }
    }

    // Caches EntryEventData per {eventType, includingValues}
    private class EntryEventDataPerEventTypeCache implements EntryEventDataCache {
        Map<Integer, EntryEventData> eventDataIncludingValues;
        Map<Integer, EntryEventData> eventDataExcludingValues;
        boolean empty = true;

        @Override
        public EntryEventData getOrCreateEventData(String mapName, Address caller, Data dataKey, Object newValue, Object oldValue,
                                                   Object mergingValue, int eventType, boolean includingValues) {
            if (includingValues) {
                if (eventDataIncludingValues == null) {
                    eventDataIncludingValues = new Int2ObjectHashMap<EntryEventData>(EVENT_DATA_MAP_CAPACITY);
                }
                return getOrCreateEventData(eventDataIncludingValues, mapName, caller, dataKey, newValue, oldValue, mergingValue,
                        eventType);
            } else {
                if (eventDataExcludingValues == null) {
                    eventDataExcludingValues = new Int2ObjectHashMap<EntryEventData>(EVENT_DATA_MAP_CAPACITY);
                }
                return getOrCreateEventData(eventDataExcludingValues, mapName, caller, dataKey, null, null, null,
                        eventType);
            }
        }

        @Override
        public boolean isEmpty() {
            return empty;
        }

        @Override
        public Collection<EntryEventData> eventDataIncludingValues() {
            return eventDataIncludingValues == null ? null : eventDataIncludingValues.values();
        }

        @Override
        public Collection<EntryEventData> eventDataExcludingValues() {
            return eventDataExcludingValues == null ? null : eventDataExcludingValues.values();
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
                Data dataOldValue = oldValue == null ? null : mapServiceContext.toData(oldValue);
                Data dataNewValue = newValue == null ? null : mapServiceContext.toData(newValue);
                Data dataMergingValue = mergingValue == null ? null : mapServiceContext.toData(mergingValue);

                EntryEventData entryEventData = new EntryEventData(getThisNodesAddress(), mapName, caller,
                        dataKey, dataNewValue, dataOldValue, dataMergingValue, eventType);
                eventDataPerEventType.put(eventType, entryEventData);
                if (empty) {
                    empty = false;
                }
                return entryEventData;
            }
        }
    }
}
