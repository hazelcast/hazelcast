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
import com.hazelcast.map.impl.EntryEventFilter;
import com.hazelcast.map.impl.EventListenerFilter;
import com.hazelcast.map.impl.MapPartitionLostEventFilter;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.nearcache.invalidation.UuidFilter;
import com.hazelcast.map.impl.query.QueryEventFilter;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.eventservice.EventFilter;
import com.hazelcast.spi.impl.eventservice.impl.TrueEventFilter;
import com.hazelcast.internal.util.collection.Int2ObjectHashMap;

import java.util.Collection;

import static com.hazelcast.core.EntryEventType.ADDED;
import static com.hazelcast.core.EntryEventType.EVICTED;
import static com.hazelcast.core.EntryEventType.EXPIRED;
import static com.hazelcast.core.EntryEventType.INVALIDATION;
import static com.hazelcast.core.EntryEventType.REMOVED;
import static com.hazelcast.core.EntryEventType.UPDATED;
import static com.hazelcast.internal.util.MapUtil.createInt2ObjectHashMap;

/**
 * A filtering strategy that preserves the default behavior in most cases, but processes entry events for listeners with
 * predicates to fit with "query cache" concept. For example when the original event indicates an update from an old value that
 * does not match the predicate to a new value that does match, then the entry listener will be notified with an ADDED event.
 * This affects only map listeners with predicates and the way entry updates are handled. Put/remove operations are not
 * affected, neither are listeners without predicates.
 * This filtering strategy is used when Hazelcast property {@code hazelcast.map.entry.filtering.natural.event.types} is set
 * to {@code true}. The complete decision matrix for event types published with this filtering strategy.
 * <p>
 * <table summary="">
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

    public QueryCacheNaturalFilteringStrategy(InternalSerializationService serializationService,
                                              MapServiceContext mapServiceContext) {
        super(serializationService, mapServiceContext);
    }

    @SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:cyclomaticcomplexity"})
    @Override
    public int doFilter(EventFilter filter, Data dataKey, Object oldValue, Object dataValue, EntryEventType eventType,
                        String mapNameOrNull) {
        if (filter instanceof MapPartitionLostEventFilter) {
            return FILTER_DOES_NOT_MATCH;
        }

        // Since the event type may change if we have a QueryEventFilter to evaluate,
        // the effective event type may change after execution of QueryEventFilter.eval
        EventListenerFilter filterAsEventListenerFilter = null;
        boolean originalFilterEventTypeMatches = true;

        if (filter instanceof EventListenerFilter) {
            int type = eventType.getType();
            if (type == INVALIDATION.getType()) {
                return FILTER_DOES_NOT_MATCH;
            }

            // evaluate whether the filter matches the original event type
            originalFilterEventTypeMatches = filter.eval(type);
            // hold a reference to the original event filter; this may be used later, in case there is a query event filter
            // and it alters the event type to be published
            filterAsEventListenerFilter = ((EventListenerFilter) filter);
            filter = ((EventListenerFilter) filter).getEventFilter();
            if (filter instanceof UuidFilter) {
                return FILTER_DOES_NOT_MATCH;
            }
        }

        if (filter instanceof TrueEventFilter) {
            return originalFilterEventTypeMatches ? eventType.getType() : FILTER_DOES_NOT_MATCH;
        }

        if (filter instanceof QueryEventFilter) {
            int effectiveEventType = processQueryEventFilterWithAlternativeEventType(filter, eventType, dataKey, oldValue,
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

    private int processQueryEventFilterWithAlternativeEventType(EventFilter filter,
                                                                EntryEventType eventType,
                                                                Data dataKey,
                                                                Object dataOldValue,
                                                                Object dataValue,
                                                                String mapNameOrNull) {
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

    /**
     * Caches 2 different {@link EntryEventData} objects for each event type - one for
     * including values and one for excluding values.
     */
    private class EntryEventDataPerEventTypeCache implements EntryEventDataCache {
        Int2ObjectHashMap<EntryEventData> eventDataIncludingValues;
        Int2ObjectHashMap<EntryEventData> eventDataExcludingValues;
        boolean empty = true;

        @Override
        public EntryEventData getOrCreateEventData(String mapName, Address caller, Data dataKey, Object newValue, Object oldValue,
                                                   Object mergingValue, int eventType, boolean includingValues) {
            if (includingValues) {
                if (eventDataIncludingValues == null) {
                    eventDataIncludingValues = createInt2ObjectHashMap(EVENT_DATA_MAP_CAPACITY);
                }
                return getOrCreateEventData(eventDataIncludingValues, mapName, caller, dataKey, newValue, oldValue, mergingValue,
                        eventType);
            } else {
                if (eventDataExcludingValues == null) {
                    eventDataExcludingValues = createInt2ObjectHashMap(EVENT_DATA_MAP_CAPACITY);
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
         *
         * @param eventDataPerEventType map from event type to cached event data
         * @param mapName               name of map
         * @param caller                the address of the caller that caused the event
         * @param dataKey               the key of the event map entry
         * @param newValue              the new value of the map entry
         * @param oldValue              the old value of the map entry
         * @param mergingValue          the value used when performing a merge operation in case of
         *                              a {@link EntryEventType#MERGED} event. This value together with the old value
         *                              produced the new value.
         * @param eventType             the event type
         * @return {@code EntryEventData} already cached in {@code Map eventDataPerEventType} for the given {@code eventType} or
         * if not already cached, a new {@code EntryEventData} object.
         */
        private EntryEventData getOrCreateEventData(Int2ObjectHashMap<EntryEventData> eventDataPerEventType, String mapName,
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
