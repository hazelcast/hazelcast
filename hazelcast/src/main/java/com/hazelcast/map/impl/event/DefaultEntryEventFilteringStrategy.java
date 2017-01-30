/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import java.util.Collection;
import java.util.Collections;

import static com.hazelcast.core.EntryEventType.EVICTED;
import static com.hazelcast.core.EntryEventType.EXPIRED;
import static com.hazelcast.core.EntryEventType.REMOVED;

/**
 * This entry event filtering strategy models the default backwards compatible Hazelcast behaviour.
 * In particular, when processing {@code UPDATED} events, the predicate is evaluated against the new value; if the new value
 * matches the predicate, then the {@code UPDATED} event will be published to the registered listeners.
 * <p>
 * Note that when trying to build a continuous query cache, this filtering strategy is flawed, as the listener will not be
 * notified for updated entries whose old value matched the predicate while new value does not match the predicate. This has
 * been addressed in {@link QueryCacheNaturalFilteringStrategy}.
 * </p>
 *
 * @see QueryCacheNaturalFilteringStrategy
 */
public class DefaultEntryEventFilteringStrategy extends AbstractFilteringStrategy {

    public DefaultEntryEventFilteringStrategy(SerializationService serializationService, MapServiceContext mapServiceContext) {
        super(serializationService, mapServiceContext);
    }

    // This code has been moved from MapEventPublisherImpl.doFilter and
    // provides the default backwards compatible filtering strategy implementation.
    @SuppressWarnings("checkstyle:npathcomplexity")
    @Override
    public int doFilter(EventFilter filter, Data dataKey, Object dataOldValue, Object dataValue, EntryEventType eventType,
                        String mapNameOrNull) {
            if (filter instanceof MapPartitionLostEventFilter) {
                return FILTER_DOES_NOT_MATCH;
            }

            // the order of the following ifs is important!
            // QueryEventFilter is instance of EntryEventFilter
            if (filter instanceof EventListenerFilter) {
                if (!filter.eval(eventType.getType())) {
                    return FILTER_DOES_NOT_MATCH;
                } else {
                    filter = ((EventListenerFilter) filter).getEventFilter();
                }
            }
            if (filter instanceof TrueEventFilter) {
                return eventType.getType();
            }
            if (filter instanceof QueryEventFilter) {
                return processQueryEventFilter(filter, eventType, dataKey, dataOldValue, dataValue, mapNameOrNull)
                        ? eventType.getType() : FILTER_DOES_NOT_MATCH;
            }
            if (filter instanceof EntryEventFilter) {
                return processEntryEventFilter(filter, dataKey) ? eventType.getType() : FILTER_DOES_NOT_MATCH;
            }
            throw new IllegalArgumentException("Unknown EventFilter type = [" + filter.getClass().getCanonicalName() + "]");
    }

    @Override
    public EntryEventDataCache getEntryEventDataCache() {
        return new DefaultEntryEventDataCache();
    }

    @Override
    public String toString() {
        return "DefaultEntryEventFilteringStrategy";
    }

    private boolean processQueryEventFilter(EventFilter filter, EntryEventType eventType,
                                            Data dataKey, Object dataOldValue, Object dataValue, String mapNameOrNull) {
        Object testValue;
        if (eventType == REMOVED || eventType == EVICTED || eventType == EXPIRED) {
            testValue = dataOldValue;
        } else {
            testValue = dataValue;
        }

        return evaluateQueryEventFilter(filter, dataKey, testValue, mapNameOrNull);
    }

    private class DefaultEntryEventDataCache implements EntryEventDataCache {
        EntryEventData eventDataIncludingValues;
        EntryEventData eventDataExcludingValues;

        @Override
        public EntryEventData getOrCreateEventData(String mapName, Address caller, Data dataKey, Object newValue, Object oldValue,
                Object mergingValue, int eventType, boolean includingValues) {

            if (includingValues && eventDataIncludingValues != null) {
                return eventDataIncludingValues;
            } else if (!includingValues && eventDataExcludingValues != null) {
                return eventDataExcludingValues;
            } else {
                EntryEventData entryEventData = new EntryEventData(getThisNodesAddress(), mapName, caller, dataKey,
                        includingValues ? mapServiceContext.toData(newValue) : null,
                        includingValues ? mapServiceContext.toData(oldValue) : null,
                        includingValues ? mapServiceContext.toData(mergingValue) : null, eventType);

                if (includingValues) {
                    eventDataIncludingValues = entryEventData;
                } else {
                    eventDataExcludingValues = entryEventData;
                }
                return entryEventData;
            }
        }

        @Override
        public boolean isEmpty() {
            return eventDataIncludingValues == null && eventDataExcludingValues == null;
        }

        @Override
        public Collection<EntryEventData> eventDataIncludingValues() {
            return eventDataIncludingValues == null ? null : Collections.singleton(eventDataIncludingValues);
        }

        @Override
        public Collection<EntryEventData> eventDataExcludingValues() {
            return eventDataExcludingValues == null ? null : Collections.singleton(eventDataExcludingValues);
        }
    }
}
