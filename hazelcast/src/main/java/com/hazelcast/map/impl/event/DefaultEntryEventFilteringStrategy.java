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
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.EventFilter;
import com.hazelcast.spi.impl.eventservice.impl.TrueEventFilter;
import com.hazelcast.spi.serialization.SerializationService;

import static com.hazelcast.core.EntryEventType.EVICTED;
import static com.hazelcast.core.EntryEventType.EXPIRED;
import static com.hazelcast.core.EntryEventType.REMOVED;

/**
 * This entry event filtering strategy models the default Hazelcast behaviour.
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
}
