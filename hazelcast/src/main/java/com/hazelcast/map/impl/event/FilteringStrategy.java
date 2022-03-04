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
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.eventservice.EventFilter;

/**
 * A filtering strategy determines whether an event must be published based on a specific filter
 * and if so, may alter the type of event that should be published.
 *
 * @see AbstractFilteringStrategy
 * @see DefaultEntryEventFilteringStrategy
 * @see QueryCacheNaturalFilteringStrategy
 */
public interface FilteringStrategy {

    /**
     * Used as return value from {@link #doFilter(EventFilter, Data, Object, Object, EntryEventType, String)}
     * to indicate that an event registration's filter does not match.
     */
     int FILTER_DOES_NOT_MATCH = -1;

    /**
     * Main entry point for filtering events according to given filter.
     *
     * @param filter        the event filter
     * @param dataKey       the event entry key
     * @param oldValue  the old value of the event entry
     * @param dataValue     the new value of the event entry
     * @param eventType     the event type
     * @param mapNameOrNull the map name. May be null if this is not a map event (e.g. cache event)
     * @return {@link #FILTER_DOES_NOT_MATCH} if the event does not match the filter, otherwise
     * the integer event type of the event to be published. This allows a filtering strategy
     * to alter the type of event that is actually published, depending on the attributes of the
     * individual event registration.
     */
    int doFilter(EventFilter filter, Data dataKey, Object oldValue, Object dataValue, EntryEventType eventType,
                 String mapNameOrNull);

    /**
     * @return a new instance of {@link EntryEventDataCache} implementation to be used with this filtering strategy
     */
    EntryEventDataCache getEntryEventDataCache();
}
