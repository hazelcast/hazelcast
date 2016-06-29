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

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.impl.EntryEventFilter;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.query.QueryEventFilter;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.impl.CachedQueryEntry;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.spi.EventFilter;
import com.hazelcast.spi.serialization.SerializationService;

/**
 * A common abstract class to support implementations of filtering strategies.
 *
 * @see DefaultEntryEventFilteringStrategy
 * @see QueryCacheNaturalFilteringStrategy
 */
public abstract class AbstractFilteringStrategy implements FilteringStrategy {

    protected final SerializationService serializationService;
    protected final MapServiceContext mapServiceContext;

    public AbstractFilteringStrategy(SerializationService serializationService, MapServiceContext mapServiceContext) {
        this.serializationService = serializationService;
        this.mapServiceContext = mapServiceContext;
    }

    protected String getThisNodesAddress() {
        return mapServiceContext.getNodeEngine().getThisAddress().toString();
    }

    protected boolean processEntryEventFilter(EventFilter filter, Data dataKey) {
        EntryEventFilter eventFilter = (EntryEventFilter) filter;
        return eventFilter.eval(dataKey);
    }

    protected boolean evaluateQueryEventFilter(EventFilter filter, Data dataKey, Object testValue, String mapNameOrNull) {
        Extractors extractors = getExtractorsForMapName(mapNameOrNull);
        QueryEventFilter queryEventFilter = (QueryEventFilter) filter;
        QueryableEntry entry = new CachedQueryEntry((InternalSerializationService) serializationService,
                dataKey, testValue, extractors);
        return queryEventFilter.eval(entry);
    }

    private Extractors getExtractorsForMapName(String mapNameOrNull) {
        if (mapNameOrNull == null) {
            return Extractors.empty();
        }
        return mapServiceContext.getExtractors(mapNameOrNull);
    }
}
