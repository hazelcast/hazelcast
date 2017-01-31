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

package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorHandler;
import com.hazelcast.map.impl.querycache.event.QueryCacheEventData;
import com.hazelcast.nio.serialization.Data;

/**
 * This handler is used to process event data in {@link SubscriberAccumulator}.
 */
class SubscriberAccumulatorHandler implements AccumulatorHandler<QueryCacheEventData> {

    private final InternalQueryCache queryCache;
    private final boolean includeValue;
    private final InternalSerializationService serializationService;

    public SubscriberAccumulatorHandler(boolean includeValue, InternalQueryCache queryCache,
                                        InternalSerializationService serializationService) {
        this.includeValue = includeValue;
        this.queryCache = queryCache;
        this.serializationService = serializationService;
    }

    @Override
    public void handle(QueryCacheEventData eventData, boolean ignored) {
        eventData.setSerializationService(serializationService);

        Data keyData = eventData.getDataKey();
        Data valueData = includeValue ? eventData.getDataNewValue() : null;

        int eventType = eventData.getEventType();
        EntryEventType entryEventType = EntryEventType.getByType(eventType);
        switch (entryEventType) {
            case ADDED:
            case UPDATED:
            case MERGED:
                queryCache.setInternal(keyData, valueData, false, entryEventType);
                break;
            case REMOVED:
            case EVICTED:
                queryCache.deleteInternal(keyData, false, entryEventType);
                break;
            // TODO if we want strongly consistent clear & evict, removal can be made based on sequence and partition-id.
            case CLEAR_ALL:
            case EVICT_ALL:
                queryCache.clearInternal(entryEventType);
                break;
            default:
                throw new IllegalArgumentException("Not a known type EntryEventType." + entryEventType);
        }
    }
}
