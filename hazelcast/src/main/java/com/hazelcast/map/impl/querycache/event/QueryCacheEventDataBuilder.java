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

package com.hazelcast.map.impl.querycache.event;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.Data;

/**
 * Builder for creating a serializable event data for query cache system.
 */
public final class QueryCacheEventDataBuilder {

    private long sequence;
    private Data dataKey;
    private Data dataNewValue;
    private Data dataOldValue;
    private int eventType;
    private int partitionId;
    private InternalSerializationService serializationService;
    private final boolean includeValue;

    private QueryCacheEventDataBuilder(boolean includeValue) {
        this.includeValue = includeValue;
    }

    public static QueryCacheEventDataBuilder newQueryCacheEventDataBuilder(boolean includeValue) {
        return new QueryCacheEventDataBuilder(includeValue);
    }

    public QueryCacheEventDataBuilder withDataKey(Data dataKey) {
        this.dataKey = dataKey;
        return this;
    }

    public QueryCacheEventDataBuilder withDataNewValue(Data dataNewValue) {
        this.dataNewValue = includeValue ? dataNewValue : null;
        return this;
    }

    public QueryCacheEventDataBuilder withDataOldValue(Data dataOldValue) {
        this.dataOldValue = includeValue ? dataOldValue : null;
        return this;
    }

    public QueryCacheEventDataBuilder withSequence(long sequence) {
        this.sequence = sequence;
        return this;
    }

    public QueryCacheEventDataBuilder withEventType(int eventType) {
        this.eventType = eventType;
        return this;
    }

    public QueryCacheEventDataBuilder withPartitionId(int partitionId) {
        this.partitionId = partitionId;
        return this;
    }

    public QueryCacheEventDataBuilder withSerializationService(InternalSerializationService serializationService) {
        this.serializationService = serializationService;
        return this;
    }

    public QueryCacheEventData build() {
        DefaultQueryCacheEventData eventData = new DefaultQueryCacheEventData();
        eventData.setDataKey(dataKey);
        eventData.setDataNewValue(dataNewValue);
        eventData.setDataOldValue(dataOldValue);
        eventData.setSequence(sequence);
        eventData.setSerializationService(serializationService);
        eventData.setEventType(eventType);
        eventData.setPartitionId(partitionId);

        return eventData;
    }
}
