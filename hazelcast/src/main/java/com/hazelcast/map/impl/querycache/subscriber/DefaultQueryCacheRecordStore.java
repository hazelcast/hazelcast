/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.internal.eviction.EvictionListener;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.impl.querycache.subscriber.record.DataQueryCacheRecordFactory;
import com.hazelcast.map.impl.querycache.subscriber.record.ObjectQueryCacheRecordFactory;
import com.hazelcast.map.impl.querycache.subscriber.record.QueryCacheRecord;
import com.hazelcast.map.impl.querycache.subscriber.record.QueryCacheRecordFactory;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.query.impl.QueryEntry;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.util.Clock;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Default implementation of {@link QueryCacheRecordStore}.
 *
 * @see QueryCacheRecordStore
 */
class DefaultQueryCacheRecordStore implements QueryCacheRecordStore {

    private static final int DEFAULT_CACHE_CAPACITY = 1000;
    private final QueryCacheRecordHashMap cache;
    private final QueryCacheRecordFactory recordFactory;
    private final Indexes indexes;
    private final InternalSerializationService serializationService;
    private final EvictionOperator evictionOperator;

    public DefaultQueryCacheRecordStore(InternalSerializationService serializationService,
                                        Indexes indexes,
                                        QueryCacheConfig config, EvictionListener listener) {
        this.cache = new QueryCacheRecordHashMap(serializationService, DEFAULT_CACHE_CAPACITY);
        this.serializationService = serializationService;
        this.recordFactory = getRecordFactory(config.getInMemoryFormat());
        this.indexes = indexes;
        this.evictionOperator = new EvictionOperator(cache, config, listener, serializationService.getClassLoader());
    }

    private QueryCacheRecord accessRecord(QueryCacheRecord record) {
        if (record == null) {
            return null;
        }
        record.incrementAccessHit();
        record.setAccessTime(Clock.currentTimeMillis());
        return record;
    }

    private QueryCacheRecordFactory getRecordFactory(InMemoryFormat inMemoryFormat) {
        switch (inMemoryFormat) {
            case BINARY:
                return new DataQueryCacheRecordFactory(serializationService);
            case OBJECT:
                return new ObjectQueryCacheRecordFactory(serializationService);
            default:
                throw new IllegalArgumentException("Not a known format [" + inMemoryFormat + "]");
        }
    }

    @Override
    public QueryCacheRecord add(Data keyData, Data valueData) {
        evictionOperator.evictIfRequired();

        QueryCacheRecord newRecord = recordFactory.createRecord(valueData);
        QueryCacheRecord oldRecord = cache.put(keyData, newRecord);
        saveIndex(keyData, newRecord, oldRecord);

        return oldRecord;
    }

    private void saveIndex(Data keyData, QueryCacheRecord currentRecord, QueryCacheRecord oldRecord) {
        if (indexes.hasIndex()) {
            Object currentValue = currentRecord.getValue();
            QueryEntry queryEntry = new QueryEntry(serializationService, keyData, currentValue, Extractors.empty());
            Object oldValue = oldRecord == null ? null : oldRecord.getValue();
            indexes.saveEntryIndex(queryEntry, oldValue);
        }
    }

    @Override
    public QueryCacheRecord get(Data keyData) {
        QueryCacheRecord record = cache.get(keyData);
        return accessRecord(record);
    }

    @Override
    public QueryCacheRecord remove(Data keyData) {
        QueryCacheRecord oldRecord = cache.remove(keyData);
        if (oldRecord != null) {
            removeIndex(keyData, oldRecord.getValue());
        }
        return oldRecord;
    }

    private void removeIndex(Data keyData, Object value) {
        if (indexes.hasIndex()) {
            indexes.removeEntryIndex(keyData, value);
        }
    }

    @Override
    public boolean containsKey(Data keyData) {
        QueryCacheRecord record = get(keyData);
        return record != null;
    }

    @Override
    public boolean containsValue(Object value) {
        Collection<QueryCacheRecord> values = cache.values();
        for (QueryCacheRecord cacheRecord : values) {
            Object cacheRecordValue = cacheRecord.getValue();
            if (recordFactory.isEquals(cacheRecordValue, value)) {
                accessRecord(cacheRecord);
                return true;
            }
        }
        return false;
    }

    @Override
    public Set<Data> keySet() {
        return cache.keySet();
    }

    @Override
    public Set<Map.Entry<Data, QueryCacheRecord>> entrySet() {
        return cache.entrySet();
    }

    @Override
    public int clear() {
        int removeCount = 0;
        Set<Data> dataKeys = keySet();
        for (Data dataKey : dataKeys) {
            QueryCacheRecord oldRecord = remove(dataKey);
            if (oldRecord != null) {
                removeCount++;
            }
        }
        return removeCount;
    }

    @Override
    public boolean isEmpty() {
        return cache.isEmpty();
    }

    @Override
    public int size() {
        return cache.size();
    }
}
