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

package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.internal.eviction.EvictionListener;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.map.impl.querycache.subscriber.record.DataQueryCacheRecordFactory;
import com.hazelcast.map.impl.querycache.subscriber.record.ObjectQueryCacheRecordFactory;
import com.hazelcast.map.impl.querycache.subscriber.record.QueryCacheRecord;
import com.hazelcast.map.impl.querycache.subscriber.record.QueryCacheRecordFactory;
import com.hazelcast.query.impl.CachedQueryEntry;
import com.hazelcast.query.impl.Index;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.query.impl.QueryEntry;
import com.hazelcast.query.impl.getters.Extractors;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

/**
 * Default implementation of {@link QueryCacheRecordStore}.
 *
 * @see QueryCacheRecordStore
 */
class DefaultQueryCacheRecordStore implements QueryCacheRecordStore {

    private static final int DEFAULT_CACHE_CAPACITY = 1000;

    private final Indexes indexes;
    private final QueryCacheRecordHashMap cache;
    private final EvictionOperator evictionOperator;
    private final QueryCacheRecordFactory recordFactory;
    private final InternalSerializationService ss;
    private final Extractors extractors;
    private final int maxCapacity;
    private final boolean serializeKeys;

    DefaultQueryCacheRecordStore(InternalSerializationService ss,
                                 Indexes indexes,
                                 QueryCacheConfig config, EvictionListener listener,
                                 Extractors extractors) {
        this.cache = new QueryCacheRecordHashMap(ss, DEFAULT_CACHE_CAPACITY);
        this.ss = ss;
        this.recordFactory = getRecordFactory(config.getInMemoryFormat());
        this.indexes = indexes;
        this.evictionOperator = new EvictionOperator(cache, config, listener, ss.getClassLoader());
        this.extractors = extractors;
        EvictionConfig evictionConfig = config.getEvictionConfig();
        MaxSizePolicy maximumSizePolicy = evictionConfig.getMaxSizePolicy();
        this.maxCapacity = maximumSizePolicy == MaxSizePolicy.ENTRY_COUNT
                ? evictionConfig.getSize()
                : Integer.MAX_VALUE;
        this.serializeKeys = config.isSerializeKeys();
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
                return new DataQueryCacheRecordFactory(ss);
            case OBJECT:
                return new ObjectQueryCacheRecordFactory(ss);
            default:
                throw new IllegalArgumentException("Not a known format [" + inMemoryFormat + "]");
        }
    }

    @Override
    public QueryCacheRecord add(Object queryCacheKey, Data valueData) {
        evictionOperator.evictIfRequired();

        return addWithoutEvictionCheck(queryCacheKey, valueData);
    }

    @Override
    public QueryCacheRecord addWithoutEvictionCheck(Object queryCacheKey, Data valueData) {
        QueryCacheRecord newRecord = recordFactory.createRecord(valueData);
        QueryCacheRecord oldRecord = cache.put(queryCacheKey, newRecord);
        saveIndex(queryCacheKey, newRecord, oldRecord);

        return oldRecord;
    }

    @Override
    public void addBatch(Iterator<Map.Entry<Data, Data>> entryIterator,
                         BiConsumer<Map.Entry<Data, Data>, QueryCacheRecord> postProcessor) {
        CachedQueryEntry newEntry = new CachedQueryEntry(ss, extractors);
        CachedQueryEntry oldEntry = new CachedQueryEntry(ss, extractors);
        while (entryIterator.hasNext()) {
            if (cache.size() == maxCapacity) {
                break;
            }
            Map.Entry<Data, Data> entry = entryIterator.next();
            QueryCacheRecord oldRecord = addWithoutEvictionCheck(entry.getKey(), entry.getValue(), newEntry, oldEntry);
            postProcessor.accept(entry, oldRecord);
        }
    }

    /**
     * Similar to {@link #addWithoutEvictionCheck} with explicit
     * {@link CachedQueryEntry} arguments, to be reused when saving to index.
     * Suitable for usage with {@link #addBatch(Iterator, BiConsumer)}.
     */
    public QueryCacheRecord addWithoutEvictionCheck(Data keyData, Data valueData,
                                                    CachedQueryEntry newEntry, CachedQueryEntry oldEntry) {
        QueryCacheRecord newRecord = recordFactory.createRecord(valueData);
        QueryCacheRecord oldRecord = cache.put(toQueryCacheKey(keyData), newRecord);
        saveIndex(keyData, newRecord, oldRecord, newEntry, oldEntry);

        return oldRecord;
    }

    public Object toQueryCacheKey(Object key) {
        return serializeKeys ? ss.toData(key) : ss.toObject(key);
    }

    /**
     * Same as {@link #saveIndex}
     * with explicit {@link CachedQueryEntry} arguments for reuse, to avoid
     * excessive litter when adding several entries in batch.
     */
    private void saveIndex(Data keyData, QueryCacheRecord currentRecord, QueryCacheRecord oldRecord,
                           CachedQueryEntry newEntry, CachedQueryEntry oldEntry) {
        if (indexes.haveAtLeastOneIndex()) {
            Object currentValue = currentRecord.getValue();
            QueryEntry queryEntry = new QueryEntry(ss, keyData, currentValue, extractors);
            Object oldValue = oldRecord == null ? null : oldRecord.getValue();
            newEntry.init(keyData, currentValue);
            oldEntry.init(keyData, oldValue);
            indexes.putEntry(newEntry, oldEntry, queryEntry, Index.OperationSource.USER);
        }
    }

    private void saveIndex(Object queryCacheKey, QueryCacheRecord currentRecord, QueryCacheRecord oldRecord) {
        if (indexes.haveAtLeastOneIndex()) {
            Data keyData = ss.toData(queryCacheKey);
            Object currentValue = currentRecord.getValue();
            QueryEntry queryEntry = new QueryEntry(ss, keyData, currentValue, extractors);
            Object oldValue = oldRecord == null ? null : oldRecord.getValue();
            CachedQueryEntry newEntry = new CachedQueryEntry(ss, keyData, currentValue, extractors);
            CachedQueryEntry oldEntry = new CachedQueryEntry(ss, keyData, oldValue, extractors);
            indexes.putEntry(newEntry, oldEntry, queryEntry, Index.OperationSource.USER);
        }
    }

    @Override
    public QueryCacheRecord get(Object queryCacheKey) {
        QueryCacheRecord record = cache.get(queryCacheKey);
        return accessRecord(record);
    }

    @Override
    public QueryCacheRecord remove(Object queryCacheKey) {
        QueryCacheRecord oldRecord = cache.remove(queryCacheKey);
        if (oldRecord != null) {
            removeIndex(queryCacheKey, oldRecord.getValue());
        }
        return oldRecord;
    }

    private void removeIndex(Object queryCacheKey, Object value) {
        if (indexes.haveAtLeastOneIndex()) {
            indexes.removeEntry(ss.toData(queryCacheKey), value, Index.OperationSource.USER);
        }
    }

    @Override
    public boolean containsKey(Object queryCacheKey) {
        QueryCacheRecord record = get(queryCacheKey);
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
    public Set keySet() {
        return cache.keySet();
    }

    @Override
    public Set<Map.Entry<Object, QueryCacheRecord>> entrySet() {
        return cache.entrySet();
    }

    @Override
    public int clear() {
        int removedEntryCount = cache.size();
        cache.clear();
        indexes.clearAll();
        return removedEntryCount;
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
