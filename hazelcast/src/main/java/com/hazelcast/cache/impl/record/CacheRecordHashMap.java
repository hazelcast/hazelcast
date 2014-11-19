/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache.impl.record;

import com.hazelcast.cache.impl.CacheInfo;
import com.hazelcast.cache.impl.CacheKeyIteratorResult;
import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Callback;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ConcurrentReferenceHashMap;
import com.hazelcast.util.FetchableConcurrentHashMap;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

public class CacheRecordHashMap
        extends FetchableConcurrentHashMap<Data, CacheRecord>
        implements CacheRecordMap<Data, CacheRecord> {

    private static final int MIN_EVICTION_ELEMENT_COUNT = 100;

    private Callback<Data> evictionCallback;
    private CacheInfo cacheInfo;

    public CacheRecordHashMap(int initialCapacity, CacheInfo cacheInfo) {
        super(initialCapacity);
        this.cacheInfo = cacheInfo;
    }

    public CacheRecordHashMap(int initialCapacity,
                              float loadFactor,
                              int concurrencyLevel,
                              ConcurrentReferenceHashMap.ReferenceType keyType,
                              ConcurrentReferenceHashMap.ReferenceType valueType,
                              EnumSet<Option> options,
                              CacheInfo cacheInfo) {
        this(initialCapacity, loadFactor, concurrencyLevel, keyType, valueType, options, null, cacheInfo);
    }

    public CacheRecordHashMap(int initialCapacity,
                              float loadFactor,
                              int concurrencyLevel,
                              ConcurrentReferenceHashMap.ReferenceType keyType,
                              ConcurrentReferenceHashMap.ReferenceType valueType,
                              EnumSet<ConcurrentReferenceHashMap.Option> options,
                              Callback<Data> evictionCallback,
                              CacheInfo cacheInfo) {
        super(initialCapacity, loadFactor, concurrencyLevel, keyType, valueType, options);
        this.evictionCallback = evictionCallback;
        this.cacheInfo = cacheInfo;
    }

    @Override
    public CacheRecord put(Data key, CacheRecord value) {
        CacheRecord record = super.put(key, value);
        // If there is no previous value with specified key, means that new entry is added
        if (record == null) {
            cacheInfo.increaseEntryCount();
        }
        return record;
    }

    @Override
    public CacheRecord putIfAbsent(Data key, CacheRecord value) {
        CacheRecord record = super.putIfAbsent(key, value);
        // If there is no previous value with specified key, means that new entry is added
        if (record == null) {
            cacheInfo.increaseEntryCount();
        }
        return record;
    }

    @Override
    public CacheRecord remove(Object key) {
        CacheRecord record = super.remove(key);
        if (record != null) {
            cacheInfo.decreaseEntryCount();
        }
        return record;
    }

    @Override
    public boolean remove(Object key, Object value) {
        boolean removed = super.remove(key, value);
        if (removed) {
            cacheInfo.decreaseEntryCount();
        }
        return removed;
    }

    @Override
    public void clear() {
        final int sizeBeforeClear = size();
        super.clear();
        cacheInfo.removeEntryCount(sizeBeforeClear);
    }

    @Override
    public CacheKeyIteratorResult fetchNext(int nextTableIndex, int size) {
        List<Data> keys = new ArrayList<Data>();
        int tableIndex = fetch(nextTableIndex, size, keys);
        return new CacheKeyIteratorResult(keys, tableIndex);
    }

    private void callbackEvictionListeners(Data data) {
        if (evictionCallback != null) {
            evictionCallback.notify(data);
        }
    }

    //CHECKSTYLE:OFF
    @Override
    public int evictExpiredRecords(int percentage) {
        if (percentage <= 0) {
            return 0;
        }
        final int size = size();
        if (percentage >= ICacheRecordStore.ONE_HUNDRED_PERCENT || size <= MIN_EVICTION_ELEMENT_COUNT) {
            clear();
            return size;
        }

        long now = Clock.currentTimeMillis();
        int sizeLimitForEviction = (int) ((double) (size * percentage)
                / (double) ICacheRecordStore.ONE_HUNDRED_PERCENT);
        // TODO Maybe instead of creating new list for every evict operation,
        // thread local based reusable list can be used
        // or maybe eviction can be done without a helper list to hold entries will be evicted
        List<Map.Entry<Data, CacheRecord>> entriesWillBeEvicted =
                new ArrayList<Map.Entry<Data, CacheRecord>>(sizeLimitForEviction);

        int i = 0;
        for (Map.Entry<Data, CacheRecord> entry : entrySet()) {
            CacheRecord record = entry.getValue();
            final boolean isExpired = record.isExpiredAt(now);
            if (isExpired) {
                entriesWillBeEvicted.add(entry);
                if (++i >= sizeLimitForEviction) {
                    break;
                }
            }
        }

        int actualEvictedCount = 0;
        for (Map.Entry<Data, CacheRecord> entry : entriesWillBeEvicted) {
            CacheRecord record = entry.getValue();
            Object value = record.getValue();
            if (value instanceof Data) {
                callbackEvictionListeners((Data) value);
            }
            if (remove(entry.getKey()) != null) {
                actualEvictedCount++;
            }
        }

        cacheInfo.removeEntryCount(actualEvictedCount);

        return actualEvictedCount;
    }
    //CHECKSTYLE:ON

    @Override
    public int evictRecords(int percentage, EvictionPolicy policy) {
        int evictedCount;

        switch (policy) {
            case RANDOM:
                evictedCount = evictRecordsRandom(percentage);
                break;
            case LRU:
                evictedCount = evictRecordsLRU(percentage);
                break;
            case LFU:
                evictedCount = evictRecordsLFU(percentage);
                break;
            default:
                throw new IllegalArgumentException("Unsupported eviction policy: " + policy);
        }

        cacheInfo.removeEntryCount(evictedCount);

        return evictedCount;
    }

    //CHECKSTYLE:OFF
    private int evictRecordsRandom(int percentage) {
        if (percentage <= 0) {
            return 0;
        }
        final int size = size();
        if (percentage >= ICacheRecordStore.ONE_HUNDRED_PERCENT || size <= MIN_EVICTION_ELEMENT_COUNT) {
            clear();
            return size;
        }

        int sizeLimitForEviction = (int) ((double) (size() * percentage)
                / (double) ICacheRecordStore.ONE_HUNDRED_PERCENT);
        // TODO Maybe instead of creating new list for every evict operation,
        // thread local based reusable list can be used
        // or maybe eviction can be done without a helper list to hold entries will be evicted
        List<Map.Entry<Data, CacheRecord>> entriesWillBeEvicted =
                new ArrayList<Map.Entry<Data, CacheRecord>>(sizeLimitForEviction);

        int i = 0;
        for (Map.Entry<Data, CacheRecord> entry : entrySet()) {
            entriesWillBeEvicted.add(entry);
            if (++i >= sizeLimitForEviction) {
                break;
            }
        }

        int actualEvictedCount = 0;
        for (Map.Entry<Data, CacheRecord> entry : entriesWillBeEvicted) {
            CacheRecord record = entry.getValue();
            Object value = record.getValue();
            if (value instanceof Data) {
                callbackEvictionListeners((Data) value);
            }
            if (remove(entry.getKey()) != null) {
                actualEvictedCount++;
            }
        }
        return actualEvictedCount;
    }
    //CHECKSTYLE:ON

    private int evictRecordsLRU(int percentage) {
        throw new UnsupportedOperationException("LRU eviction policy is not supported right now !");
    }

    private int evictRecordsLFU(int percentage) {
        throw new UnsupportedOperationException("LFU eviction policy is not supported right now !");
    }

}
