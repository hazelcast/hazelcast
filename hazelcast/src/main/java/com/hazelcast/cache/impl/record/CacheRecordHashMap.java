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

    public CacheRecordHashMap(int initialCapacity) {
        super(initialCapacity);
    }

    public CacheRecordHashMap(int initialCapacity,
                              float loadFactor,
                              int concurrencyLevel,
                              ConcurrentReferenceHashMap.ReferenceType keyType,
                              ConcurrentReferenceHashMap.ReferenceType valueType,
                              EnumSet<Option> options) {
        this(initialCapacity, loadFactor, concurrencyLevel, keyType, valueType, options, null);
    }

    public CacheRecordHashMap(int initialCapacity,
                              float loadFactor,
                              int concurrencyLevel,
                              ConcurrentReferenceHashMap.ReferenceType keyType,
                              ConcurrentReferenceHashMap.ReferenceType valueType,
                              EnumSet<ConcurrentReferenceHashMap.Option> options,
                              Callback<Data> evictionCallback) {
        super(initialCapacity, loadFactor, concurrencyLevel, keyType, valueType, options);
        this.evictionCallback = evictionCallback;
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

        return actualEvictedCount;
    }
    //CHECKSTYLE:ON

    @Override
    public int evictRecords(int percentage, EvictionPolicy policy) {
        switch (policy) {
            case RANDOM:
                return evictRecordsRandom(percentage);
            case LRU:
                return evictRecordsLRU(percentage);
            case LFU:
                return evictRecordsLFU(percentage);
            default:
                throw new IllegalArgumentException("Unsupported eviction policy: " + policy);
        }
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
