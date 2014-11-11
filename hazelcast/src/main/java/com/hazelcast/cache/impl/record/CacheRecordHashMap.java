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
import com.hazelcast.util.EmptyStatement;
import com.hazelcast.util.FetchableConcurrentHashMap;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

public class CacheRecordHashMap<K, V>
        extends FetchableConcurrentHashMap<K, V>
        implements CacheRecordMap<K, V> {

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

    @Override
    public int evictExpiredRecords(int percentage) {
        long now = Clock.currentTimeMillis();
        int sizeLimitForEviction = (int) ((double) (size() * percentage)
                / (double) ICacheRecordStore.ONE_HUNDRED_PERCENT);

        if (sizeLimitForEviction < MIN_EVICTION_ELEMENT_COUNT) {
            return 0;
        }

        List<Map.Entry<K, Expirable>> expiredEntries =
                new ArrayList<Map.Entry<K, Expirable>>(sizeLimitForEviction);
        int expiredCount = 0;
        for (Map.Entry<K, V> entry : entrySet()) {
            K key = entry.getKey();
            V value = entry.getValue();
            final boolean isExpired =
                    (value instanceof Expirable)
                            && ((Expirable) value).isExpiredAt(now);
            if (isExpired) {
                expiredEntries.add((Map.Entry<K, Expirable>) entry);
                if (++expiredCount >= sizeLimitForEviction) {
                    break;
                }
            }
        }
        int actualExpiredCount = 0;
        for (Map.Entry<K, Expirable> entry : expiredEntries) {
            Expirable expirableValue = entry.getValue();
            if (expirableValue instanceof Data) {
                callbackEvictionListeners((Data) expirableValue);
            }
            if (remove(entry.getKey()) != null) {
                actualExpiredCount++;
            }
        }
        return actualExpiredCount;
    }

    @Override
    public int evictRecords(int percentage, EvictionPolicy policy) {
        switch (policy) {
            case RANDOM:
                try {
                    return evictRecordsRandom(percentage);
                } catch (Throwable e) {
                    EmptyStatement.ignore(e);
                    break;
                }

            case LRU:
                try {
                    return evictRecordsLRU(percentage);
                } catch (Throwable e) {
                    EmptyStatement.ignore(e);
                    break;
                }

            case LFU:
                try {
                    return evictRecordsLFU(percentage);
                } catch (Throwable e) {
                    EmptyStatement.ignore(e);
                    break;
                }

            default:
                throw new IllegalArgumentException();
        }

        return evictExpiredRecords(percentage);
    }

    private int evictRecordsLRU(int percentage) {
        throw new UnsupportedOperationException(
                "\"LRU\" eviction is not supported right now !");
    }

    private int evictRecordsLFU(int percentage) {
        throw new UnsupportedOperationException(
                "\"LFU\" eviction is not supported right now !");
    }

    private int evictRecordsRandom(int percentage) {
        int sizeLimitForEviction = (int) ((double) (size() * percentage)
                / (double) ICacheRecordStore.ONE_HUNDRED_PERCENT);

        if (sizeLimitForEviction < MIN_EVICTION_ELEMENT_COUNT) {
            return 0;
        }

        List<Map.Entry<K, V>> expiredEntries =
                new ArrayList<Map.Entry<K, V>>(sizeLimitForEviction);
        int expiredCount = 0;
        for (Map.Entry<K, V> entry : entrySet()) {
            expiredEntries.add(entry);
            if (++expiredCount >= sizeLimitForEviction) {
                break;
            }
        }
        int actualExpiredCount = 0;
        for (Map.Entry<K, V> entry : expiredEntries) {
            V value = entry.getValue();
            if (value instanceof Data) {
                callbackEvictionListeners((Data) value);
            }
            if (remove(entry.getKey()) != null) {
                actualExpiredCount++;
            }
        }
        return actualExpiredCount;
    }

}
