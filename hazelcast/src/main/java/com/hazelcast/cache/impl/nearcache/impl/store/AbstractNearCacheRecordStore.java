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

package com.hazelcast.cache.impl.nearcache.impl.store;

import com.hazelcast.cache.impl.nearcache.NearCacheContext;
import com.hazelcast.cache.impl.nearcache.NearCacheRecord;
import com.hazelcast.cache.impl.nearcache.NearCacheRecordStore;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.monitor.impl.NearCacheStatsImpl;
import com.hazelcast.nio.UnsafeHelper;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ExceptionUtil;

public abstract class AbstractNearCacheRecordStore<K, V, R extends NearCacheRecord>
        implements NearCacheRecordStore<K, V> {

    /*
     * If Unsafe is available, Object array index scale (every index represents a reference)
     * can be assumed as reference size.
     *
     * Otherwise, we assume reference size as integer size that means
     * we assume 32 bit JVM or compressed-references enabled 64 bit JVM
     * by ignoring compressed-references disable mode on 64 bit JVM.
     */
    protected static final int REFERENCE_SIZE =
            UnsafeHelper.UNSAFE_AVAILABLE
                    ? UnsafeHelper.UNSAFE.arrayIndexScale(Object[].class)
                    : (Integer.SIZE / Byte.SIZE);

    private static final int MILLI_SECONDS_IN_A_SECOND = 1000;

    protected final long timeToLiveMillis;
    protected final long maxIdleMillis;

    protected final NearCacheConfig nearCacheConfig;
    protected final SerializationService serializationService;
    protected final NearCacheStatsImpl nearCacheStats = new NearCacheStatsImpl();

    public AbstractNearCacheRecordStore(NearCacheConfig nearCacheConfig, NearCacheContext nearCacheContext) {
        this.nearCacheConfig = nearCacheConfig;
        this.timeToLiveMillis = nearCacheConfig.getTimeToLiveSeconds() * MILLI_SECONDS_IN_A_SECOND;
        this.maxIdleMillis = nearCacheConfig.getMaxIdleSeconds() * MILLI_SECONDS_IN_A_SECOND;
        this.serializationService = nearCacheContext.getSerializationService();
    }

    protected abstract boolean isAvailable();

    protected abstract long getKeyStorageMemoryCost(K key);
    protected abstract long getRecordStorageMemoryCost(R record);

    protected abstract R valueToRecord(V value);
    protected abstract V recordToValue(R record);

    protected abstract R getRecord(K key);
    protected abstract R putRecord(K key, R record);
    protected abstract R removeRecord(K key);
    protected abstract void clearRecords();
    protected abstract void destroyStore();

    protected void checkAvailable() {
        if (!isAvailable()) {
            throw new IllegalStateException(nearCacheConfig.getName()
                    + " named near cache record store is not available");
        }
    }

    protected Data valueToData(V value) {
        if (value instanceof Data) {
            return (Data) value;
        } else if (value != null) {
            return serializationService.toData(value);
        } else {
            return null;
        }
    }

    protected V dataToValue(Data data) {
        if (data != null) {
            return serializationService.toObject(data);
        } else {
            return null;
        }
    }

    protected Data toData(Object obj) {
        if (obj == null) {
            return null;
        } else if (obj instanceof Data) {
            return (Data) obj;
        } else {
            return valueToData((V) obj);
        }
    }

    protected V toValue(Object obj) {
        if (obj == null) {
            return null;
        } else if (obj instanceof Data) {
            return dataToValue((Data) obj);
        } else {
            return (V) obj;
        }
    }

    protected long getTotalStorageMemoryCost(K key, R record) {
        return getKeyStorageMemoryCost(key) + getRecordStorageMemoryCost(record);
    }

    protected boolean isRecordExpired(R record) {
        long now = Clock.currentTimeMillis();
        if (record.isExpiredAt(now)) {
            return true;
        } else {
            return record.isIdleAt(maxIdleMillis, now);
        }
    }

    protected void onRecordCreate(R record) {
        record.setCreationTime(Clock.currentTimeMillis());
    }

    protected void onRecordAccess(R record) {
        record.setAccessTime(Clock.currentTimeMillis());
        record.incrementAccessHit();
    }

    protected void onGet(K key, V value, R record) {

    }

    protected void onGetError(K key, V value, R record, Throwable error) {

    }

    protected void onPut(K key, V value, R record, boolean newPut) {

    }

    protected void onPutError(K key, V value, R record, boolean newPut, Throwable error) {

    }

    protected void onRemove(K key, R record, boolean removed) {

    }

    protected void onRemoveError(K key, R record, boolean removed, Throwable error) {

    }

    @Override
    public V get(K key) {
        checkAvailable();

        R record = null;
        V value = null;
        try {
            record = getRecord(key);
            if (record != null) {
                if (isRecordExpired(record)) {
                    remove(key);
                    return null;
                }
                onRecordAccess(record);
                nearCacheStats.incrementHits();
                value = recordToValue(record);
                onGet(key, value, record);
                return value;
            } else {
                nearCacheStats.incrementMisses();
                return null;
            }
        } catch (Throwable error) {
            onGetError(key, value, record, error);
            throw ExceptionUtil.rethrow(error);
        }
    }

    @Override
    public void put(K key, V value) {
        checkAvailable();

        R record = null;
        boolean newPut = false;
        try {
            record = getRecord(key);
            if (record == null) {
                newPut = true;
                record = valueToRecord(value);
                onRecordCreate(record);
                putRecord(key, record);
                nearCacheStats.incrementOwnedEntryCount();
                nearCacheStats.incrementOwnedEntryMemoryCost(getTotalStorageMemoryCost(key, record));
            } else {
                long oldRecordMemoryCost = getRecordStorageMemoryCost(record);
                record.setValue(value);
                long newRecordMemoryCost = getRecordStorageMemoryCost(record);
                nearCacheStats.incrementOwnedEntryMemoryCost(newRecordMemoryCost - oldRecordMemoryCost);
            }
            onPut(key, value, record, newPut);
        } catch (Throwable error) {
            onPutError(key, value, record, newPut, error);
            throw ExceptionUtil.rethrow(error);
        }
    }

    @Override
    public boolean remove(K key) {
        checkAvailable();

        R record = null;
        boolean removed = false;
        try {
            record = removeRecord(key);
            if (record != null) {
                removed = true;
                nearCacheStats.decrementOwnedEntryCount();
                nearCacheStats.decrementOwnedEntryMemoryCost(getTotalStorageMemoryCost(key, record));
            }
            onRemove(key, record, removed);
            return record != null;
        } catch (Throwable error) {
            onRemoveError(key, record, removed, error);
            throw ExceptionUtil.rethrow(error);
        }
    }

    @Override
    public void clear() {
        checkAvailable();

        clearRecords();
        nearCacheStats.setOwnedEntryCount(0);
        nearCacheStats.setOwnedEntryMemoryCost(0L);
    }

    @Override
    public void destroy() {
        checkAvailable();

        destroyStore();
        nearCacheStats.setOwnedEntryCount(0);
        nearCacheStats.setOwnedEntryMemoryCost(0L);
    }

    @Override
    public NearCacheStats getNearCacheStats() {
        checkAvailable();

        return nearCacheStats;
    }

}
