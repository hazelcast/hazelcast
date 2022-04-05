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

package com.hazelcast.cache.impl;

import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.Timer;
import com.hazelcast.spi.impl.operationexecutor.impl.PartitionOperationThread;

import javax.cache.expiry.ExpiryPolicy;
import javax.cache.processor.MutableEntry;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * This class is an implementation of {@link MutableEntry} which is provided into
 * {@link javax.cache.processor.EntryProcessor#process(javax.cache.processor.MutableEntry, Object...)}.
 * <p>CacheEntryProcessorEntry may face multiple mutating operations like setValue, remove or CacheLoading, etc.</p>
 * <p>This implementation may handle multiple operations executed on this entry and persist the resultant state into
 * {@link CacheRecordStore} after entry processor get completed.</p>
 *
 * @param <K> the type of key.
 * @param <V> the type of value.
 * @see javax.cache.processor.EntryProcessor#process(javax.cache.processor.MutableEntry, Object...)
 */
public class CacheEntryProcessorEntry<K, V, R extends CacheRecord>
        implements MutableEntry<K, V> {

    protected K key;
    protected V value;

    protected State state = State.NONE;

    protected final Data keyData;
    protected R record;
    protected R recordLoaded;
    protected V valueLoaded;

    protected final AbstractCacheRecordStore cacheRecordStore;
    protected final long now;
    protected final long startNanos;
    protected final ExpiryPolicy expiryPolicy;
    protected final int completionId;

    public CacheEntryProcessorEntry(Data keyData, R record,
                                    AbstractCacheRecordStore cacheRecordStore,
                                    long now, int completionId) {
        this.keyData = keyData;
        this.record = record;
        this.cacheRecordStore = cacheRecordStore;
        this.now = now;
        this.completionId = completionId;
        this.startNanos = cacheRecordStore.cacheConfig.isStatisticsEnabled() ? Timer.nanos() : 0;

        this.expiryPolicy = cacheRecordStore.getExpiryPolicy(record, null);
    }

    @Override
    public boolean exists() {
        return (record != null && state == State.NONE) || this.value != null;
    }

    @Override
    public void remove() {
        this.value = null;
        this.state = (this.state == State.CREATE || this.state == State.LOAD) ? State.NONE : State.REMOVE;
    }

    @Override
    public void setValue(V value) {
        checkNotNull(value, "Null value not allowed");

        if (this.record == null) {
            this.state = State.CREATE;
        } else {
            this.state = State.UPDATE;
        }
        this.value = value;
    }

    @Override
    public K getKey() {
        if (key == null) {
            key = (K) cacheRecordStore.cacheService.toObject(keyData);
        }
        return key;
    }

    @Override
    public V getValue() {
        if (state == State.REMOVE) {
            return null;
        }
        if (value != null) {
            return value;
        }
        if (record != null) {
            state = State.ACCESS;
            value = getRecordValue(record);
            return value;
        }
        if (recordLoaded == null) {
            //LOAD IT
            recordLoaded = (R) cacheRecordStore.readThroughRecord(keyData, now);
        }
        if (recordLoaded != null) {
            state = State.LOAD;
            valueLoaded = getRecordValue(recordLoaded);
            return valueLoaded;
        }
        return null;
    }

    protected V getRecordValue(R record) {
        final Object objValue;
        switch (cacheRecordStore.cacheConfig.getInMemoryFormat()) {
            case BINARY:
                objValue = cacheRecordStore.cacheService.toObject(record.getValue());
                break;
            case OBJECT:
                objValue = record.getValue();
                break;
            default:
                throw new IllegalArgumentException("Invalid storage format: "
                        + cacheRecordStore.cacheConfig.getInMemoryFormat());
        }
        return (V) objValue;
    }

    public R getRecord() {
        assert (Thread.currentThread() instanceof PartitionOperationThread);
        return record;
    }

    /**
     * Provides a similar functionality as committing a transaction. So, at the end of the process method, applyChanges
     * will be called to apply latest data into {@link CacheRecordStore}.
     */
    public void applyChanges() {
        final boolean isStatisticsEnabled = cacheRecordStore.cacheConfig.isStatisticsEnabled();
        final CacheStatisticsImpl statistics = cacheRecordStore.statistics;

        switch (state) {
            case ACCESS:
                cacheRecordStore.accessRecord(keyData, record, expiryPolicy, now);
                break;
            case CREATE:
                if (isStatisticsEnabled) {
                    statistics.increaseCachePuts(1);
                    statistics.addGetTimeNanos(Timer.nanosElapsed(startNanos));
                }
                boolean saved =
                        cacheRecordStore.createRecordWithExpiry(keyData, value, expiryPolicy,
                                now, false, completionId) != null;
                onCreate(keyData, value, expiryPolicy, now, false, completionId, saved);
                break;
            case LOAD:
                saved = cacheRecordStore.createRecordWithExpiry(keyData, valueLoaded, expiryPolicy,
                        now, true, completionId) != null;
                onLoad(keyData, valueLoaded, expiryPolicy, now, true, completionId, saved);
                break;
            case UPDATE:
                saved = cacheRecordStore.updateRecordWithExpiry(keyData, value, record,
                        expiryPolicy, now, false, completionId);
                onUpdate(keyData, value, record, expiryPolicy, now, false, completionId, saved);
                if (isStatisticsEnabled) {
                    statistics.increaseCachePuts(1);
                    statistics.addGetTimeNanos(Timer.nanosElapsed(startNanos));
                }
                break;
            case REMOVE:
                boolean removed = cacheRecordStore.remove(keyData, null, null, completionId);
                onRemove(keyData, null, completionId, removed);
                break;
            case NONE:
                cacheRecordStore.publishEvent(
                        CacheEventContextUtil.createCacheCompleteEvent(
                                cacheRecordStore.toEventData(keyData),
                                completionId));
                break;
            default:
                break;
        }
    }

    protected void onCreate(Data key, Object value, ExpiryPolicy expiryPolicy,
                            long now, boolean disableWriteThrough, int completionId, boolean saved) {
    }

    protected void onLoad(Data key, Object value, ExpiryPolicy expiryPolicy,
                          long now, boolean disableWriteThrough, int completionId, boolean saved) {
    }

    protected void onUpdate(Data key, Object value, R record, ExpiryPolicy expiryPolicy,
                            long now, boolean disableWriteThrough, int completionId, boolean saved) {
    }

    protected void onRemove(Data key, String source, int completionId, boolean removed) {
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        if (clazz.isAssignableFrom(((Object) this).getClass())) {
            return clazz.cast(this);
        }
        throw new IllegalArgumentException("Unwrapping to " + clazz + " is not supported by this implementation");
    }

    protected enum State {

        NONE, ACCESS, UPDATE, LOAD, CREATE, REMOVE

    }

}
