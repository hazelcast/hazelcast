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

package com.hazelcast.cache.impl;

import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.nio.serialization.Data;

import javax.cache.configuration.Factory;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.processor.MutableEntry;

/**
 * This class is an implementation of {@link MutableEntry} which is provided into
 * {@link javax.cache.processor.EntryProcessor#process(javax.cache.processor.MutableEntry, Object...)}.
 * <p>CacheEntryProcessorEntry may face multiple mutating operations like setValue, remove or CacheLoading, etc.</p>
 * <p>This implementation may handle multiple operations executed on this entry and persist the resultant state into
 * {@link CacheRecordStore} after entry processor get completed.</p>
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

    protected final AbstractCacheRecordStore cacheRecordStore;
    protected final long now;
    protected final long start;
    protected final ExpiryPolicy expiryPolicy;

    public CacheEntryProcessorEntry(Data keyData,
                                    R record,
                                    AbstractCacheRecordStore cacheRecordStore,
                                    long now) {
        this.keyData = keyData;
        this.record = record;
        this.cacheRecordStore = cacheRecordStore;
        this.now = now;
        this.start = cacheRecordStore.cacheConfig.isStatisticsEnabled() ? System.nanoTime() : 0;

        final Factory<ExpiryPolicy> expiryPolicyFactory =
                cacheRecordStore.cacheConfig.getExpiryPolicyFactory();
        this.expiryPolicy = expiryPolicyFactory.create();
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
        if (value == null) {
            throw new NullPointerException("Null value not allowed");
        }
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
            return getRecordValue(record);
        }
        if (recordLoaded == null) {
            //LOAD IT
            recordLoaded = (R) cacheRecordStore.readThroughRecord(keyData, now);
        }
        if (recordLoaded != null) {
            state = State.LOAD;
            return getRecordValue(recordLoaded);
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

    /**
     * Provides a similar functionality as committing a transaction. So, at the end of the process method, applyChanges
     * will be called to apply latest data into {@link CacheRecordStore}.
     */
    public void applyChanges() {
        final boolean isStatisticsEnabled = cacheRecordStore.cacheConfig.isStatisticsEnabled();
        final CacheStatisticsImpl statistics = cacheRecordStore.statistics;

        switch (state) {
            case ACCESS:
                cacheRecordStore.accessRecord(record, expiryPolicy, now);
                break;
            case UPDATE:
                cacheRecordStore.updateRecordWithExpiry(keyData, value, record, expiryPolicy, now, false);
                if (isStatisticsEnabled) {
                    statistics.increaseCachePuts(1);
                    statistics.addGetTimeNanos(System.nanoTime() - start);
                }
                break;
            case REMOVE:
                cacheRecordStore.remove(keyData, null);
                break;
            case CREATE:
                if (isStatisticsEnabled) {
                    statistics.increaseCachePuts(1);
                    statistics.addGetTimeNanos(System.nanoTime() - start);
                }
                cacheRecordStore.createRecordWithExpiry(keyData, value, expiryPolicy, now, false);
                break;
            case LOAD:
                cacheRecordStore.createRecordWithExpiry(keyData, value, expiryPolicy, now, true);
                break;
            case NONE:
                //NOOP
                break;
            default:
                break;
        }
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
