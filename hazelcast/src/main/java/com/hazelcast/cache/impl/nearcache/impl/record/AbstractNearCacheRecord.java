/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache.impl.nearcache.impl.record;

import com.hazelcast.cache.impl.nearcache.NearCacheRecord;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * Abstract implementation of {@link NearCacheRecord} with value and
 * expiration time as internal state.
 *
 * @param <V> the type of the value stored by this {@link AbstractNearCacheRecord}
 */
public abstract class AbstractNearCacheRecord<V> implements NearCacheRecord<V> {

    private static final AtomicIntegerFieldUpdater<AbstractNearCacheRecord> ACCESS_HIT =
            AtomicIntegerFieldUpdater.newUpdater(AbstractNearCacheRecord.class, "accessHit");

    protected V value;
    protected long creationTime = TIME_NOT_SET;
    protected volatile long expirationTime = TIME_NOT_SET;
    protected volatile long accessTime = TIME_NOT_SET;
    protected volatile int accessHit;

    public AbstractNearCacheRecord(V value, long creationTime, long expirationTime) {
        this.value = value;
        this.creationTime = creationTime;
        this.expirationTime = expirationTime;
    }

    @Override
    public V getValue() {
        return value;
    }

    @Override
    public void setValue(V value) {
        this.value = value;
    }

    @Override
    public long getExpirationTime() {
        return expirationTime;
    }

    @Override
    public void setExpirationTime(long expirationTime) {
        this.expirationTime = expirationTime;
    }

    @Override
    public long getCreationTime() {
        return creationTime;
    }

    @Override
    public void setCreationTime(long creationTime) {
        this.creationTime = creationTime;
    }

    @Override
    public long getAccessTime() {
        return accessTime;
    }

    @Override
    public void setAccessTime(long accessTime) {
        this.accessTime = accessTime;
    }

    @Override
    public int getAccessHit() {
        return accessHit;
    }

    @Override
    public void setAccessHit(int accessHit) {
        ACCESS_HIT.set(this, accessHit);
    }

    @Override
    public void incrementAccessHit() {
        ACCESS_HIT.addAndGet(this, 1);
    }

    @Override
    public void resetAccessHit() {
        ACCESS_HIT.set(this, 0);
    }

    @Override
    public boolean isExpiredAt(long now) {
        return (expirationTime > TIME_NOT_SET) && (expirationTime <= now);
    }

    @Override
    public boolean isIdleAt(long maxIdleMilliSeconds, long now) {
        if (maxIdleMilliSeconds > 0) {
            if (accessTime > TIME_NOT_SET) {
                return accessTime + maxIdleMilliSeconds < now;
            } else {
                return creationTime + maxIdleMilliSeconds < now;
            }
        } else {
            return false;
        }
    }

}
