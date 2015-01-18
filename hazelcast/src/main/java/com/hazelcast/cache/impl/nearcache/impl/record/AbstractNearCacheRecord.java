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

package com.hazelcast.cache.impl.nearcache.impl.record;

import com.hazelcast.cache.impl.nearcache.NearCacheRecord;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Abstract implementation of {@link NearCacheRecord} with value and
 * expiration time as internal state.
 *
 * @param <V> the type of the value stored by this {@link AbstractNearCacheRecord}
 */
public abstract class AbstractNearCacheRecord<V> implements NearCacheRecord<V> {

    protected V value;
    protected long creationTime = -1;
    protected volatile long expirationTime = -1;
    protected volatile long accessTime = -1;
    protected AtomicInteger accessHit = new AtomicInteger(0);

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
        return accessHit.get();
    }

    @Override
    public void setAccessHit(int accessHit) {
        this.accessHit.set(accessHit);
    }

    @Override
    public void incrementAccessHit() {
        accessHit.incrementAndGet();
    }

    @Override
    public void resetAccessHit() {
        accessHit.set(0);
    }

    @Override
    public boolean isExpiredAt(long now) {
        return expirationTime > -1 && expirationTime <= now;
    }

}
