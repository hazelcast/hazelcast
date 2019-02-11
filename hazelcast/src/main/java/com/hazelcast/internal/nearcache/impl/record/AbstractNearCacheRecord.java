/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.nearcache.impl.record;

import com.hazelcast.internal.nearcache.NearCacheRecord;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static java.util.concurrent.atomic.AtomicIntegerFieldUpdater.newUpdater;
import static java.util.concurrent.atomic.AtomicReferenceFieldUpdater.newUpdater;

/**
 * Abstract implementation of {@link NearCacheRecord} with value and expiration time as internal state.
 *
 * @param <V> the type of the value stored by this {@link AbstractNearCacheRecord}
 */
public abstract class AbstractNearCacheRecord<V> implements NearCacheRecord<V> {

    // "creationTime", "expirationTime" and "accessTime", "sequence"
    public static final int NUMBER_OF_LONG_FIELD_TYPES = 4;
    // "accessHit", "partitionId"
    public static final int NUMBER_OF_INTEGER_FIELD_TYPES = 2;
    private static final AtomicIntegerFieldUpdater<AbstractNearCacheRecord> ACCESS_HIT =
            newUpdater(AbstractNearCacheRecord.class, "accessHit");

    private static final AtomicReferenceFieldUpdater<AbstractNearCacheRecord, Object> STATE =
            newUpdater(AbstractNearCacheRecord.class, Object.class, "state");

    protected long creationTime;

    protected volatile int partitionId;
    protected volatile int accessHit;
    protected volatile long sequence;
    protected volatile long expirationTime;
    protected volatile long accessTime = TIME_NOT_SET;
    protected volatile UUID uuid;
    protected volatile Object state;

    public AbstractNearCacheRecord(V value, long creationTime, long expirationTime) {
        this.state = value;
        this.creationTime = creationTime;
        this.expirationTime = expirationTime;
    }

    @Override
    public void setState(Object newState) {
        state = newState;
    }

    @Override
    public void casState(Object expect, Object update) {
        STATE.compareAndSet(this, expect, update);
    }

    @Override
    public Object getState() {
        return state;
    }

    @Override
    public V getValue() {
        return ((V) state);
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
    public long getLastAccessTime() {
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

    @Override
    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public void setPartitionId(int partitionId) {
        this.partitionId = partitionId;
    }

    @Override
    public long getInvalidationSequence() {
        return sequence;
    }

    @Override
    public void setInvalidationSequence(long sequence) {
        this.sequence = sequence;
    }

    @Override
    public void setUuid(UUID uuid) {
        this.uuid = uuid;
    }

    @Override
    public boolean hasSameUuid(UUID thatUuid) {
        return uuid != null && uuid.equals(thatUuid);
    }

    @Override
    public String toString() {
        return "creationTime=" + creationTime
                + ", sequence=" + sequence
                + ", uuid=" + uuid
                + ", expirationTime=" + expirationTime
                + ", accessTime=" + accessTime
                + ", accessHit=" + accessHit
                + ", value=" + state;
    }
}
