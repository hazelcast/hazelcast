/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * Abstract implementation of {@link NearCacheRecord} with value and expiration time as internal state.
 *
 * @param <V> the type of the value stored by this {@link AbstractNearCacheRecord}
 */
public abstract class AbstractNearCacheRecord<V> implements NearCacheRecord<V> {

    // primitive long typed fields:
    // "creationTime", "expirationTime" and "lastAccessTime", "reservationId", "sequence"
    public static final int NUMBER_OF_LONG_FIELD_TYPES = 5;
    // primitive int typed fields: "accessHit"
    public static final int NUMBER_OF_INTEGER_FIELD_TYPES = 1;

    private static final AtomicIntegerFieldUpdater<AbstractNearCacheRecord> ACCESS_HIT =
            AtomicIntegerFieldUpdater.newUpdater(AbstractNearCacheRecord.class, "accessHit");

    private static final AtomicLongFieldUpdater<AbstractNearCacheRecord> RECORD_STATE =
            AtomicLongFieldUpdater.newUpdater(AbstractNearCacheRecord.class, "reservationId");

    protected volatile int partitionId;
    protected volatile long creationTime = TIME_NOT_SET;
    protected long sequence;
    // longs of uuid
    protected long mostSigBits;
    protected long leastSigBits;

    protected volatile V value;
    protected volatile long reservationId = NOT_RESERVED;
    protected volatile long expirationTime = TIME_NOT_SET;
    protected volatile long lastAccessTime = TIME_NOT_SET;
    protected volatile int accessHit;

    public AbstractNearCacheRecord() {
    }

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
    public long getLastAccessTime() {
        return lastAccessTime;
    }

    @Override
    public void setLastAccessTime(long lastAccessTime) {
        this.lastAccessTime = lastAccessTime;
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
    public boolean isExpiredAt(long now) {
        return (expirationTime > TIME_NOT_SET) && (expirationTime <= now);
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
    public boolean hasSameUuid(UUID thatUuid) {
        if (thatUuid == null) {
            return false;
        }
        return thatUuid.getMostSignificantBits() == mostSigBits
                && thatUuid.getLeastSignificantBits() == leastSigBits;
    }

    @Override
    public void setUuid(UUID uuid) {
        this.mostSigBits = uuid.getMostSignificantBits();
        this.leastSigBits = uuid.getLeastSignificantBits();
    }

    @Override
    public boolean isIdleAt(long maxIdleMilliSeconds, long now) {
        if (maxIdleMilliSeconds > 0) {
            if (lastAccessTime > TIME_NOT_SET) {
                return lastAccessTime + maxIdleMilliSeconds < now;
            } else {
                return creationTime + maxIdleMilliSeconds < now;
            }
        } else {
            return false;
        }
    }

    @Override
    public long getReservationId() {
        return reservationId;
    }

    @Override
    public void reserveWithId(long reservationId) {
        this.reservationId = reservationId;
    }

    @Override
    public void doReservable() {
        reserveWithId(NOT_RESERVED);
    }

    @Override
    public void setMostSignificantBits(long mostSigBits) {
        this.mostSigBits = mostSigBits;
    }

    @Override
    public void setLeastSignificantBits(long leastSigBits) {
        this.leastSigBits = leastSigBits;
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
    public String toString() {
        return "creationTime=" + creationTime
                + ", sequence=" + sequence
                + ", uuid=" + new UUID(mostSigBits, leastSigBits)
                + ", expirationTime=" + expirationTime
                + ", lastAccessTime=" + lastAccessTime
                + ", accessHit=" + accessHit
                + ", reservationId=" + reservationId
                + ", value=" + value;
    }
}
