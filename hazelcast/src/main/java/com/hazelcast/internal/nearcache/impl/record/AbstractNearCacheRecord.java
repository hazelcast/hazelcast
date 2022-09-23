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

package com.hazelcast.internal.nearcache.impl.record;

import com.hazelcast.internal.nearcache.NearCacheRecord;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static com.hazelcast.internal.util.TimeStripUtil.recomputeWithBaseTime;
import static com.hazelcast.internal.util.TimeStripUtil.stripBaseTime;

/**
 * Abstract implementation of {@link NearCacheRecord}
 * with value and expiration time as internal state.
 *
 * @param <V> the type of the value stored
 *            by this {@link AbstractNearCacheRecord}
 */
public abstract class AbstractNearCacheRecord<V> implements NearCacheRecord<V> {
    // primitive long typed fields: "reservationId", "sequence"
    public static final int NUMBER_OF_LONG_FIELD_TYPES = 2;
    // primitive int typed fields: "partitionId", "hits",
    // "lastAccessTime","expirationTime" and "creationTime"
    public static final int NUMBER_OF_INTEGER_FIELD_TYPES = 5;
    // primitive boolean typed field: "cachedAsNull"
    public static final int NUMBER_OF_BOOLEAN_FIELD_TYPES = 1;

    private static final AtomicIntegerFieldUpdater<AbstractNearCacheRecord> HITS =
            AtomicIntegerFieldUpdater.newUpdater(AbstractNearCacheRecord.class, "hits");
    private static final AtomicLongFieldUpdater<AbstractNearCacheRecord> RECORD_STATE =
            AtomicLongFieldUpdater.newUpdater(AbstractNearCacheRecord.class, "reservationId");

    protected int creationTime;

    protected volatile V value;
    protected volatile UUID uuid;
    protected volatile boolean cachedAsNull;
    protected volatile int hits;
    protected volatile int partitionId;
    protected volatile int lastAccessTime = TIME_NOT_SET;
    protected volatile int expirationTime;
    protected volatile long invalidationSequence;
    protected volatile long reservationId = READ_PERMITTED;

    public AbstractNearCacheRecord(V value, long creationTime, long expirationTime) {
        this.value = value;
        this.creationTime = stripBaseTime(creationTime);
        this.expirationTime = stripBaseTime(expirationTime);
    }

    @Override
    public boolean isCachedAsNull() {
        return cachedAsNull;
    }

    @Override
    public void setCachedAsNull(boolean valueCachedAsNull) {
        this.cachedAsNull = valueCachedAsNull;
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
        return recomputeWithBaseTime(expirationTime);
    }

    @Override
    public void setExpirationTime(long expirationTime) {
        this.expirationTime = stripBaseTime(expirationTime);
    }

    @Override
    public long getCreationTime() {
        return recomputeWithBaseTime(creationTime);
    }

    @Override
    public void setCreationTime(long creationTime) {
        this.creationTime = stripBaseTime(creationTime);
    }

    @Override
    public long getLastAccessTime() {
        return recomputeWithBaseTime(lastAccessTime);
    }

    @Override
    public void setLastAccessTime(long lastAccessTime) {
        this.lastAccessTime = stripBaseTime(lastAccessTime);
    }

    @Override
    public long getHits() {
        return hits;
    }

    @Override
    public void setHits(int hits) {
        HITS.set(this, hits);
    }

    @Override
    public void incrementHits() {
        HITS.addAndGet(this, 1);
    }

    @Override
    public long getReservationId() {
        return reservationId;
    }

    @Override
    public void setReservationId(long reservationId) {
        RECORD_STATE.set(this, reservationId);
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
        return invalidationSequence;
    }

    @Override
    public void setInvalidationSequence(long sequence) {
        this.invalidationSequence = sequence;
    }

    @Override
    public void setUuid(UUID uuid) {
        this.uuid = uuid;
    }

    @Override
    public boolean hasSameUuid(UUID thatUuid) {
        return uuid != null && thatUuid != null && uuid.equals(thatUuid);
    }

    @Override
    public String toString() {
        return "AbstractNearCacheRecord{"
                + "creationTime=" + creationTime
                + ", value=" + value
                + ", uuid=" + uuid
                + ", cachedAsNull=" + cachedAsNull
                + ", hits=" + hits
                + ", partitionId=" + partitionId
                + ", lastAccessTime=" + lastAccessTime
                + ", expirationTime=" + expirationTime
                + ", invalidationSequence=" + invalidationSequence
                + ", reservationId=" + reservationId
                + '}';
    }
}
