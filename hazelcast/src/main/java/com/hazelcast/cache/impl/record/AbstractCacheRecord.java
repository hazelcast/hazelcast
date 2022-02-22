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

package com.hazelcast.cache.impl.record;

import com.hazelcast.cache.impl.CacheDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;

/**
 * Abstract implementation of {@link com.hazelcast.cache.impl.record.CacheRecord} with key, value and
 * expiration time as internal state.
 * <p>This implementation provides getter, setter and serialization methods.</p>
 *
 * @param <V> the type of the value stored by this {@link AbstractCacheRecord}
 */
public abstract class AbstractCacheRecord<V, E> implements CacheRecord<V, E>, IdentifiedDataSerializable {

    protected long creationTime = TIME_NOT_AVAILABLE;

    protected volatile int hits;
    protected volatile long expirationTime = TIME_NOT_AVAILABLE;
    protected volatile long lastAccessTime = TIME_NOT_AVAILABLE;

    protected AbstractCacheRecord() {
    }

    public AbstractCacheRecord(long creationTime, long expirationTime) {
        this.creationTime = creationTime;
        this.expirationTime = expirationTime;
    }

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
    public long getHits() {
        return hits;
    }

    @Override
    public void setHits(long accessHit) {
        this.hits = accessHit > Integer.MAX_VALUE
                ? Integer.MAX_VALUE : (int) accessHit;
    }

    @Override
    @SuppressFBWarnings(value = "VO_VOLATILE_INCREMENT",
            justification = "CacheRecord can be accessed by only its own partition thread.")
    public void incrementHits() {
        hits++;
    }

    @Override
    public boolean isExpiredAt(long now) {
        return expirationTime > TIME_NOT_AVAILABLE && expirationTime <= now;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(creationTime);
        out.writeLong(expirationTime);
        out.writeLong(lastAccessTime);
        out.writeInt(hits);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        creationTime = in.readLong();
        expirationTime = in.readLong();
        lastAccessTime = in.readLong();
        hits = in.readInt();
    }

    @Override
    public int getFactoryId() {
        return CacheDataSerializerHook.F_ID;
    }
}
