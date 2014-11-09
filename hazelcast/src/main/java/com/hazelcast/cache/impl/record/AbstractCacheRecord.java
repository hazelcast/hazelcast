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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

/**
 * Abstract implementation of {@link com.hazelcast.cache.impl.record.CacheRecord} with key, value and
 * expiration time as internal state.
 * <p>This implementation provides getter, setter and serialization methods.</p>
 *
 * @param <V> the type of the value stored by this {@link AbstractCacheRecord}
 */
public abstract class AbstractCacheRecord<V> implements CacheRecord<V>, DataSerializable {

    protected long creationTime = -1;
    protected volatile long expirationTime = -1;
    protected volatile long accessTime = -1;
    protected volatile int accessHit = 0;

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
        this.accessHit = accessHit;
    }

    @Override
    public void incrementAccessHit() {
        accessHit++;
    }

    @Override
    public void resetAccessHit() {
        accessHit = 0;
    }

    @Override
    public boolean isExpiredAt(long now) {
        return expirationTime > -1 && expirationTime <= now;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(creationTime);
        out.writeLong(expirationTime);
        out.writeLong(accessTime);
        out.writeInt(accessHit);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        creationTime = in.readLong();
        expirationTime = in.readLong();
        accessTime = in.readLong();
        accessHit = in.readInt();
    }
}
