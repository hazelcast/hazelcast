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
import com.hazelcast.nio.serialization.Data;

import java.io.IOException;

/**
 * Abstract implementation of {@link com.hazelcast.cache.impl.record.CacheRecord} with key, value and
 * expiration time as internal state.
 * <p>This implementation provides getter, setter and serialization methods.</p>
 *
 * @param <V>
 */
abstract class AbstractCacheRecord<V> implements CacheRecord<V> {

    private Data key;

    private long expirationTime = -1;

    public AbstractCacheRecord() {
    }

    AbstractCacheRecord(Data key, long expirationTime) {
        this.key = key;
        this.expirationTime = expirationTime;
    }

    @Override
    public Data getKey() {
        return key;
    }

    public long getExpirationTime() {
        return expirationTime;
    }

    @Override
    public void setExpirationTime(long expirationTime) {
        this.expirationTime = expirationTime;
    }

    @Override
    public boolean isExpiredAt(long now) {
        return expirationTime > -1 && expirationTime <= now;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeData(key);
        out.writeLong(expirationTime);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        key = in.readData();
        expirationTime = in.readLong();
    }
}
