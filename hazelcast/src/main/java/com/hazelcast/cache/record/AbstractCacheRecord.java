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

package com.hazelcast.cache.record;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;

import java.io.IOException;

class AbstractCacheRecord<V> implements CacheRecord<V> {

    private Data key;
    private V value;

    private long expirationTime = -1;

    AbstractCacheRecord(Data key, V value, long expirationTime) {
        this.key = key;
        this.value = value;
        this.expirationTime = expirationTime;
    }

    @Override
    public Data getKey() {
        return key;
    }

    @Override
    public V getValue() {
        return value;
    }

    @Override
    public void setValue(V value) {
        this.value = value;
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
        key.writeData(out);
        out.writeLong(expirationTime);
        out.writeObject(value);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        key = new Data();
        key.readData(in);
        expirationTime = in.readLong();
        value = in.readObject();

    }
}
