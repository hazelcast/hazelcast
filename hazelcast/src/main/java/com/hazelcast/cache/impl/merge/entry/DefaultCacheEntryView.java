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

package com.hazelcast.cache.impl.merge.entry;

import com.hazelcast.cache.CacheEntryView;
import com.hazelcast.cache.impl.CacheDataSerializerHook;
import com.hazelcast.internal.nio.DataWriter;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

/**
 * Default heap based implementation of {@link com.hazelcast.cache.CacheEntryView}.
 */
public class DefaultCacheEntryView
        implements CacheEntryView<Data, Data>, IdentifiedDataSerializable {

    private Data key;
    private Data value;
    private long creationTime;
    private long expirationTime;
    private long lastAccessTime;
    private long accessHit;
    private Data expiryPolicy;

    public DefaultCacheEntryView() {
    }

    public DefaultCacheEntryView(Data key, Data value, long creationTime,
                                 long expirationTime, long lastAccessTime, long accessHit,
                                 Data expiryPolicy) {
        this.key = key;
        this.value = value;
        this.creationTime = creationTime;
        this.expirationTime = expirationTime;
        this.lastAccessTime = lastAccessTime;
        this.accessHit = accessHit;
        this.expiryPolicy = expiryPolicy;
    }

    @Override
    public Data getKey() {
        return key;
    }

    @Override
    public Data getValue() {
        return value;
    }

    @Override
    public long getCreationTime() {
        return creationTime;
    }

    @Override
    public long getExpirationTime() {
        return expirationTime;
    }

    @Override
    public long getLastAccessTime() {
        return lastAccessTime;
    }

    @Override
    public long getHits() {
        return accessHit;
    }

    /**
     * Gets the expiry policy associated with this entry if any
     *
     * @return expiry policy associated with this entry or {@code null}
     */
    @Override
    public Data getExpiryPolicy() {
        return expiryPolicy;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        assert out instanceof DataWriter;
        out.writeLong(creationTime);
        out.writeLong(expirationTime);
        out.writeLong(lastAccessTime);
        out.writeLong(accessHit);
        IOUtil.writeData(out, key);
        IOUtil.writeData(out, value);
        IOUtil.writeData(out, expiryPolicy);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        creationTime = in.readLong();
        expirationTime = in.readLong();
        lastAccessTime = in.readLong();
        accessHit = in.readLong();
        key = IOUtil.readData(in);
        value = IOUtil.readData(in);
        expiryPolicy = IOUtil.readData(in);
    }

    @Override
    public int getFactoryId() {
        return CacheDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return CacheDataSerializerHook.DEFAULT_CACHE_ENTRY_VIEW;
    }
}
