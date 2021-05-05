/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.compatibility.cache;

import com.hazelcast.cache.CacheEntryView;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.Objects;

/**
 * WAN heap based implementation of {@link CacheEntryView} for processing
 * compatibility WAN replication events from 3.x clusters.
 */
public class CompatibilityWanCacheEntryView implements CacheEntryView<Data, Data>, IdentifiedDataSerializable {

    private Data key;
    private Data value;
    private long creationTime;
    private long expirationTime;
    private long lastAccessTime;
    private long accessHit;

    public CompatibilityWanCacheEntryView() {
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

    @Override
    public Data getExpiryPolicy() {
        return null;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        throw new UnsupportedOperationException(getClass().getName() + " should not be serialized!");
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        creationTime = in.readLong();
        expirationTime = in.readLong();
        lastAccessTime = in.readLong();
        accessHit = in.readLong();
        key = IOUtil.readData(in);
        value = IOUtil.readData(in);
    }

    @Override
    public int getFactoryId() {
        return CompatibilityCacheDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return CompatibilityCacheDataSerializerHook.DEFAULT_CACHE_ENTRY_VIEW;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CompatibilityWanCacheEntryView that = (CompatibilityWanCacheEntryView) o;
        return creationTime == that.creationTime
                && expirationTime == that.expirationTime
                && lastAccessTime == that.lastAccessTime
                && accessHit == that.accessHit
                && Objects.equals(key, that.key)
                && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value, creationTime, expirationTime, lastAccessTime, accessHit);
    }
}
