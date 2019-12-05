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

package com.hazelcast.spi.impl.merge;

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.MapMergeTypes;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.SerializationServiceAware;

import java.io.IOException;

/**
 * Implementation of {@link MapMergeTypes}.
 *
 * @since 3.10
 */
@SuppressWarnings({"WeakerAccess", "checkstyle:methodcount"})
public class MapMergingEntryImpl
        implements MapMergeTypes, SerializationServiceAware, IdentifiedDataSerializable {

    private Data value;
    private Data key;
    private long cost = -1;
    private long creationTime = -1;
    private long expirationTime = -1;
    private long hits = -1;
    private long lastAccessTime = -1;
    private long lastStoredTime = -1;
    private long lastUpdateTime = -1;
    private long version = -1;
    private long ttl = -1;
    // can be null when merging entries received through WAN
    // see com.hazelcast.map.impl.wan.WanMapEntryView.getMaxIdle
    private Long maxIdle;

    private transient SerializationService serializationService;

    public MapMergingEntryImpl() {
    }

    public MapMergingEntryImpl(SerializationService serializationService) {
        this.serializationService = serializationService;
    }

    @Override
    public Data getValue() {
        return value;
    }

    @Override
    public Object getDeserializedValue() {
        return serializationService.toObject(value);
    }

    public MapMergingEntryImpl setValue(Data value) {
        this.value = value;
        return this;
    }

    @Override
    public Data getKey() {
        return key;
    }

    @Override
    public Object getDeserializedKey() {
        return serializationService.toObject(key);
    }

    public MapMergingEntryImpl setKey(Data key) {
        this.key = key;
        return this;
    }

    @Override
    public long getCost() {
        return cost;
    }

    public MapMergingEntryImpl setCost(long cost) {
        this.cost = cost;
        return this;
    }

    @Override
    public long getCreationTime() {
        return creationTime;
    }

    public MapMergingEntryImpl setCreationTime(long creationTime) {
        this.creationTime = creationTime;
        return this;
    }

    @Override
    public long getExpirationTime() {
        return expirationTime;
    }

    public MapMergingEntryImpl setExpirationTime(long expirationTime) {
        this.expirationTime = expirationTime;
        return this;
    }

    @Override
    public long getHits() {
        return hits;
    }

    public MapMergingEntryImpl setHits(long hits) {
        this.hits = hits;
        return this;
    }

    @Override
    public long getLastAccessTime() {
        return lastAccessTime;
    }

    public MapMergingEntryImpl setLastAccessTime(long lastAccessTime) {
        this.lastAccessTime = lastAccessTime;
        return this;
    }

    @Override
    public long getLastStoredTime() {
        return lastStoredTime;
    }

    public MapMergingEntryImpl setLastStoredTime(long lastStoredTime) {
        this.lastStoredTime = lastStoredTime;
        return this;
    }

    @Override
    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public MapMergingEntryImpl setLastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
        return this;
    }

    @Override
    public long getVersion() {
        return version;
    }

    public MapMergingEntryImpl setVersion(long version) {
        this.version = version;
        return this;
    }

    @Override
    public long getTtl() {
        return ttl;
    }

    public MapMergingEntryImpl setTtl(long ttl) {
        this.ttl = ttl;
        return this;
    }

    @Override
    public Long getMaxIdle() {
        return maxIdle;
    }

    public MapMergingEntryImpl setMaxIdle(Long maxIdle) {
        this.maxIdle = maxIdle;
        return this;
    }

    @Override
    public void setSerializationService(SerializationService serializationService) {
        this.serializationService = serializationService;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        IOUtil.writeObject(out, key);
        IOUtil.writeObject(out, value);
        out.writeLong(cost);
        out.writeLong(creationTime);
        out.writeLong(expirationTime);
        out.writeLong(hits);
        out.writeLong(lastAccessTime);
        out.writeLong(lastStoredTime);
        out.writeLong(lastUpdateTime);
        out.writeLong(version);
        out.writeLong(ttl);
        // WAN events received from source cluster also carry null maxIdle
        // see com.hazelcast.map.impl.wan.WanMapEntryView.getMaxIdle
        boolean hasMaxIdle = maxIdle != null;
        out.writeBoolean(hasMaxIdle);
        if (hasMaxIdle) {
            out.writeLong(maxIdle);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        key = IOUtil.readObject(in);
        value = IOUtil.readObject(in);
        cost = in.readLong();
        creationTime = in.readLong();
        expirationTime = in.readLong();
        hits = in.readLong();
        lastAccessTime = in.readLong();
        lastStoredTime = in.readLong();
        lastUpdateTime = in.readLong();
        version = in.readLong();
        ttl = in.readLong();
        // WAN events received from source cluster also carry null maxIdle
        // see com.hazelcast.map.impl.wan.WanMapEntryView.getMaxIdle
        boolean hasMaxIdle = in.readBoolean();
        if (hasMaxIdle) {
            maxIdle = in.readLong();
        }
    }

    @Override
    public int getFactoryId() {
        return SplitBrainDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SplitBrainDataSerializerHook.MAP_MERGING_ENTRY;
    }

    @Override
    @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity"})
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MapMergingEntryImpl that = (MapMergingEntryImpl) o;

        if (cost != that.cost) {
            return false;
        }
        if (creationTime != that.creationTime) {
            return false;
        }
        if (expirationTime != that.expirationTime) {
            return false;
        }
        if (hits != that.hits) {
            return false;
        }
        if (lastAccessTime != that.lastAccessTime) {
            return false;
        }
        if (lastStoredTime != that.lastStoredTime) {
            return false;
        }
        if (lastUpdateTime != that.lastUpdateTime) {
            return false;
        }
        if (version != that.version) {
            return false;
        }
        if (ttl != that.ttl) {
            return false;
        }
        if (value != null ? !value.equals(that.value) : that.value != null) {
            return false;
        }
        if (key != null ? !key.equals(that.key) : that.key != null) {
            return false;
        }
        return maxIdle != null ? maxIdle.equals(that.maxIdle) : that.maxIdle == null;
    }

    @Override
    public int hashCode() {
        int result = value != null ? value.hashCode() : 0;
        result = 31 * result + (key != null ? key.hashCode() : 0);
        result = 31 * result + (int) (cost ^ (cost >>> 32));
        result = 31 * result + (int) (creationTime ^ (creationTime >>> 32));
        result = 31 * result + (int) (expirationTime ^ (expirationTime >>> 32));
        result = 31 * result + (int) (hits ^ (hits >>> 32));
        result = 31 * result + (int) (lastAccessTime ^ (lastAccessTime >>> 32));
        result = 31 * result + (int) (lastStoredTime ^ (lastStoredTime >>> 32));
        result = 31 * result + (int) (lastUpdateTime ^ (lastUpdateTime >>> 32));
        result = 31 * result + (int) (version ^ (version >>> 32));
        result = 31 * result + (int) (ttl ^ (ttl >>> 32));
        result = 31 * result + (maxIdle != null ? maxIdle.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "MapMergingEntry{"
                + "key=" + key
                + ", value=" + value
                + ", cost=" + cost
                + ", creationTime=" + creationTime
                + ", expirationTime=" + expirationTime
                + ", hits=" + hits
                + ", lastAccessTime=" + lastAccessTime
                + ", lastStoredTime=" + lastStoredTime
                + ", lastUpdateTime=" + lastUpdateTime
                + ", version=" + version
                + ", ttl=" + ttl
                + ", maxIdle=" + maxIdle
                + '}';
    }
}
