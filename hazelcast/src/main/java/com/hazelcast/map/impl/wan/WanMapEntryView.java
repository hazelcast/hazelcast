/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.wan;

import com.hazelcast.core.EntryView;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.BinaryInterface;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

/**
 * WAN heap based implementation of {@link EntryView} for keeping
 * compatibility when sending to older (3.8+) clusters.
 *
 * @param <K> the type of key.
 * @param <V> the type of value.
 */
@BinaryInterface
public class WanMapEntryView<K, V> implements EntryView<K, V>, IdentifiedDataSerializable {

    private final K key;
    private final V value;
    private final long cost;
    private final long creationTime;
    private final long expirationTime;
    private final long hits;
    private final long lastAccessTime;
    private final long lastStoredTime;
    private final long lastUpdateTime;
    private final long version;
    private final long ttl;

    public WanMapEntryView(EntryView<K, V> entryView) {
        this.key = entryView.getKey();
        this.value = entryView.getValue();
        this.cost = entryView.getCost();
        this.version = entryView.getVersion();
        this.hits = entryView.getHits();
        this.lastAccessTime = entryView.getLastAccessTime();
        this.lastUpdateTime = entryView.getLastUpdateTime();
        this.ttl = entryView.getTtl();
        this.creationTime = entryView.getCreationTime();
        this.expirationTime = entryView.getExpirationTime();
        this.lastStoredTime = entryView.getLastStoredTime();
    }

    @Override
    public K getKey() {
        return key;
    }

    @Override
    public V getValue() {
        return value;
    }

    @Override
    public long getCost() {
        return cost;
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
    public long getHits() {
        return hits;
    }

    @Override
    public long getLastAccessTime() {
        return lastAccessTime;
    }

    @Override
    public long getLastStoredTime() {
        return lastStoredTime;
    }

    @Override
    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    @Override
    public long getVersion() {
        return version;
    }

    @Override
    public long getTtl() {
        return ttl;
    }

    @Override
    public Long getMaxIdle() {
        return null;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        // the serialized format must match the serialized format for a 3.8 SimpleEntryView
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
        out.writeLong(0);
        out.writeLong(ttl);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        throw new UnsupportedOperationException(getClass().getName() + " should not be deserialized!");
    }

    @Override
    public int getFactoryId() {
        // needs to have same factoryId and ID as SimpleEntryView
        // for backwards compatibility when sending to a 3.8+ cluster
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        // needs to have same factoryId and ID as SimpleEntryView
        // for backwards compatibility when sending to a 3.8+ cluster
        return MapDataSerializerHook.ENTRY_VIEW;
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

        WanMapEntryView<?, ?> that = (WanMapEntryView<?, ?>) o;
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
        if (key != null ? !key.equals(that.key) : that.key != null) {
            return false;
        }
        return value != null ? value.equals(that.value) : that.value == null;
    }

    @Override
    public int hashCode() {
        int result = key != null ? key.hashCode() : 0;
        result = 31 * result + (value != null ? value.hashCode() : 0);
        result = 31 * result + (int) (cost ^ (cost >>> 32));
        result = 31 * result + (int) (creationTime ^ (creationTime >>> 32));
        result = 31 * result + (int) (expirationTime ^ (expirationTime >>> 32));
        result = 31 * result + (int) (hits ^ (hits >>> 32));
        result = 31 * result + (int) (lastAccessTime ^ (lastAccessTime >>> 32));
        result = 31 * result + (int) (lastStoredTime ^ (lastStoredTime >>> 32));
        result = 31 * result + (int) (lastUpdateTime ^ (lastUpdateTime >>> 32));
        result = 31 * result + (int) (version ^ (version >>> 32));
        result = 31 * result + (int) (ttl ^ (ttl >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "WanMapEntryView{"
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
                + '}';
    }
}
