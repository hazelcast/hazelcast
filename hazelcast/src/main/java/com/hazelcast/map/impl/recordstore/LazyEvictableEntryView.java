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

package com.hazelcast.map.impl.recordstore;

import com.hazelcast.core.EntryView;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.SampleableConcurrentHashMap;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.expiry.ExpiryMetadata;


/**
 * Internally used {@link EntryView} implementation
 * for sampling based eviction specific purposes.
 * <p/>
 * Mainly :
 * - Wraps a {@link Record} and reaches all {@link EntryView} specific info over it
 * - Lazily de-serializes key and value.
 *
 * @param <R> Type of record to construct {@link EntryView} over it.
 */
@SuppressWarnings("checkstyle:cyclomaticcomplexity")
public class LazyEvictableEntryView<R extends Record>
        extends SampleableConcurrentHashMap.SamplingEntry implements EntryView {

    private Data dataKey;
    private Object key;
    private Object value;
    private R record;

    private ExpiryMetadata expiryMetadata;
    private SerializationService serializationService;

    public LazyEvictableEntryView(Data dataKey, R record,
                                  ExpiryMetadata expiryMetadata,
                                  SerializationService serializationService) {
        super(dataKey, record);
        this.dataKey = dataKey;
        this.record = record;
        this.expiryMetadata = expiryMetadata;
        this.serializationService = serializationService;
    }

    @Override
    public Object getKey() {
        if (key == null) {
            key = serializationService.toObject(dataKey);
        }
        return key;
    }

    public Data getDataKey() {
        return dataKey;
    }

    @Override
    public Object getValue() {
        if (value == null) {
            value = serializationService.toObject(record.getValue());
        }
        return value;
    }

    @Override
    public long getCost() {
        return record.getCost();
    }

    @Override
    public long getCreationTime() {
        return record.getCreationTime();
    }

    @Override
    public long getExpirationTime() {
        return expiryMetadata.getExpirationTime();
    }

    @Override
    public long getHits() {
        return record.getHits();
    }

    @Override
    public long getLastAccessTime() {
        return record.getLastAccessTime();
    }

    @Override
    public long getLastStoredTime() {
        return record.getLastStoredTime();
    }

    @Override
    public long getLastUpdateTime() {
        return record.getLastUpdateTime();
    }

    @Override
    public long getVersion() {
        return record.getVersion();
    }

    @Override
    public long getTtl() {
        return expiryMetadata.getTtl();
    }

    @Override
    public long getMaxIdle() {
        return expiryMetadata.getMaxIdle();
    }

    public Record getRecord() {
        return record;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof EntryView)) {
            return false;
        }

        EntryView that = (EntryView) o;

        return getKey().equals(that.getKey())
                && getValue().equals(that.getValue())
                && getVersion() == that.getVersion()
                && getCost() == that.getCost()
                && getCreationTime() == that.getCreationTime()
                && getExpirationTime() == that.getExpirationTime()
                && getHits() == that.getHits()
                && getLastAccessTime() == that.getLastAccessTime()
                && getLastStoredTime() == that.getLastStoredTime()
                && getLastUpdateTime() == that.getLastUpdateTime()
                && getTtl() == that.getTtl();
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + getKey().hashCode();
        result = 31 * result + getValue().hashCode();

        long cost = getCost();
        long creationTime = getCreationTime();
        long expirationTime = getExpirationTime();
        long hits = getHits();
        long lastAccessTime = getLastAccessTime();
        long lastStoredTime = getLastStoredTime();
        long lastUpdateTime = getLastUpdateTime();
        long version = getVersion();
        long ttl = getTtl();

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
        return "EntryView{key=" + getKey()
                + ", value=" + getValue()
                + ", cost=" + getCost()
                + ", version=" + getVersion()
                + ", creationTime=" + getCreationTime()
                + ", expirationTime=" + getExpirationTime()
                + ", hits=" + getHits()
                + ", lastAccessTime=" + getLastAccessTime()
                + ", lastStoredTime=" + getLastStoredTime()
                + ", lastUpdateTime=" + getLastUpdateTime()
                + ", ttl=" + getTtl()
                + '}';
    }
}
