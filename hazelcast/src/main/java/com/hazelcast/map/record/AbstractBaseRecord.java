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

package com.hazelcast.map.record;

/**
 * @param <V>
 */
@SuppressWarnings("VolatileLongOrDoubleField")
abstract class AbstractBaseRecord<V> implements Record<V> {

    protected long version;
    /**
     * evictionCriteriaNumber may be used for LRU or LFU eviction depending on configuration.
     */
    protected long evictionCriteriaNumber;
    protected long ttl;
    protected long creationTime;
    protected long lastAccessTime;
    protected long lastUpdatedTime;
    protected volatile byte flags;

    public AbstractBaseRecord() {
        version = 0L;
    }

    public final long getVersion() {
        return version;
    }

    public final void setVersion(long version) {
        this.version = version;
    }

    @Override
    public long getEvictionCriteriaNumber() {
        return evictionCriteriaNumber;
    }

    @Override
    public void setEvictionCriteriaNumber(long evictionCriteriaNumber) {
        this.evictionCriteriaNumber = evictionCriteriaNumber;
    }

    @Override
    public long getTtl() {
        return ttl;
    }

    @Override
    public void setTtl(long ttl) {
        this.ttl = ttl;
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
    public long getLastUpdatedTime() {
        return lastUpdatedTime;
    }

    @Override
    public void setLastUpdatedTime(long lastUpdatedTime) {
        this.lastUpdatedTime = lastUpdatedTime;
    }

    @Override
    public byte getFlags() {
        return flags;
    }

    @Override
    public void setFlags(byte flags) {
        this.flags = flags;
    }

    @Override
    public long getCost() {
        int size = 0;
        // add size of version.
        size += (Long.SIZE / Byte.SIZE);
        // add size of evictionCriteriaNumber.
        size += (Long.SIZE / Byte.SIZE);
        return size;
    }

}
