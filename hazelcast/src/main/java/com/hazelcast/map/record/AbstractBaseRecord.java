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
    protected long lastAccessTime;
    protected long lastUpdateTime;
    protected long creationTime;

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
    public long getLastAccessTime() {
        return lastAccessTime;
    }

    @Override
    public void setLastAccessTime(long lastAccessTime) {
        this.lastAccessTime = lastAccessTime;
    }

    @Override
    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    @Override
    public void setLastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
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
    public long getCost() {
        final int numberOfLongs = 6;
        return numberOfLongs * (Long.SIZE / Byte.SIZE);
    }

}
