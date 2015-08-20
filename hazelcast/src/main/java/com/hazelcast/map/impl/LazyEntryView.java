/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl;

import com.hazelcast.core.EntryView;
import com.hazelcast.map.merge.HigherHitsMapMergePolicy;
import com.hazelcast.map.merge.LatestUpdateMapMergePolicy;
import com.hazelcast.map.merge.MapMergePolicy;
import com.hazelcast.map.merge.PassThroughMergePolicy;
import com.hazelcast.map.merge.PutIfAbsentMapMergePolicy;
import com.hazelcast.internal.serialization.SerializationService;

/**
 * LazyEntryView is an implementation of {@link com.hazelcast.core.EntryView} and also it is writable.
 *
 * @param <K> the type of key.
 * @param <V> the type of value.
 */

class LazyEntryView<K, V> implements EntryView<K, V> {

    private K key;
    private V value;
    private long cost;
    private long creationTime;
    private long expirationTime;
    private long hits;
    private long lastAccessTime;
    private long lastStoredTime;
    private long lastUpdateTime;
    private long version;
    private long evictionCriteriaNumber;
    private long ttl;

    private SerializationService serializationService;
    private MapMergePolicy mergePolicy;

    public LazyEntryView() {
    }

    public LazyEntryView(K key, V value, SerializationService serializationService, MapMergePolicy mergePolicy) {
        this.value = value;
        this.key = key;
        this.serializationService = serializationService;
        this.mergePolicy = mergePolicy;
    }

    @Override
    public K getKey() {
        key = serializationService.toObject(key);
        return key;
    }

    public void setKey(K key) {
        this.key = key;
    }

    @Override
    public V getValue() {
        if (returnRawData(mergePolicy)) {
            return value;
        }
        value = serializationService.toObject(value);
        return value;
    }

    private boolean returnRawData(MapMergePolicy mergePolicy) {
        return mergePolicy instanceof PutIfAbsentMapMergePolicy
                || mergePolicy instanceof PassThroughMergePolicy
                || mergePolicy instanceof HigherHitsMapMergePolicy
                || mergePolicy instanceof LatestUpdateMapMergePolicy;
    }

    public void setValue(V value) {
        this.value = value;
    }

    @Override
    public long getCost() {
        return cost;
    }

    public void setCost(long cost) {
        this.cost = cost;
    }

    @Override
    public long getCreationTime() {
        return creationTime;
    }

    public void setCreationTime(long creationTime) {
        this.creationTime = creationTime;
    }

    @Override
    public long getExpirationTime() {
        return expirationTime;
    }

    public void setExpirationTime(long expirationTime) {
        this.expirationTime = expirationTime;
    }

    @Override
    public long getHits() {
        return hits;
    }

    public void setHits(long hits) {
        this.hits = hits;
    }

    @Override
    public long getLastAccessTime() {
        return lastAccessTime;
    }

    public void setLastAccessTime(long lastAccessTime) {
        this.lastAccessTime = lastAccessTime;
    }

    @Override
    public long getLastStoredTime() {
        return lastStoredTime;
    }

    public void setLastStoredTime(long lastStoredTime) {
        this.lastStoredTime = lastStoredTime;
    }

    @Override
    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    @Override
    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public long getEvictionCriteriaNumber() {
        return evictionCriteriaNumber;
    }

    public void setEvictionCriteriaNumber(long evictionCriteriaNumber) {
        this.evictionCriteriaNumber = evictionCriteriaNumber;
    }

    @Override
    public long getTtl() {
        return ttl;
    }

    public void setTtl(long ttl) {
        this.ttl = ttl;
    }
}
