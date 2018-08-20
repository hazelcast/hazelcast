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

package com.hazelcast.map.impl;

import com.hazelcast.core.EntryView;
import com.hazelcast.map.merge.HigherHitsMapMergePolicy;
import com.hazelcast.map.merge.LatestUpdateMapMergePolicy;
import com.hazelcast.map.merge.MapMergePolicy;
import com.hazelcast.map.merge.PassThroughMergePolicy;
import com.hazelcast.map.merge.PutIfAbsentMapMergePolicy;
import com.hazelcast.spi.serialization.SerializationService;

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
    private long ttl;
    private long maxIdle;

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

    public LazyEntryView<K, V> setKey(K key) {
        this.key = key;
        return this;
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

    public LazyEntryView<K, V> setValue(V value) {
        this.value = value;
        return this;
    }

    @Override
    public long getCost() {
        return cost;
    }

    public LazyEntryView<K, V> setCost(long cost) {
        this.cost = cost;
        return this;
    }

    @Override
    public long getCreationTime() {
        return creationTime;
    }

    public LazyEntryView<K, V> setCreationTime(long creationTime) {
        this.creationTime = creationTime;
        return this;
    }

    @Override
    public long getExpirationTime() {
        return expirationTime;
    }

    public LazyEntryView<K, V> setExpirationTime(long expirationTime) {
        this.expirationTime = expirationTime;
        return this;
    }

    @Override
    public long getHits() {
        return hits;
    }

    public LazyEntryView<K, V> setHits(long hits) {
        this.hits = hits;
        return this;
    }

    @Override
    public long getLastAccessTime() {
        return lastAccessTime;
    }

    public LazyEntryView<K, V> setLastAccessTime(long lastAccessTime) {
        this.lastAccessTime = lastAccessTime;
        return this;
    }

    @Override
    public long getLastStoredTime() {
        return lastStoredTime;
    }

    public LazyEntryView<K, V> setLastStoredTime(long lastStoredTime) {
        this.lastStoredTime = lastStoredTime;
        return this;
    }

    @Override
    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public LazyEntryView<K, V> setLastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
        return this;
    }

    @Override
    public long getVersion() {
        return version;
    }

    public LazyEntryView<K, V> setVersion(long version) {
        this.version = version;
        return this;
    }

    public long getEvictionCriteriaNumber() {
        return 0;
    }

    public LazyEntryView<K, V> setEvictionCriteriaNumber(long evictionCriteriaNumber) {
        return this;
    }

    @Override
    public long getTtl() {
        return ttl;
    }

    public LazyEntryView<K, V> setTtl(long ttl) {
        this.ttl = ttl;
        return this;
    }

    @Override
    public long getMaxIdle() {
        return maxIdle;
    }

    public LazyEntryView<K, V> setMaxIdle(long maxIdle) {
        this.maxIdle = maxIdle;
        return this;
    }
}
