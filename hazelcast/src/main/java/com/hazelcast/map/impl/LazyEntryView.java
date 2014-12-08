package com.hazelcast.map.impl;

import com.hazelcast.core.EntryView;
import com.hazelcast.map.merge.MapMergePolicy;
import com.hazelcast.map.merge.PassThroughMergePolicy;
import com.hazelcast.map.merge.PutIfAbsentMapMergePolicy;
import com.hazelcast.nio.serialization.SerializationService;

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

    public K getKey() {
        key = serializationService.toObject(key);
        return key;
    }

    public void setKey(K key) {
        this.key = key;
    }

    public V getValue() {
        if (validMergePolicyForValueNullCheck(mergePolicy)) {
            return value;
        }
        value = serializationService.toObject(value);
        return value;
    }

    private boolean validMergePolicyForValueNullCheck(MapMergePolicy mergePolicy) {
        return mergePolicy instanceof PutIfAbsentMapMergePolicy
                || mergePolicy instanceof PassThroughMergePolicy;
    }

    public void setValue(V value) {
        this.value = value;
    }

    public long getCost() {
        return cost;
    }

    public void setCost(long cost) {
        this.cost = cost;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public void setCreationTime(long creationTime) {
        this.creationTime = creationTime;
    }

    public long getExpirationTime() {
        return expirationTime;
    }

    public void setExpirationTime(long expirationTime) {
        this.expirationTime = expirationTime;
    }

    public long getHits() {
        return hits;
    }

    public void setHits(long hits) {
        this.hits = hits;
    }

    public long getLastAccessTime() {
        return lastAccessTime;
    }

    public void setLastAccessTime(long lastAccessTime) {
        this.lastAccessTime = lastAccessTime;
    }

    public long getLastStoredTime() {
        return lastStoredTime;
    }

    public void setLastStoredTime(long lastStoredTime) {
        this.lastStoredTime = lastStoredTime;
    }

    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

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

    public long getTtl() {
        return ttl;
    }

    public void setTtl(long ttl) {
        this.ttl = ttl;
    }
}
