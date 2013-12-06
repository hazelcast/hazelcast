package com.hazelcast.replicatedmap;

import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.replicatedmap.record.ReplicatedRecordStore;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.NodeEngine;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class ReplicatedMapProxy<K, V> extends AbstractDistributedObject implements ReplicatedMap<K, V> {

    private final ReplicatedRecordStore replicatedRecordStore;

    ReplicatedMapProxy(NodeEngine nodeEngine, ReplicatedRecordStore replicatedRecordStore) {
        super(nodeEngine, replicatedRecordStore.getReplicatedMapService());
        this.replicatedRecordStore = replicatedRecordStore;
    }

    @Override
    public String getName() {
        return replicatedRecordStore.getName();
    }

    @Override
    public String getPartitionKey() {
        return getName();
    }

    @Override
    public String getServiceName() {
        return ReplicatedMapService.SERVICE_NAME;
    }

    @Override
    public int size() {
        return replicatedRecordStore.size();
    }

    @Override
    public boolean isEmpty() {
        return replicatedRecordStore.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return replicatedRecordStore.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return replicatedRecordStore.containsValue(value);
    }

    @Override
    public V get(Object key) {
        return (V) replicatedRecordStore.get(key);
    }

    @Override
    public V put(K key, V value) {
        return (V) replicatedRecordStore.put(key, value);
    }

    @Override
    public V remove(Object key) {
        return (V) replicatedRecordStore.remove(key);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        for (Map.Entry<? extends K, ? extends V> entry : m.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void clear() {
        replicatedRecordStore.clear();
    }

    @Override
    public Set<K> keySet() {
        return replicatedRecordStore.keySet();
    }

    @Override
    public Collection<V> values() {
        return replicatedRecordStore.values();
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return replicatedRecordStore.entrySet();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " -> " + replicatedRecordStore.getName();
    }

}
