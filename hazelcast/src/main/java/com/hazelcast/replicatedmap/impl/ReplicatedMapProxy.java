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

package com.hazelcast.replicatedmap.impl;

import com.hazelcast.core.EntryListener;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.monitor.LocalReplicatedMapStats;
import com.hazelcast.query.Predicate;
import com.hazelcast.replicatedmap.impl.record.AbstractReplicatedMapContainer;
import com.hazelcast.replicatedmap.impl.record.ReplicationPublisher;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.InitializingObject;
import com.hazelcast.spi.NodeEngine;

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * The internal {@link com.hazelcast.core.ReplicatedMap} implementation proxying the requests to the underlying
 * {@code ReplicatedMapContainer}
 *
 * @param <K> key type
 * @param <V> value type
 */
public class ReplicatedMapProxy<K, V>
        extends AbstractDistributedObject
        implements ReplicatedMap<K, V>, InitializingObject {

    private final AbstractReplicatedMapContainer<K, V> replicatedMapContainer;

    ReplicatedMapProxy(NodeEngine nodeEngine, AbstractReplicatedMapContainer<K, V> replicatedMapContainer) {
        super(nodeEngine, replicatedMapContainer.getReplicatedMapService());
        this.replicatedMapContainer = replicatedMapContainer;
    }

    @Override
    public String getName() {
        return replicatedMapContainer.getName();
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
        return replicatedMapContainer.size();
    }

    @Override
    public boolean isEmpty() {
        return replicatedMapContainer.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return replicatedMapContainer.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return replicatedMapContainer.containsValue(value);
    }

    @Override
    public V get(Object key) {
        return (V) replicatedMapContainer.get(key);
    }

    @Override
    public V put(K key, V value) {
        return (V) replicatedMapContainer.put(key, value);
    }

    @Override
    public V put(K key, V value, long ttl, TimeUnit timeUnit) {
        return (V) replicatedMapContainer.put(key, value, ttl, timeUnit);
    }

    @Override
    public V remove(Object key) {
        return (V) replicatedMapContainer.remove(key);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        if (m == null) {
            throw new NullPointerException("m cannot be null");
        }
        for (Map.Entry<? extends K, ? extends V> entry : m.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void clear() {
        replicatedMapContainer.clear(true, true);
    }

    @Override
    public boolean removeEntryListener(String id) {
        return replicatedMapContainer.removeEntryListenerInternal(id);
    }

    @Override
    public String addEntryListener(EntryListener<K, V> listener) {
        return replicatedMapContainer.addEntryListener(listener, null);
    }

    @Override
    public String addEntryListener(EntryListener<K, V> listener, K key) {
        return replicatedMapContainer.addEntryListener(listener, key);
    }

    @Override
    public String addEntryListener(EntryListener<K, V> listener, Predicate<K, V> predicate) {
        return replicatedMapContainer.addEntryListener(listener, predicate, null);
    }

    @Override
    public String addEntryListener(EntryListener<K, V> listener, Predicate<K, V> predicate, K key) {
        return replicatedMapContainer.addEntryListener(listener, predicate, key);
    }

    @Override
    public Set<K> keySet() {
        return replicatedMapContainer.keySet();
    }

    @Override
    public Collection<V> values() {
        return replicatedMapContainer.values();
    }

    @Override
    public Collection<V> values(Comparator<V> comparator) {
        return replicatedMapContainer.values(comparator);
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return replicatedMapContainer.entrySet();
    }

    public boolean storageEquals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        ReplicatedMapProxy that = (ReplicatedMapProxy) o;

        if (!replicatedMapContainer.equals(that.replicatedMapContainer)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + replicatedMapContainer.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " -> " + replicatedMapContainer.getName();
    }

    @Override
    public void initialize() {
        replicatedMapContainer.initialize();
    }

    public LocalReplicatedMapStats getReplicatedMapStats() {
        return replicatedMapContainer.createReplicatedMapStats();
    }

    public void setPreReplicationHook(PreReplicationHook preReplicationHook) {
        ReplicationPublisher<K, V> replicationPublisher = replicatedMapContainer.getReplicationPublisher();
        replicationPublisher.setPreReplicationHook(preReplicationHook);
    }

}
