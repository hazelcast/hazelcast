/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.stream.impl;

import com.hazelcast.core.EntryListener;
import com.hazelcast.core.EntryView;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.stream.IStreamMap;
import com.hazelcast.jet.stream.DistributedStream;
import com.hazelcast.jet.stream.impl.pipeline.StreamContext;
import com.hazelcast.jet.stream.impl.source.MapSourcePipeline;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.MapInterceptor;
import com.hazelcast.map.listener.MapListener;
import com.hazelcast.map.listener.MapPartitionLostListener;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.aggregation.Aggregation;
import com.hazelcast.mapreduce.aggregation.Supplier;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.query.Predicate;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@SuppressWarnings({"checkstyle:methodcount", "deprecation"})
public class MapDecorator<K, V> implements IStreamMap<K, V> {

    private final IMap<K, V> map;
    private final HazelcastInstance instance;

    public MapDecorator(IMap<K, V> map, HazelcastInstance instance) {
        this.map = map;
        this.instance = instance;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        map.putAll(m);
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return map.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return map.containsValue(value);
    }

    @Override
    public V get(Object key) {
        return map.get(key);
    }

    @Override
    public V put(K key, V value) {
        return map.put(key, value);
    }

    @Override
    public V remove(Object key) {
        return map.remove(key);
    }

    @Override
    public boolean remove(Object key, Object value) {
        return map.remove(key, value);
    }

    @Override
    public void delete(Object key) {
        map.delete(key);
    }

    @Override
    public void flush() {
        map.flush();
    }

    @Override
    public Map<K, V> getAll(Set<K> keys) {
        return map.getAll(keys);
    }

    @Override
    public void loadAll(boolean replaceExistingValues) {
        map.loadAll(replaceExistingValues);
    }

    @Override
    public void loadAll(Set<K> keys, boolean replaceExistingValues) {
        map.loadAll(keys, replaceExistingValues);
    }

    @Override
    public void clear() {
        map.clear();
    }

    @Override
    public ICompletableFuture<V> getAsync(K key) {
        return map.getAsync(key);
    }

    @Override
    public ICompletableFuture<V> putAsync(K key, V value) {
        return map.putAsync(key, value);
    }

    @Override
    public ICompletableFuture<V> putAsync(K key, V value, long ttl, TimeUnit timeunit) {
        return map.putAsync(key, value, ttl, timeunit);
    }

    @Override
    public ICompletableFuture<Void> setAsync(K key, V value) {
        return map.setAsync(key, value);
    }

    @Override
    public ICompletableFuture<Void> setAsync(K key, V value, long ttl, TimeUnit timeunit) {
        return map.setAsync(key, value, ttl, timeunit);
    }

    @Override
    public ICompletableFuture<V> removeAsync(K key) {
        return map.removeAsync(key);
    }

    @Override
    public boolean tryRemove(K key, long timeout, TimeUnit timeunit) {
        return map.tryRemove(key, timeout, timeunit);
    }

    @Override
    public boolean tryPut(K key, V value, long timeout, TimeUnit timeunit) {
        return map.tryPut(key, value, timeout, timeunit);
    }

    @Override
    public V put(K key, V value, long ttl, TimeUnit timeunit) {
        return map.put(key, value, ttl, timeunit);
    }

    @Override
    public void putTransient(K key, V value, long ttl, TimeUnit timeunit) {
        map.putTransient(key, value, ttl, timeunit);
    }

    @Override
    public V putIfAbsent(K key, V value) {
        return map.putIfAbsent(key, value);
    }

    @Override
    public V putIfAbsent(K key, V value, long ttl, TimeUnit timeunit) {
        return map.putIfAbsent(key, value, ttl, timeunit);
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        return map.replace(key, oldValue, newValue);
    }

    @Override
    public V replace(K key, V value) {
        return map.replace(key, value);
    }

    @Override
    public void set(K key, V value) {
        map.set(key, value);
    }

    @Override
    public void set(K key, V value, long ttl, TimeUnit timeunit) {
        map.set(key, value, ttl, timeunit);
    }

    @Override
    public void lock(K key) {
        map.lock(key);
    }

    @Override
    public void lock(K key, long leaseTime, TimeUnit timeUnit) {
        map.lock(key, leaseTime, timeUnit);
    }

    @Override
    public boolean isLocked(K key) {
        return map.isLocked(key);
    }

    @Override
    public boolean tryLock(K key) {
        return map.tryLock(key);
    }

    @Override
    public boolean tryLock(K key, long time, TimeUnit timeunit) throws InterruptedException {
        return map.tryLock(key, time, timeunit);
    }

    @Override
    public boolean tryLock(K key, long time, TimeUnit timeunit, long leaseTime,
                           TimeUnit leaseTimeunit) throws InterruptedException {
        return map.tryLock(key, time, timeunit, leaseTime, leaseTimeunit);
    }

    @Override
    public void unlock(K key) {
        map.unlock(key);
    }

    @Override
    public void forceUnlock(K key) {
        map.forceUnlock(key);
    }

    @Override
    public String addLocalEntryListener(MapListener listener) {
        return map.addLocalEntryListener(listener);
    }

    @Override
    public String addLocalEntryListener(EntryListener listener) {
        return map.addLocalEntryListener(listener);
    }

    @Override
    public String addLocalEntryListener(MapListener listener, Predicate<K, V> predicate, boolean includeValue) {
        return map.addLocalEntryListener(listener, predicate, includeValue);
    }

    @Override
    public String addLocalEntryListener(EntryListener listener, Predicate<K, V> predicate, boolean includeValue) {
        return map.addLocalEntryListener(listener, predicate, includeValue);
    }

    @Override
    public String addLocalEntryListener(MapListener listener, Predicate<K, V> predicate, K key, boolean includeValue) {
        return map.addLocalEntryListener(listener, predicate, key, includeValue);
    }

    @Override
    public String addLocalEntryListener(EntryListener listener, Predicate<K, V> predicate, K key,
                                        boolean includeValue) {
        return map.addLocalEntryListener(listener, predicate, key, includeValue);
    }

    @Override
    public String addInterceptor(MapInterceptor interceptor) {
        return map.addInterceptor(interceptor);
    }

    @Override
    public void removeInterceptor(String id) {
        map.removeInterceptor(id);
    }

    @Override
    public String addEntryListener(MapListener listener, boolean includeValue) {
        return map.addEntryListener(listener, includeValue);
    }

    @Override
    public String addEntryListener(EntryListener listener, boolean includeValue) {
        return map.addEntryListener(listener, includeValue);
    }

    @Override
    public boolean removeEntryListener(String id) {
        return map.removeEntryListener(id);
    }

    @Override
    public String addPartitionLostListener(MapPartitionLostListener listener) {
        return map.addPartitionLostListener(listener);
    }

    @Override
    public boolean removePartitionLostListener(String id) {
        return map.removePartitionLostListener(id);
    }

    @Override
    public String addEntryListener(MapListener listener, K key, boolean includeValue) {
        return map.addEntryListener(listener, key, includeValue);
    }

    @Override
    public String addEntryListener(EntryListener listener, K key, boolean includeValue) {
        return map.addEntryListener(listener, key, includeValue);
    }

    @Override
    public String addEntryListener(MapListener listener, Predicate<K, V> predicate, boolean includeValue) {
        return map.addEntryListener(listener, predicate, includeValue);
    }

    @Override
    public String addEntryListener(EntryListener listener, Predicate<K, V> predicate, boolean includeValue) {
        return map.addEntryListener(listener, predicate, includeValue);
    }

    @Override
    public String addEntryListener(MapListener listener, Predicate<K, V> predicate, K key, boolean includeValue) {
        return map.addEntryListener(listener, predicate, key, includeValue);
    }

    @Override
    public String addEntryListener(EntryListener listener, Predicate<K, V> predicate, K key, boolean includeValue) {
        return map.addEntryListener(listener, predicate, key, includeValue);
    }

    @Override
    public EntryView<K, V> getEntryView(K key) {
        return map.getEntryView(key);
    }

    @Override
    public boolean evict(K key) {
        return map.evict(key);
    }

    @Override
    public void evictAll() {
        map.evictAll();
    }

    @Override
    public Set<K> keySet() {
        return map.keySet();
    }

    @Override
    public Collection<V> values() {
        return map.values();
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return map.entrySet();
    }

    @Override
    public Set<K> keySet(Predicate predicate) {
        return map.keySet(predicate);
    }

    @Override
    public Set<Entry<K, V>> entrySet(Predicate predicate) {
        return map.entrySet(predicate);
    }

    @Override
    public Collection<V> values(Predicate predicate) {
        return map.values(predicate);
    }

    @Override
    public Set<K> localKeySet() {
        return map.localKeySet();
    }

    @Override
    public Set<K> localKeySet(Predicate predicate) {
        return map.localKeySet(predicate);
    }

    @Override
    public void addIndex(String attribute, boolean ordered) {
        map.addIndex(attribute, ordered);
    }

    @Override
    public LocalMapStats getLocalMapStats() {
        return map.getLocalMapStats();
    }

    @Override
    public Object executeOnKey(K key, EntryProcessor entryProcessor) {
        return map.executeOnKey(key, entryProcessor);
    }

    @Override
    public Map<K, Object> executeOnKeys(Set<K> keys, EntryProcessor entryProcessor) {
        return map.executeOnKeys(keys, entryProcessor);
    }

    @Override
    public void submitToKey(K key, EntryProcessor entryProcessor, ExecutionCallback callback) {
        map.submitToKey(key, entryProcessor, callback);
    }

    @Override
    public ICompletableFuture submitToKey(K key, EntryProcessor entryProcessor) {
        return map.submitToKey(key, entryProcessor);
    }

    @Override
    public Map<K, Object> executeOnEntries(EntryProcessor entryProcessor) {
        return map.executeOnEntries(entryProcessor);
    }

    @Override
    public Map<K, Object> executeOnEntries(EntryProcessor entryProcessor, Predicate predicate) {
        return map.executeOnEntries(entryProcessor, predicate);
    }

    @Override
    public <SuppliedValue, Result> Result aggregate(Supplier<K, V, SuppliedValue> supplier,
                                                    Aggregation<K, SuppliedValue, Result> aggregation) {
        return map.aggregate(supplier, aggregation);
    }

    @Override
    public <SuppliedValue, Result> Result aggregate(Supplier<K, V, SuppliedValue> supplier,
                                                    Aggregation<K, SuppliedValue, Result> aggregation,
                                                    JobTracker jobTracker) {
        return map.aggregate(supplier, aggregation, jobTracker);
    }

    @Override
    public String getPartitionKey() {
        return map.getPartitionKey();
    }

    @Override
    public String getName() {
        return map.getName();
    }

    @Override
    public String getServiceName() {
        return map.getServiceName();
    }

    @Override
    public void destroy() {
        map.destroy();
    }

    @Override
    public DistributedStream<Entry<K, V>> stream() {
        return new MapSourcePipeline<>(new StreamContext(instance), map);
    }
}
