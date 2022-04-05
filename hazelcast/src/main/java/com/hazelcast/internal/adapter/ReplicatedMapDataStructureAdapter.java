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

package com.hazelcast.internal.adapter;

import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.map.LocalMapStats;
import com.hazelcast.query.Predicate;

import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.util.MapUtil.createHashMap;

@SuppressWarnings("checkstyle:methodcount")
public class ReplicatedMapDataStructureAdapter<K, V> implements DataStructureAdapter<K, V> {

    private final ReplicatedMap<K, V> map;

    public ReplicatedMapDataStructureAdapter(ReplicatedMap<K, V> map) {
        this.map = map;
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public V get(K key) {
        return map.get(key);
    }

    @Override
    @MethodNotAvailable
    public CompletionStage<V> getAsync(K key) {
        throw new MethodNotAvailableException();
    }

    @Override
    @MethodNotAvailable
    public void set(K key, V value) {
        throw new MethodNotAvailableException();
    }

    @Override
    @MethodNotAvailable
    public CompletionStage<Void> setAsync(K key, V value) {
        throw new MethodNotAvailableException();
    }

    @Override
    @MethodNotAvailable
    public CompletionStage<Void> setAsync(K key, V value, long ttl, TimeUnit timeunit) {
        throw new MethodNotAvailableException();
    }

    @Override
    @MethodNotAvailable
    public CompletionStage<Void> setAsync(K key, V value, ExpiryPolicy expiryPolicy) {
        throw new MethodNotAvailableException();
    }

    @Override
    public V put(K key, V value) {
        return map.put(key, value);
    }

    @Override
    @MethodNotAvailable
    public CompletionStage<V> putAsync(K key, V value) {
        throw new MethodNotAvailableException();
    }

    @Override
    @MethodNotAvailable
    public CompletionStage<V> putAsync(K key, V value, long ttl, TimeUnit timeunit) {
        throw new MethodNotAvailableException();
    }

    @Override
    @MethodNotAvailable
    public CompletionStage<V> putAsync(K key, V value, ExpiryPolicy expiryPolicy) {
        throw new MethodNotAvailableException();
    }

    @Override
    @MethodNotAvailable
    public void putTransient(K key, V value, long ttl, TimeUnit timeunit) {
        throw new MethodNotAvailableException();
    }

    @Override
    @MethodNotAvailable
    public boolean putIfAbsent(K key, V value) {
        throw new MethodNotAvailableException();
    }

    @Override
    @MethodNotAvailable
    public CompletionStage<Boolean> putIfAbsentAsync(K key, V value) {
        throw new MethodNotAvailableException();
    }

    @Override
    @MethodNotAvailable
    public void setTtl(K key, long duration, TimeUnit timeUnit) {
        throw new MethodNotAvailableException();
    }

    @Override
    @MethodNotAvailable
    public V replace(K key, V newValue) {
        throw new MethodNotAvailableException();
    }

    @Override
    @MethodNotAvailable
    public boolean replace(K key, V oldValue, V newValue) {
        throw new MethodNotAvailableException();
    }

    public V remove(K key) {
        return map.remove(key);
    }

    @Override
    @MethodNotAvailable
    public boolean remove(K key, V oldValue) {
        throw new MethodNotAvailableException();
    }

    @Override
    @MethodNotAvailable
    public CompletionStage<V> removeAsync(K key) {
        throw new MethodNotAvailableException();
    }

    @Override
    @MethodNotAvailable
    public void delete(K key) {
        throw new MethodNotAvailableException();
    }

    @Override
    @MethodNotAvailable
    public CompletionStage<Boolean> deleteAsync(K key) {
        throw new MethodNotAvailableException();
    }

    @Override
    @MethodNotAvailable
    public boolean evict(K key) {
        throw new MethodNotAvailableException();
    }

    @Override
    @MethodNotAvailable
    public <T> T invoke(K key, EntryProcessor<K, V, T> entryProcessor, Object... arguments) throws EntryProcessorException {
        throw new MethodNotAvailableException();
    }

    @Override
    @MethodNotAvailable
    public Object executeOnKey(K key, com.hazelcast.map.EntryProcessor entryProcessor) {
        throw new MethodNotAvailableException();
    }

    @Override
    @MethodNotAvailable
    public Map<K, Object> executeOnKeys(Set<K> keys, com.hazelcast.map.EntryProcessor entryProcessor) {
        throw new MethodNotAvailableException();
    }

    @Override
    @MethodNotAvailable
    public Map<K, Object> executeOnEntries(com.hazelcast.map.EntryProcessor entryProcessor) {
        throw new MethodNotAvailableException();
    }

    @Override
    @MethodNotAvailable
    public Map<K, Object> executeOnEntries(com.hazelcast.map.EntryProcessor entryProcessor, Predicate predicate) {
        throw new MethodNotAvailableException();
    }

    @Override
    public boolean containsKey(K key) {
        return map.containsKey(key);
    }

    @Override
    @MethodNotAvailable
    public void loadAll(boolean replaceExistingValues) {
        throw new MethodNotAvailableException();
    }

    @Override
    @MethodNotAvailable
    public void loadAll(Set<K> keys, boolean replaceExistingValues) {
        throw new MethodNotAvailableException();
    }

    @Override
    @MethodNotAvailable
    public void loadAll(Set<? extends K> keys, boolean replaceExistingValues, CompletionListener completionListener) {
        throw new MethodNotAvailableException();
    }

    @Override
    public Map<K, V> getAll(Set<K> keys) {
        Map<K, V> result = createHashMap(keys.size());
        for (K key : keys) {
            result.put(key, map.get(key));
        }
        return result;
    }

    @Override
    public void putAll(Map<K, V> map) {
        this.map.putAll(map);
    }

    @Override
    @MethodNotAvailable
    public void removeAll() {
        throw new MethodNotAvailableException();
    }

    @Override
    @MethodNotAvailable
    public void removeAll(Set<K> keys) {
        throw new MethodNotAvailableException();
    }

    @Override
    @MethodNotAvailable
    public void evictAll() {
        throw new MethodNotAvailableException();
    }

    @Override
    @MethodNotAvailable
    public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys, EntryProcessor<K, V, T> entryProcessor,
                                                         Object... arguments) {
        throw new MethodNotAvailableException();
    }

    @Override
    public void clear() {
        map.clear();
    }

    @Override
    @MethodNotAvailable
    public void close() {
        throw new MethodNotAvailableException();
    }

    @Override
    public void destroy() {
        map.destroy();
    }

    @Override
    @MethodNotAvailable
    public void setExpiryPolicy(Set<K> keys, ExpiryPolicy expiryPolicy) {
        throw new MethodNotAvailableException();
    }

    @Override
    @MethodNotAvailable
    public boolean setExpiryPolicy(K key, ExpiryPolicy expiryPolicy) {
        throw new MethodNotAvailableException();
    }

    @Override
    @MethodNotAvailable
    public LocalMapStats getLocalMapStats() {
        throw new MethodNotAvailableException();
    }
}
