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

import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.map.LocalMapStats;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;

import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("checkstyle:methodcount")
public class IMapDataStructureAdapter<K, V> implements DataStructureAdapter<K, V> {

    private final IMap<K, V> map;

    public IMapDataStructureAdapter(IMap<K, V> map) {
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
    public CompletionStage<V> getAsync(K key) {
        return map.getAsync(key);
    }

    @Override
    public void set(K key, V value) {
        map.set(key, value);
    }

    @Override
    public CompletionStage<Void> setAsync(K key, V value) {
        return map.setAsync(key, value);
    }

    @Override
    public CompletionStage<Void> setAsync(K key, V value, long ttl, TimeUnit timeunit) {
        return map.setAsync(key, value, ttl, timeunit);
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
    public CompletionStage<V> putAsync(K key, V value) {
        return map.putAsync(key, value);
    }

    @Override
    public CompletionStage<V> putAsync(K key, V value, long ttl, TimeUnit timeunit) {
        return map.putAsync(key, value, ttl, timeunit);
    }

    @Override
    @MethodNotAvailable
    public CompletionStage<V> putAsync(K key, V value, ExpiryPolicy expiryPolicy) {
        throw new MethodNotAvailableException();
    }

    @Override
    public void putTransient(K key, V value, long ttl, TimeUnit timeunit) {
        map.putTransient(key, value, ttl, timeunit);
    }

    @Override
    public boolean putIfAbsent(K key, V value) {
        return map.putIfAbsent(key, value) == null;
    }

    @Override
    @MethodNotAvailable
    public CompletionStage<Boolean> putIfAbsentAsync(K key, V value) {
        throw new MethodNotAvailableException();
    }

    @Override
    public void setTtl(K key, long duration, TimeUnit timeUnit) {
        map.setTtl(key, duration, timeUnit);
    }

    @Override
    public V replace(K key, V newValue) {
        return map.replace(key, newValue);
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        return map.replace(key, oldValue, newValue);
    }

    @Override
    public V remove(K key) {
        return map.remove(key);
    }

    @Override
    public boolean remove(K key, V oldValue) {
        return map.remove(key, oldValue);
    }

    @Override
    public CompletionStage<V> removeAsync(K key) {
        return map.removeAsync(key);
    }

    @Override
    public void delete(K key) {
        map.delete(key);
    }

    @Override
    @MethodNotAvailable
    public CompletionStage<Boolean> deleteAsync(K key) {
        throw new MethodNotAvailableException();
    }

    @Override
    public boolean evict(K key) {
        return map.evict(key);
    }

    @Override
    @MethodNotAvailable
    public <T> T invoke(K key, EntryProcessor<K, V, T> entryProcessor, Object... arguments) throws EntryProcessorException {
        throw new MethodNotAvailableException();
    }

    @Override
    public Object executeOnKey(K key, com.hazelcast.map.EntryProcessor entryProcessor) {
        return map.executeOnKey(key, entryProcessor);
    }

    @Override
    public Map<K, Object> executeOnKeys(Set<K> keys, com.hazelcast.map.EntryProcessor entryProcessor) {
        return map.executeOnKeys(keys, entryProcessor);
    }

    @Override
    public Map<K, Object> executeOnEntries(com.hazelcast.map.EntryProcessor entryProcessor) {
        return map.executeOnEntries(entryProcessor);
    }

    @Override
    public Map<K, Object> executeOnEntries(com.hazelcast.map.EntryProcessor entryProcessor, Predicate predicate) {
        return map.executeOnEntries(entryProcessor, predicate);
    }

    @Override
    public boolean containsKey(K key) {
        return map.containsKey(key);
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
    @MethodNotAvailable
    public void loadAll(Set<? extends K> keys, boolean replaceExistingValues, CompletionListener completionListener) {
        throw new MethodNotAvailableException();
    }

    @Override
    public Map<K, V> getAll(Set<K> keys) {
        return map.getAll(keys);
    }

    @Override
    public void putAll(Map<K, V> map) {
        this.map.putAll(map);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void removeAll() {
        map.removeAll(Predicates.alwaysTrue());
    }

    @Override
    @MethodNotAvailable
    public void removeAll(final Set<K> keys) {
        throw new MethodNotAvailableException();
    }

    @Override
    public void evictAll() {
        map.evictAll();
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
    public LocalMapStats getLocalMapStats() {
        return map.getLocalMapStats();
    }

    public void waitUntilLoaded() {
        if (map instanceof MapProxyImpl) {
            ((MapProxyImpl) map).waitUntilLoaded();
        }
    }
}
