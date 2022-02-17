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

import com.hazelcast.cache.ICache;
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

@SuppressWarnings("checkstyle:methodcount")
public class ICacheDataStructureAdapter<K, V> implements DataStructureAdapter<K, V> {

    private final ICache<K, V> cache;

    public ICacheDataStructureAdapter(ICache<K, V> cache) {
        this.cache = cache;
    }

    @Override
    public int size() {
        return cache.size();
    }

    @Override
    public V get(K key) {
        return cache.get(key);
    }

    @Override
    public CompletionStage<V> getAsync(K key) {
        return cache.getAsync(key);
    }

    @Override
    public void set(K key, V value) {
        cache.put(key, value);
    }

    @Override
    public CompletionStage<Void> setAsync(K key, V value) {
        return cache.putAsync(key, value);
    }

    @Override
    @MethodNotAvailable
    public CompletionStage<Void> setAsync(K key, V value, long ttl, TimeUnit timeunit) {
        throw new MethodNotAvailableException();
    }

    @Override
    public CompletionStage<Void> setAsync(K key, V value, ExpiryPolicy expiryPolicy) {
        return cache.putAsync(key, value, expiryPolicy);
    }

    @Override
    public V put(K key, V value) {
        return cache.getAndPut(key, value);
    }

    @Override
    public CompletionStage<V> putAsync(K key, V value) {
        return cache.getAndPutAsync(key, value);
    }

    @Override
    @MethodNotAvailable
    public CompletionStage<V> putAsync(K key, V value, long time, TimeUnit unit) {
        throw new MethodNotAvailableException();
    }

    @Override
    public CompletionStage<V> putAsync(K key, V value, ExpiryPolicy expiryPolicy) {
        return cache.getAndPutAsync(key, value, expiryPolicy);
    }

    @Override
    @MethodNotAvailable
    public void putTransient(K key, V value, long ttl, TimeUnit timeunit) {
        throw new MethodNotAvailableException();
    }

    @Override
    public boolean putIfAbsent(K key, V value) {
        return cache.putIfAbsent(key, value);
    }

    @Override
    public CompletionStage<Boolean> putIfAbsentAsync(K key, V value) {
        return cache.putIfAbsentAsync(key, value);
    }

    @Override
    @MethodNotAvailable
    public void setTtl(K key, long duration, TimeUnit timeUnit) {
        throw new MethodNotAvailableException();
    }

    @Override
    public V replace(K key, V newValue) {
        return cache.getAndReplace(key, newValue);
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        return cache.replace(key, oldValue, newValue);
    }

    @Override
    public V getAndReplace(K key, V value) {
        return cache.getAndReplace(key, value);
    }

    @Override
    public CompletionStage<V> getAndReplaceAsync(K key, V value) {
        return cache.getAndReplaceAsync(key, value);
    }

    @Override
    public V remove(K key) {
        return cache.getAndRemove(key);
    }

    @Override
    public boolean remove(K key, V oldValue) {
        return cache.remove(key, oldValue);
    }

    @Override
    public CompletionStage<V> removeAsync(K key) {
        return cache.getAndRemoveAsync(key);
    }

    @Override
    public V getAndRemove(K key) {
        return cache.getAndRemove(key);
    }

    @Override
    public CompletionStage<V> getAndRemoveAsync(K key) {
        return cache.getAndRemoveAsync(key);
    }

    @Override
    public void delete(K key) {
        cache.remove(key);
    }

    @Override
    public CompletionStage<Boolean> deleteAsync(K key) {
        return cache.removeAsync(key);
    }

    @Override
    @MethodNotAvailable
    public boolean evict(K key) {
        throw new MethodNotAvailableException();
    }

    @Override
    public <T> T invoke(K key, EntryProcessor<K, V, T> entryProcessor, Object... arguments) throws EntryProcessorException {
        return cache.invoke(key, entryProcessor, arguments);
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
        return cache.containsKey(key);
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
    public void loadAll(Set<? extends K> keys, boolean replaceExistingValues, CompletionListener completionListener) {
        cache.loadAll(keys, replaceExistingValues, completionListener);
    }

    @Override
    public Map<K, V> getAll(Set<K> keys) {
        return cache.getAll(keys);
    }

    @Override
    public void putAll(Map<K, V> map) {
        cache.putAll(map);
    }

    @Override
    public void removeAll() {
        cache.removeAll();
    }

    @Override
    public void removeAll(Set<K> keys) {
        cache.removeAll(keys);
    }

    @Override
    @MethodNotAvailable
    public void evictAll() {
        throw new MethodNotAvailableException();
    }

    @Override
    public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys, EntryProcessor<K, V, T> entryProcessor,
                                                         Object... arguments) {
        return cache.invokeAll(keys, entryProcessor, arguments);
    }

    @Override
    public void clear() {
        cache.clear();
    }

    @Override
    public void close() {
        cache.close();
    }

    @Override
    public void destroy() {
        cache.destroy();
    }

    @Override
    public void setExpiryPolicy(Set<K> keys, ExpiryPolicy expiryPolicy) {
        cache.setExpiryPolicy(keys, expiryPolicy);
    }

    @Override
    public boolean setExpiryPolicy(K key, ExpiryPolicy expiryPolicy) {
        return cache.setExpiryPolicy(key, expiryPolicy);
    }

    @Override
    @MethodNotAvailable
    public LocalMapStats getLocalMapStats() {
        throw new MethodNotAvailableException();
    }
}
