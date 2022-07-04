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

package com.hazelcast.jet.impl;

import com.hazelcast.cache.CacheStatistics;
import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.event.CachePartitionLostListener;
import com.hazelcast.jet.JetInstance;

import javax.cache.CacheManager;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

@SuppressWarnings({"checkstyle:methodcount", "deprecation"})
public class ICacheDecorator<K, V> implements ICache<K, V> {

    private final ICache<K, V> cache;
    private final JetInstance instance;

    public ICacheDecorator(ICache<K, V> cache, JetInstance instance) {
        this.cache = cache;
        this.instance = instance;
    }

    public JetInstance getInstance() {
        return instance;
    }

    // ICache decorated methods

    @Override
    public boolean setExpiryPolicy(K key, ExpiryPolicy expiryPolicy) {
        return cache.setExpiryPolicy(key, expiryPolicy);
    }

    @Override
    public void setExpiryPolicy(Set<? extends K> keys, ExpiryPolicy expiryPolicy) {
        cache.setExpiryPolicy(keys, expiryPolicy);
    }

    @Override
    public CompletionStage<V> getAsync(K key) {
        return cache.getAsync(key);
    }

    @Override
    public CompletionStage<V> getAsync(K key, ExpiryPolicy expiryPolicy) {
        return cache.getAsync(key, expiryPolicy);
    }

    @Override
    public CompletionStage<Void> putAsync(K key, V value) {
        return cache.putAsync(key, value);
    }

    @Override
    public CompletionStage<Void> putAsync(K key, V value, ExpiryPolicy expiryPolicy) {
        return cache.putAsync(key, value, expiryPolicy);
    }

    @Override
    public CompletionStage<Boolean> putIfAbsentAsync(K key, V value) {
        return cache.putIfAbsentAsync(key, value);
    }

    @Override
    public CompletionStage<Boolean> putIfAbsentAsync(K key, V value, ExpiryPolicy expiryPolicy) {
        return cache.putIfAbsentAsync(key, value, expiryPolicy);
    }

    @Override
    public CompletionStage<V> getAndPutAsync(K key, V value) {
        return cache.getAndPutAsync(key, value);
    }

    @Override
    public CompletionStage<V> getAndPutAsync(K key, V value, ExpiryPolicy expiryPolicy) {
        return cache.getAndPutAsync(key, value, expiryPolicy);
    }

    @Override
    public CompletionStage<Boolean> removeAsync(K key) {
        return cache.removeAsync(key);
    }

    @Override
    public CompletionStage<Boolean> removeAsync(K key, V oldValue) {
        return cache.removeAsync(key, oldValue);
    }

    @Override
    public CompletionStage<V> getAndRemoveAsync(K key) {
        return cache.getAndRemoveAsync(key);
    }

    @Override
    public CompletionStage<Boolean> replaceAsync(K key, V value) {
        return cache.replaceAsync(key, value);
    }

    @Override
    public CompletionStage<Boolean> replaceAsync(K key, V value, ExpiryPolicy expiryPolicy) {
        return cache.replaceAsync(key, value, expiryPolicy);
    }

    @Override
    public CompletionStage<Boolean> replaceAsync(K key, V oldValue, V newValue) {
        return cache.replaceAsync(key, oldValue, newValue);
    }

    @Override
    public CompletionStage<Boolean> replaceAsync(K key, V oldValue, V newValue, ExpiryPolicy expiryPolicy) {
        return cache.replaceAsync(key, oldValue, newValue, expiryPolicy);
    }

    @Override
    public CompletionStage<V> getAndReplaceAsync(K key, V value) {
        return cache.getAndReplaceAsync(key, value);
    }

    @Override
    public CompletionStage<V> getAndReplaceAsync(K key, V value, ExpiryPolicy expiryPolicy) {
        return cache.getAndReplaceAsync(key, value, expiryPolicy);
    }

    @Override
    public V get(K key, ExpiryPolicy expiryPolicy) {
        return cache.get(key, expiryPolicy);
    }

    @Override
    public Map<K, V> getAll(Set<? extends K> keys, ExpiryPolicy expiryPolicy) {
        return cache.getAll(keys, expiryPolicy);
    }

    @Override
    public void put(K key, V value, ExpiryPolicy expiryPolicy) {
        cache.put(key, value, expiryPolicy);
    }

    @Override
    public V getAndPut(K key, V value, ExpiryPolicy expiryPolicy) {
        return cache.getAndPut(key, value, expiryPolicy);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map, ExpiryPolicy expiryPolicy) {
        cache.putAll(map, expiryPolicy);
    }

    @Override
    public boolean putIfAbsent(K key, V value, ExpiryPolicy expiryPolicy) {
        return cache.putIfAbsent(key, value, expiryPolicy);
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue, ExpiryPolicy expiryPolicy) {
        return cache.replace(key, oldValue, newValue, expiryPolicy);
    }

    @Override
    public boolean replace(K key, V value, ExpiryPolicy expiryPolicy) {
        return cache.replace(key, value, expiryPolicy);
    }

    @Override
    public V getAndReplace(K key, V value, ExpiryPolicy expiryPolicy) {
        return cache.getAndReplace(key, value, expiryPolicy);
    }

    @Override
    public int size() {
        return cache.size();
    }

    @Override
    public void destroy() {
        cache.destroy();
    }

    @Override
    public boolean isDestroyed() {
        return cache.isDestroyed();
    }

    @Override
    public CacheStatistics getLocalCacheStatistics() {
        return cache.getLocalCacheStatistics();
    }

    @Override
    public UUID addPartitionLostListener(CachePartitionLostListener listener) {
        return cache.addPartitionLostListener(listener);
    }

    @Override
    public boolean removePartitionLostListener(UUID uuid) {
        return cache.removePartitionLostListener(uuid);
    }

    @Override
    public Iterator<Entry<K, V>> iterator(int fetchSize) {
        return cache.iterator(fetchSize);
    }

    @Override
    public V get(K k) {
        return cache.get(k);
    }

    @Override
    public Map<K, V> getAll(Set<? extends K> set) {
        return cache.getAll(set);
    }

    @Override
    public boolean containsKey(K k) {
        return cache.containsKey(k);
    }

    @Override
    public void loadAll(Set<? extends K> set, boolean b, CompletionListener completionListener) {
        cache.loadAll(set, b, completionListener);
    }

    @Override
    public void put(K k, V v) {
        cache.put(k, v);
    }

    @Override
    public V getAndPut(K k, V v) {
        return cache.getAndPut(k, v);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        cache.putAll(map);
    }

    @Override
    public boolean putIfAbsent(K k, V v) {
        return cache.putIfAbsent(k, v);
    }

    @Override
    public boolean remove(K k) {
        return cache.remove(k);
    }

    @Override
    public boolean remove(K k, V v) {
        return cache.remove(k, v);
    }

    @Override
    public V getAndRemove(K k) {
        return cache.getAndRemove(k);
    }

    @Override
    public boolean replace(K k, V v, V v1) {
        return cache.replace(k, v, v1);
    }

    @Override
    public boolean replace(K k, V v) {
        return cache.replace(k, v);
    }

    @Override
    public V getAndReplace(K k, V v) {
        return cache.getAndReplace(k, v);
    }

    @Override
    public void removeAll(Set<? extends K> set) {
        cache.removeAll(set);
    }

    @Override
    public void removeAll() {
        cache.removeAll();
    }

    @Override
    public void clear() {
        cache.clear();
    }

    @Override
    public <C extends Configuration<K, V>> C getConfiguration(Class<C> aClass) {
        return cache.getConfiguration(aClass);
    }

    @Override
    public <T> T invoke(K k, EntryProcessor<K, V, T> entryProcessor, Object... objects) throws EntryProcessorException {
        return cache.invoke(k, entryProcessor, objects);
    }

    @Override
    public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> set,
                                                         EntryProcessor<K, V, T> entryProcessor,
                                                         Object... objects) {
        return cache.invokeAll(set, entryProcessor, objects);
    }

    @Override
    public String getName() {
        return cache.getName();
    }

    @Override
    public CacheManager getCacheManager() {
        return cache.getCacheManager();
    }

    @Override
    public void close() {
        cache.close();
    }

    @Override
    public boolean isClosed() {
        return cache.isClosed();
    }

    @Override
    public <T> T unwrap(Class<T> aClass) {
        return cache.unwrap(aClass);
    }

    @Override
    public void registerCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        cache.registerCacheEntryListener(cacheEntryListenerConfiguration);
    }

    @Override
    public void deregisterCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        cache.deregisterCacheEntryListener(cacheEntryListenerConfiguration);
    }

    @Override
    public Iterator<Entry<K, V>> iterator() {
        return cache.iterator();
    }

    @Override
    public void forEach(Consumer<? super Entry<K, V>> action) {
        cache.forEach(action);
    }

    @Override
    public Spliterator<Entry<K, V>> spliterator() {
        return cache.spliterator();
    }

    @Override
    public String getPrefixedName() {
        return cache.getPrefixedName();
    }

    @Override
    public String getPartitionKey() {
        return cache.getPartitionKey();
    }

    @Override
    public String getServiceName() {
        return cache.getServiceName();
    }
}
