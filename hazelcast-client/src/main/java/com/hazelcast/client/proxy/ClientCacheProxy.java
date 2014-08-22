/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.proxy;

import com.hazelcast.cache.ICache;
import com.hazelcast.client.cache.HazelcastClientCacheManager;
import com.hazelcast.client.nearcache.ClientNearCache;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.nio.serialization.Data;

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
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

public class ClientCacheProxy<K, V>  implements ICache<K, V> {
//WARNING:: this proxy do not extend ClientProxy because Cache and DistributedObject
// has getName method which have different values a distributedObject delegate used to over come this

    private static final String NULL_KEY_IS_NOT_ALLOWED = "Null key is not allowed!";
    private static final String NULL_VALUE_IS_NOT_ALLOWED = "Null value is not allowed!";
    private final CacheConfig<K, V> cacheConfig;

    //this will represent the name from the user perspective
    private String name;

    private boolean isClosed = false;
    private ClientProxy delegate;
    private HazelcastClientCacheManager cacheManager;

//    private CacheLoader<K, V> cacheLoader;

    private volatile ClientNearCache<Data> nearCache;
    private final AtomicBoolean nearCacheInitialized = new AtomicBoolean();

    public ClientCacheProxy(String name, CacheConfig<K, V> cacheConfig, ClientProxy delegate, HazelcastClientCacheManager cacheManager) {
        this.name = name;
        this.cacheConfig = cacheConfig;
        this.delegate = delegate;
        this.cacheManager = cacheManager;

        //FIXME DO WE NEED A CACHE LOADER HERE
//        if (cacheConfig.getCacheLoaderFactory() != null) {
//            final Factory<CacheLoader> cacheLoaderFactory = cacheConfig.getCacheLoaderFactory();
//            cacheLoader = cacheLoaderFactory.create();
//        }
    }
    //region ICACHE imple
    @Override
    public Future<V> getAsync(K key) {
        return null;
    }

    @Override
    public Future<V> getAsync(K key, ExpiryPolicy expiryPolicy) {
        return null;
    }

    @Override
    public Future<Void> putAsync(K key, V value) {
        return null;
    }

    @Override
    public Future<Void> putAsync(K key, V value, ExpiryPolicy expiryPolicy) {
        return null;
    }

    @Override
    public Future<Boolean> putIfAbsentAsync(K key, V value, ExpiryPolicy expiryPolicy) {
        return null;
    }

    @Override
    public Future<V> getAndPutAsync(K key, V value) {
        return null;
    }

    @Override
    public Future<V> getAndPutAsync(K key, V value, ExpiryPolicy expiryPolicy) {
        return null;
    }

    @Override
    public Future<Boolean> removeAsync(K key) {
        return null;
    }

    @Override
    public Future<Boolean> removeAsync(K key, V oldValue) {
        return null;
    }

    @Override
    public Future<V> getAndRemoveAsync(K key) {
        return null;
    }

    @Override
    public Future<Boolean> replaceAsync(K key, V oldValue, V newValue) {
        return null;
    }

    @Override
    public Future<Boolean> replaceAsync(K key, V oldValue, V newValue, ExpiryPolicy expiryPolicy) {
        return null;
    }

    @Override
    public Future<V> getAndReplaceAsync(K key, V value) {
        return null;
    }

    @Override
    public Future<V> getAndReplaceAsync(K key, V value, ExpiryPolicy expiryPolicy) {
        return null;
    }

    @Override
    public V get(K key, ExpiryPolicy expiryPolicy) {
        return null;
    }

    @Override
    public Map<K, V> getAll(Set<? extends K> keys, ExpiryPolicy expiryPolicy) {
        return null;
    }

    @Override
    public void put(K key, V value, ExpiryPolicy expiryPolicy) {

    }

    @Override
    public V getAndPut(K key, V value, ExpiryPolicy expiryPolicy) {
        return null;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map, ExpiryPolicy expiryPolicy) {

    }

    @Override
    public boolean putIfAbsent(K key, V value, ExpiryPolicy expiryPolicy) {
        return false;
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue, ExpiryPolicy expiryPolicy) {
        return false;
    }

    @Override
    public boolean replace(K key, V value, ExpiryPolicy expiryPolicy) {
        return false;
    }

    @Override
    public V getAndReplace(K key, V value, ExpiryPolicy expiryPolicy) {
        return null;
    }

    @Override
    public int size() {
        return 0;
    }
    //endregion

    //region JAVAX.CACHE impl
    @Override
    public V get(K key) {
        return null;
    }

    @Override
    public Map<K, V> getAll(Set<? extends K> keys) {
        return null;
    }

    @Override
    public boolean containsKey(K key) {
        return false;
    }

    @Override
    public void loadAll(Set<? extends K> keys, boolean replaceExistingValues, CompletionListener completionListener) {

    }

    @Override
    public void put(K key, V value) {

    }

    @Override
    public V getAndPut(K key, V value) {
        return null;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {

    }

    @Override
    public boolean putIfAbsent(K key, V value) {
        return false;
    }

    @Override
    public boolean remove(K key) {
        return false;
    }

    @Override
    public boolean remove(K key, V oldValue) {
        return false;
    }

    @Override
    public V getAndRemove(K key) {
        return null;
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        return false;
    }

    @Override
    public boolean replace(K key, V value) {
        return false;
    }

    @Override
    public V getAndReplace(K key, V value) {
        return null;
    }

    @Override
    public void removeAll(Set<? extends K> keys) {

    }

    @Override
    public void removeAll() {

    }

    @Override
    public void clear() {

    }

    @Override
    public <C extends Configuration<K, V>> C getConfiguration(Class<C> clazz) {
        return null;
    }

    @Override
    public <T> T invoke(K key, EntryProcessor<K, V, T> entryProcessor, Object... arguments) throws EntryProcessorException {
        return null;
    }

    @Override
    public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys, EntryProcessor<K, V, T> entryProcessor, Object... arguments) {
        return null;
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public CacheManager getCacheManager() {
        return null;
    }

    @Override
    public void close() {

    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        return null;
    }

    @Override
    public void registerCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {

    }

    @Override
    public void deregisterCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {

    }

    @Override
    public Iterator<Entry<K, V>> iterator() {
        return null;
    }
    //endregion

}
