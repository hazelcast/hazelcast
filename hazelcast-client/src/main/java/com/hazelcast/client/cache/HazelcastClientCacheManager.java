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

package com.hazelcast.client.cache;

import com.hazelcast.cache.HazelcastCacheManager;
import com.hazelcast.cache.ICache;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;

import javax.cache.configuration.Configuration;
import java.net.URI;
import java.util.Properties;

public final class HazelcastClientCacheManager extends HazelcastCacheManager {

    public HazelcastClientCacheManager(HazelcastClientCachingProvider cachingProvider, HazelcastInstance hazelcastInstance, URI uri, ClassLoader classLoader, Properties properties) {
        super(cachingProvider, uri, classLoader, properties);
        if (hazelcastInstance == null) {
            throw new NullPointerException("hazelcastInstance missing");
        }
        this.hazelcastInstance = hazelcastInstance;

    }

    @Override
    public void enableManagement(String cacheName, boolean enabled) {

    }

    @Override
    public void enableStatistics(String cacheName, boolean enabled) {

    }

    @Override
    protected <K, V, C extends Configuration<K, V>> CacheConfig<K, V> createCacheConfig(String cacheName, C configuration) {
        return null;
    }

    @Override
    protected <K, V> CacheConfig<K, V> getCacheConfigLocal(String cacheName) {
        return null;
    }

    @Override
    protected <K, V> boolean createConfigOnPartition(CacheConfig<K, V> cacheConfig) {
        return false;
    }

    @Override
    protected <K, V> void addCacheConfigIfAbsentToLocal(CacheConfig<K, V> cacheConfig) {

    }

    @Override
    protected <K, V> void createConfigOnAllMembers(CacheConfig<K, V> cacheConfig) {

    }

    @Override
    protected <K, V> ICache<K, V> createCacheProxy(CacheConfig<K, V> cacheConfig) {
        return null;
    }

    @Override
    protected <K, V> CacheConfig<K, V> getCacheConfigFromPartition(String cacheName) {
        return null;
    }

    @Override
    protected <K, V> void registerListeners(CacheConfig<K, V> cacheConfig, ICache<K, V> source) {

    }
}
