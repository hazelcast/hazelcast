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

package com.hazelcast.client.cache.impl;

import com.hazelcast.cache.CacheNotExistsException;
import com.hazelcast.cache.HazelcastCacheManager;
import com.hazelcast.client.cache.impl.nearcache.NearCachedClientCacheProxy;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.spi.ClientContext;
import com.hazelcast.client.impl.spi.ClientProxy;
import com.hazelcast.client.impl.spi.ClientProxyFactory;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.NearCacheConfig;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ClientCacheProxyFactory implements ClientProxyFactory {

    private final HazelcastClientInstanceImpl client;
    private final ConcurrentMap<String, CacheConfig> configs = new ConcurrentHashMap<String, CacheConfig>();

    public ClientCacheProxyFactory(HazelcastClientInstanceImpl client) {
        this.client = client;
    }

    @Override
    public ClientProxy create(String id, ClientContext context) {
        CacheConfig cacheConfig = findCacheConfig(id);
        if (cacheConfig == null) {
            throw new CacheNotExistsException("Cache " + id + " is already destroyed or not created yet");
        }
        NearCacheConfig nearCacheConfig = client.getClientConfig().getNearCacheConfig(cacheConfig.getName());
        if (nearCacheConfig != null) {
            return new NearCachedClientCacheProxy(cacheConfig, context);
        }
        return new ClientCacheProxy(cacheConfig, context);
    }

    public void recreateCachesOnCluster() {
        for (CacheConfig cacheConfig : configs.values()) {
            ClientCacheHelper.createCacheConfig(client, cacheConfig, true);
        }
    }

    @SuppressFBWarnings("RV_RETURN_VALUE_OF_PUTIFABSENT_IGNORED")
    void addCacheConfig(String cacheName, CacheConfig cacheConfig) {
        configs.putIfAbsent(cacheName, cacheConfig);
    }

    void removeCacheConfig(String cacheName) {
        configs.remove(cacheName);
    }

    CacheConfig getCacheConfig(String cacheName) {
        return configs.get(cacheName);
    }

    Set<Map.Entry<String, CacheConfig>> configs() {
        return configs.entrySet();
    }

    @SuppressFBWarnings("RV_RETURN_VALUE_OF_PUTIFABSENT_IGNORED")
    private CacheConfig findCacheConfig(String cacheName) {
        CacheConfig cacheConfig = configs.get(cacheName);
        if (cacheConfig != null) {
            return cacheConfig;
        }
        // otherwise, request it from server
        String simpleCacheName = cacheName.substring(HazelcastCacheManager.CACHE_MANAGER_PREFIX.length());
        cacheConfig = ClientCacheHelper.getCacheConfig(client, cacheName, simpleCacheName);
        if (cacheConfig != null) {
            configs.putIfAbsent(cacheName, cacheConfig);
        }
        return cacheConfig;
    }
}
