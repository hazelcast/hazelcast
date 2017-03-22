/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.ClientProxyFactory;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.NearCacheConfig;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ClientCacheProxyFactory implements ClientProxyFactory {

    private final HazelcastClientInstanceImpl client;
    private final ConcurrentMap<String, CacheConfig> configs = new ConcurrentHashMap<String, CacheConfig>();

    public ClientCacheProxyFactory(HazelcastClientInstanceImpl client) {
        this.client = client;
    }

    @Override
    public ClientProxy create(String id) {
        CacheConfig cacheConfig = findCacheConfig(id);
        if (cacheConfig == null) {
            throw new CacheNotExistsException("Cache " + id + " is already destroyed or not created yet");
        }
        NearCacheConfig nearCacheConfig = client.getClientConfig().getNearCacheConfig(cacheConfig.getName());
        if (nearCacheConfig != null) {
            return new NearCachedClientCacheProxy(cacheConfig);
        }

        return new ClientCacheProxy(cacheConfig);
    }

    @SuppressFBWarnings("RV_RETURN_VALUE_OF_PUTIFABSENT_IGNORED")
    void addCacheConfig(String cacheName, CacheConfig cacheConfig) {
        configs.putIfAbsent(cacheName, cacheConfig);
    }

    void removeCacheConfig(String cacheName) {
        configs.remove(cacheName);
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
