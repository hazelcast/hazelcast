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

import com.hazelcast.cache.HazelcastCachingProvider;
import com.hazelcast.cache.impl.AbstractHazelcastCacheManager;
import com.hazelcast.cache.impl.ICacheInternal;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.client.impl.clientside.ClientICacheManager;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.impl.spi.ProxyManager;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.net.URI;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import static com.hazelcast.internal.config.ConfigValidator.checkCacheConfig;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * {@link javax.cache.CacheManager} implementation for client side.
 * <p>
 * Provides client side CacheManager functionality.
 */
public final class HazelcastClientCacheManager extends AbstractHazelcastCacheManager {

    private final HazelcastClientInstanceImpl client;
    private final ClientCacheProxyFactory clientCacheProxyFactory;

    public HazelcastClientCacheManager(HazelcastClientCachingProvider cachingProvider, HazelcastInstance hazelcastInstance,
                                       URI uri, ClassLoader classLoader, Properties properties) {
        super(cachingProvider, hazelcastInstance, uri, classLoader, properties);

        /*
         * TODO:
         *
         * A new interface, such as `InternalHazelcastInstance` (with a
         * `getOriginalInstance()` method), might be introduced. Then the
         * underlying actual (original) Hazelcast instance can be retrieved
         * through this.
         *
         * The original Hazelcast instance is used for getting access to
         * internals. It's also used for passing the full cache name directly
         * by this cache manager itself.
         */
        if (hazelcastInstance instanceof HazelcastClientProxy) {
            client = ((HazelcastClientProxy) hazelcastInstance).client;
        } else {
            client = ((HazelcastClientInstanceImpl) hazelcastInstance);
        }

        ProxyManager proxyManager = client.getProxyManager();
        clientCacheProxyFactory = (ClientCacheProxyFactory) proxyManager.getClientProxyFactory(ICacheService.SERVICE_NAME);
    }

    @Override
    public void enableManagement(String cacheName, boolean enabled) {
        enableStatisticManagementOnNodes(cacheName, false, enabled);
    }

    @Override
    public void enableStatistics(String cacheName, boolean enabled) {
        enableStatisticManagementOnNodes(cacheName, true, enabled);
    }

    private void enableStatisticManagementOnNodes(String cacheName, boolean statOrMan, boolean enabled) {
        ensureOpen();
        checkNotNull(cacheName, "cacheName cannot be null");
        ClientCacheHelper.enableStatisticManagementOnNodes(client, getCacheNameWithPrefix(cacheName), statOrMan, enabled);
    }

    @SuppressFBWarnings("RV_RETURN_VALUE_OF_PUTIFABSENT_IGNORED")
    @Override
    protected <K, V> void addCacheConfigIfAbsent(CacheConfig<K, V> cacheConfig) {
        clientCacheProxyFactory.addCacheConfig(cacheConfig.getNameWithPrefix(), cacheConfig);
    }

    @Override
    protected void removeCacheConfigFromLocal(String cacheNameWithPrefix) {
        clientCacheProxyFactory.removeCacheConfig(cacheNameWithPrefix);
    }

    @Override
    protected <K, V> CacheConfig<K, V> getCacheConfig(String cacheName, String simpleCacheName) {
        return ClientCacheHelper.getCacheConfig(client, cacheName, simpleCacheName);
    }

    @Override
    protected <K, V> ICacheInternal<K, V> createCacheProxy(CacheConfig<K, V> cacheConfig) {
        clientCacheProxyFactory.addCacheConfig(cacheConfig.getNameWithPrefix(), cacheConfig);
        try {
            ClientICacheManager cacheManager = client.getCacheManager();
            String nameWithPrefix = cacheConfig.getNameWithPrefix();
            ICacheInternal<K, V> cache = (ICacheInternal<K, V>) cacheManager.getCacheByFullName(nameWithPrefix);
            cache.setCacheManager(this);
            return cache;
        } catch (Throwable t) {
            clientCacheProxyFactory.removeCacheConfig(cacheConfig.getNameWithPrefix());
            throw rethrow(t);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <K, V> CacheConfig<K, V> findCacheConfig(String cacheName, String simpleCacheName) {
        if (simpleCacheName == null) {
            return null;
        }
        CacheConfig<K, V> config = clientCacheProxyFactory.getCacheConfig(cacheName);
        if (config == null) {
            // if cache config not found, try to find it from partition
            config = getCacheConfig(cacheName, simpleCacheName);
            if (config != null) {
                // cache config possibly is not exist on other nodes, so create also on them if absent
                createCacheConfig(cacheName, config);
            }
        }
        return config;
    }

    @Override
    protected <K, V> void createCacheConfig(String cacheName, CacheConfig<K, V> config) {
        ClientCacheHelper.createCacheConfig(client, config, false);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T unwrap(Class<T> clazz) {
        if (HazelcastClientCacheManager.class.isAssignableFrom(clazz)) {
            return (T) this;
        }
        throw new IllegalArgumentException();
    }

    @Override
    protected void postClose() {
        if (properties.getProperty(HazelcastCachingProvider.HAZELCAST_CONFIG_LOCATION) != null) {
            hazelcastInstance.shutdown();
        }
    }

    @Override
    protected void postDestroy() {
        Iterator<Map.Entry<String, CacheConfig>> iterator = clientCacheProxyFactory.configs().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, CacheConfig> entry = iterator.next();
            String cacheName = entry.getKey();
            clientCacheProxyFactory.removeCacheConfig(cacheName);
            iterator.remove();
        }
    }

    @Override
    protected <K, V> void validateCacheConfig(CacheConfig<K, V> cacheConfig) {
        checkCacheConfig(cacheConfig, null);
    }

    @Override
    protected void onShuttingDown() {
    }
}
