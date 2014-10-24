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

package com.hazelcast.client.cache.impl;

import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.CacheProxyUtil;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.HazelcastCacheManager;
import com.hazelcast.cache.impl.client.CacheCreateConfigRequest;
import com.hazelcast.cache.impl.client.CacheGetConfigRequest;
import com.hazelcast.cache.impl.client.CacheManagementConfigRequest;
import com.hazelcast.client.impl.client.ClientRequest;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.ClientInvocationService;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.logging.Logger;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.FutureUtil;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * CacheManager implementation for client side
 *
 * Provides client side cacheManager functionality
 */
public final class HazelcastClientCacheManager extends HazelcastCacheManager {

    private final ClientContext clientContext;
    private final ConcurrentMap<String, CacheConfig> configs = new ConcurrentHashMap<String, CacheConfig>();

    public HazelcastClientCacheManager(HazelcastClientCachingProvider cachingProvider, HazelcastInstance hazelcastInstance,
                                       URI uri, ClassLoader classLoader, Properties properties) {
        super(cachingProvider, uri, classLoader, properties);
        if (hazelcastInstance == null) {
            throw new NullPointerException("hazelcastInstance missing");
        }
        this.hazelcastInstance = hazelcastInstance;
        final ClientCacheDistributedObject setupRef = hazelcastInstance
                .getDistributedObject(CacheService.SERVICE_NAME, "setupRef");
        this.clientContext = setupRef.getClientContext();
        logger = Logger.getLogger(getClass());
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
        if (isClosed()) {
            throw new IllegalStateException();
        }
        if (cacheName == null) {
            throw new NullPointerException();
        }
        final ClientInvocationService invocationService = clientContext.getInvocationService();
        final Collection<MemberImpl> members = clientContext.getClusterService().getMemberList();
        final Collection<Future> futures = new ArrayList<Future>();
        for (MemberImpl member : members) {
            try {
                CacheManagementConfigRequest request = new CacheManagementConfigRequest(getCacheNameWithPrefix(cacheName),
                        statOrMan, enabled, member.getAddress());
                final Future future = invocationService.invokeOnTarget(request, member.getAddress());
                futures.add(future);
            } catch (Exception e) {
                ExceptionUtil.sneakyThrow(e);
            }
        }
        //make sure all configs are created
        FutureUtil.waitWithDeadline(futures, CacheProxyUtil.AWAIT_COMPLETION_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    }

    @Override
    protected <K, V> CacheConfig<K, V> getCacheConfigLocal(String cacheName) {
        return configs.get(cacheName);
    }

    @Override
    protected <K, V> void addCacheConfigIfAbsentToLocal(CacheConfig<K, V> cacheConfig) {
        configs.putIfAbsent(cacheConfig.getNameWithPrefix(), cacheConfig);
    }

    @Override
    protected void removeCacheConfigFromLocal(String cacheName) {
        configs.remove(cacheName);
    }

    @Override
    protected <K, V> boolean createConfigOnPartition(CacheConfig<K, V> cacheConfig) {
        try {
            int partitionId = clientContext.getPartitionService().getPartitionId(cacheConfig.getNameWithPrefix());
            CacheCreateConfigRequest request = new CacheCreateConfigRequest(cacheConfig, true, partitionId);
            final Future future = clientContext.getInvocationService().invokeOnKeyOwner(request, cacheConfig.getNameWithPrefix());
            return (Boolean) clientContext.getSerializationService().toObject(future.get());
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    protected <K, V> ICache<K, V> createCacheProxy(CacheConfig<K, V> cacheConfig) {
        return new ClientCacheProxy<K, V>(cacheConfig, clientContext, this);
    }

    @Override
    protected <K, V> CacheConfig<K, V> getCacheConfigFromPartition(String cacheName) {
        ClientRequest request = new CacheGetConfigRequest(cacheName);
        try {
            final Future future = clientContext.getInvocationService().invokeOnKeyOwner(request, cacheName);
            return clientContext.getSerializationService().toObject(future.get());
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    protected void postClose() {
        if (!isDefaultURI) {
            hazelcastInstance.shutdown();
        }
    }
}
