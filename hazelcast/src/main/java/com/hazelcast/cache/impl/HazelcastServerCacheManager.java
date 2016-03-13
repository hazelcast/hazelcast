/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache.impl;

import com.hazelcast.cache.HazelcastCachingProvider;
import com.hazelcast.cache.impl.operation.CacheCreateConfigOperation;
import com.hazelcast.cache.impl.operation.CacheGetConfigOperation;
import com.hazelcast.cache.impl.operation.CacheManagementConfigOperation;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;

import javax.cache.CacheException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.FutureUtil.waitWithDeadline;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Hazelcast {@link javax.cache.CacheManager} for server implementation. This subclass of
 * {@link AbstractHazelcastCacheManager} is managed by {@link HazelcastServerCachingProvider}.
 * <p>As it lives on a node JVM, it has reference to {@link CacheService} and {@link NodeEngine} where this
 * manager make calls.</p>
 * <p>When JCache server implementation is configured, an instance of this class will be returned when
 * {@link javax.cache.spi.CachingProvider#getCacheManager()} is called.</p>
 */
public class HazelcastServerCacheManager
        extends AbstractHazelcastCacheManager {

    private final NodeEngine nodeEngine;
    private final ICacheService cacheService;

    public HazelcastServerCacheManager(HazelcastServerCachingProvider cachingProvider, HazelcastInstance hazelcastInstance,
                                       URI uri, ClassLoader classLoader, Properties properties) {
        super(cachingProvider, hazelcastInstance, uri, classLoader, properties);

        CacheDistributedObject setupRef =
                hazelcastInstance.getDistributedObject(CacheService.SERVICE_NAME, "setupRef");
        nodeEngine = setupRef.getNodeEngine();
        cacheService = setupRef.getService();

        // TODO should we destroy the ref like "setupRef.destroy();"
    }

    @Override
    public void enableManagement(String cacheName, boolean enabled) {
        checkIfManagerNotClosed();
        checkNotNull(cacheName, "cacheName cannot be null");
        String cacheNameWithPrefix = getCacheNameWithPrefix(cacheName);
        cacheService.setManagementEnabled(null, cacheNameWithPrefix, enabled);
        enableStatisticManagementOnOtherNodes(cacheName, false, enabled);
    }

    @Override
    public void enableStatistics(String cacheName, boolean enabled) {
        checkIfManagerNotClosed();
        checkNotNull(cacheName, "cacheName cannot be null");
        String cacheNameWithPrefix = getCacheNameWithPrefix(cacheName);
        cacheService.setStatisticsEnabled(null, cacheNameWithPrefix, enabled);
        enableStatisticManagementOnOtherNodes(cacheName, true, enabled);
    }

    private void enableStatisticManagementOnOtherNodes(String cacheName, boolean statOrMan, boolean enabled) {
        String cacheNameWithPrefix = getCacheNameWithPrefix(cacheName);
        Collection<Member> members = nodeEngine.getClusterService().getMembers();
        Collection<Future> futures = new ArrayList<Future>();
        for (Member member : members) {
            if (!member.localMember()) {
                CacheManagementConfigOperation op =
                        new CacheManagementConfigOperation(cacheNameWithPrefix, statOrMan, enabled);
                Future future = nodeEngine.getOperationService()
                                          .invokeOnTarget(CacheService.SERVICE_NAME, op, member.getAddress());
                futures.add(future);
            }
        }
        waitWithDeadline(futures, CacheProxyUtil.AWAIT_COMPLETION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    @Override
    protected <K, V> CacheConfig<K, V> getCacheConfigLocal(String cacheName) {
        return cacheService.getCacheConfig(cacheName);
    }

    @Override
    protected <K, V> CacheConfig<K, V> createConfigOnPartition(CacheConfig<K, V> cacheConfig) {
        CacheCreateConfigOperation cacheCreateConfigOperation = new CacheCreateConfigOperation(cacheConfig);
        OperationService operationService = nodeEngine.getOperationService();
        int partitionId = nodeEngine.getPartitionService().getPartitionId(cacheConfig.getNameWithPrefix());
        InternalCompletableFuture<CacheConfig<K, V>> f =
                operationService.invokeOnPartition(CacheService.SERVICE_NAME,
                                                   cacheCreateConfigOperation, partitionId);
        return f.getSafely();
    }

    @Override
    protected <K, V> void addCacheConfigIfAbsent(CacheConfig<K, V> cacheConfig) {
        cacheService.putCacheConfigIfAbsent(cacheConfig);
    }

    @Override
    protected <K, V> CacheConfig<K, V> findConfig(String cacheName,
                                                  String simpleCacheName,
                                                  boolean createAlsoOnOthers,
                                                  boolean syncCreate) {
        CacheConfig<K, V> config = cacheService.getCacheConfig(cacheName);
        if (config == null) {
            CacheSimpleConfig simpleConfig = cacheService.findCacheConfig(simpleCacheName);
            if (simpleConfig != null) {
                try {
                    config = new CacheConfig(simpleConfig);
                    config.setName(simpleCacheName);
                    config.setManagerPrefix(cacheName.substring(0, cacheName.lastIndexOf(simpleCacheName)));
                } catch (Exception e) {
                    // Cannot create the actual config from the declarative one
                    throw new CacheException(e);
                }
            }
            if (config == null) {
                // If still cache config not found, try to find it from partition
                config = getCacheConfigFromPartition(cacheName, simpleCacheName);
            }
            if (config != null) {
                // Cache config possibly is not exist on other nodes, so create also on them if absent
                createConfig(cacheName, config, createAlsoOnOthers, syncCreate);
            }
        }
        return config;
    }

    @Override
    protected <K, V> CacheConfig<K, V> createConfig(String cacheName,
                                                    CacheConfig<K, V> config,
                                                    boolean createAlsoOnOthers,
                                                    boolean syncCreate) {
        CacheConfig<K, V> currentCacheConfig = cacheService.getCacheConfig(cacheName);
        OperationService operationService = nodeEngine.getOperationService();
        // Create cache config on all nodes.
        CacheCreateConfigOperation op =
                new CacheCreateConfigOperation(config, createAlsoOnOthers);
        // Run "CacheCreateConfigOperation" on this node. Its itself handles interaction with other nodes.
        // This operation doesn't block operation thread even "syncCreate" is specified.
        // In that case, scheduled thread is used, not operation thread.
        InternalCompletableFuture future =
                operationService.invokeOnTarget(CacheService.SERVICE_NAME, op, nodeEngine.getThisAddress());
        if (syncCreate) {
            return (CacheConfig<K, V>) future.getSafely();
        } else {
            return currentCacheConfig;
        }
    }

    @Override
    protected <K, V> ICacheInternal<K, V> createCacheProxy(CacheConfig<K, V> cacheConfig) {
        return new CacheProxy<K, V>(cacheConfig, nodeEngine, cacheService, this);
    }

    @Override
    protected <K, V> CacheConfig<K, V> getCacheConfigFromPartition(String cacheNameWithPrefix, String cacheName) {
        CacheGetConfigOperation op = new CacheGetConfigOperation(cacheNameWithPrefix, cacheName);
        int partitionId = nodeEngine.getPartitionService().getPartitionId(cacheNameWithPrefix);
        InternalCompletableFuture<CacheConfig> f =
                nodeEngine.getOperationService()
                          .invokeOnPartition(CacheService.SERVICE_NAME, op, partitionId);
        return f.getSafely();
    }

    @Override
    protected void removeCacheConfigFromLocal(String cacheName) {
        cacheService.deleteCacheConfig(cacheName);
        super.removeCacheConfigFromLocal(cacheName);
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        if (HazelcastServerCacheManager.class.isAssignableFrom(clazz)) {
            return (T) this;
        }
        throw new IllegalArgumentException();
    }

    protected void postClose() {
        if (properties.getProperty(HazelcastCachingProvider.HAZELCAST_CONFIG_LOCATION) != null) {
            hazelcastInstance.shutdown();
        }
    }

    public ICacheService getCacheService() {
        return cacheService;
    }

}
