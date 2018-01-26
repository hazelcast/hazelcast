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

package com.hazelcast.cache.impl;

import com.hazelcast.cache.HazelcastCachingProvider;
import com.hazelcast.cache.impl.operation.CacheCreateConfigOperation;
import com.hazelcast.cache.impl.operation.CacheGetConfigOperation;
import com.hazelcast.cache.impl.operation.CacheManagementConfigOperation;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.HazelcastInstanceProxy;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.FutureUtil.waitWithDeadline;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * <p>
 * Hazelcast {@link javax.cache.CacheManager} for server implementation. This subclass of
 * {@link AbstractHazelcastCacheManager} is managed by {@link HazelcastServerCachingProvider}.
 * </p>
 * <p>
 * As it lives on a node JVM, it has reference to {@link CacheService} and {@link NodeEngine} where this
 * manager make calls.
 * </p>
 * <p>
 * When JCache server implementation is configured, an instance of this class will be returned when
 * {@link javax.cache.spi.CachingProvider#getCacheManager()} is called.
 * </p>
 */
public class HazelcastServerCacheManager
        extends AbstractHazelcastCacheManager {

    private final HazelcastInstanceImpl instance;
    private final NodeEngine nodeEngine;
    private final ICacheService cacheService;

    public HazelcastServerCacheManager(HazelcastServerCachingProvider cachingProvider, HazelcastInstance hazelcastInstance,
                                       URI uri, ClassLoader classLoader, Properties properties) {
        super(cachingProvider, hazelcastInstance, uri, classLoader, properties);

        /*
         * TODO
         *
         * A new interface, such as `InternalHazelcastInstance` (has `getOriginalInstance()` method),
         * might be introduced. Then underlying actual (original) Hazelcast instance is retrieved through this.
         *
         * Original Hazelcast instance is used for getting `NodeEngine` and `ICacheService`.
         * Also it is used for passing full cache name directly by this cache manager itself.
         */
        if (hazelcastInstance instanceof HazelcastInstanceProxy) {
            instance = ((HazelcastInstanceProxy) hazelcastInstance).getOriginal();
        } else {
            instance = (HazelcastInstanceImpl) hazelcastInstance;
        }
        nodeEngine = instance.node.getNodeEngine();
        cacheService = nodeEngine.getService(ICacheService.SERVICE_NAME);
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
    protected <K, V> void addCacheConfigIfAbsent(CacheConfig<K, V> cacheConfig) {
        cacheService.putCacheConfigIfAbsent(cacheConfig);
    }

    @Override
    protected <K, V> CacheConfig<K, V> findCacheConfig(String cacheName,
                                                       String simpleCacheName,
                                                       boolean createAlsoOnOthers,
                                                       boolean syncCreate) {
        CacheConfig<K, V> config = cacheService.getCacheConfig(cacheName);
        if (config == null) {
            config = cacheService.findCacheConfig(simpleCacheName);
            if (config != null) {
                config.setManagerPrefix(cacheName.substring(0, cacheName.lastIndexOf(simpleCacheName)));
            } else {
                // If still cache config not found, try to find it from partition
                config = getCacheConfig(cacheName, simpleCacheName);
            }
        }
        if (config != null) {
            // Also create cache config on other nodes to be sure that cache config is exist on all nodes.
            // This is needed because even though cache config is exist on this node
            // (for example added by an in-flight cache config creation operation)
            // it still might not exist on other nodes yet (but will created eventually).
            createCacheConfig(cacheName, config, createAlsoOnOthers, syncCreate);
        }
        return config;
    }

    @Override
    protected <K, V> CacheConfig<K, V> createCacheConfig(String cacheName,
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
            return (CacheConfig<K, V>) future.join();
        } else {
            return currentCacheConfig;
        }
    }

    @Override
    protected <K, V> ICacheInternal<K, V> createCacheProxy(CacheConfig<K, V> cacheConfig) {
        CacheProxy<K, V> cacheProxy = (CacheProxy<K, V>) instance.getCacheManager()
                                                                 .getCacheByFullName(cacheConfig.getNameWithPrefix());
        cacheProxy.setCacheManager(this);
        return cacheProxy;
    }

    @Override
    protected <K, V> CacheConfig<K, V> getCacheConfig(String cacheNameWithPrefix, String cacheName) {
        CacheGetConfigOperation op = new CacheGetConfigOperation(cacheNameWithPrefix, cacheName);
        int partitionId = nodeEngine.getPartitionService().getPartitionId(cacheNameWithPrefix);
        InternalCompletableFuture<CacheConfig> f =
                nodeEngine.getOperationService()
                          .invokeOnPartition(CacheService.SERVICE_NAME, op, partitionId);
        return f.join();
    }

    @Override
    protected void removeCacheConfigFromLocal(String cacheNameWithPrefix) {
        cacheService.deleteCacheConfig(cacheNameWithPrefix);
        super.removeCacheConfigFromLocal(cacheNameWithPrefix);
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

    @Override
    protected void onShuttingDown() {
        close();
    }

    public ICacheService getCacheService() {
        return cacheService;
    }

}
