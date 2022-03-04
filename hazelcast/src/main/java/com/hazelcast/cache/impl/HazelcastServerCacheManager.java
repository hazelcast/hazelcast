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

package com.hazelcast.cache.impl;

import com.hazelcast.cache.HazelcastCachingProvider;
import com.hazelcast.cache.impl.operation.CacheGetConfigOperation;
import com.hazelcast.cache.impl.operation.CacheManagementConfigOperation;
import com.hazelcast.cluster.Member;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;
import com.hazelcast.spi.merge.SplitBrainMergePolicyProvider;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.config.ConfigValidator.checkCacheConfig;
import static com.hazelcast.internal.util.FutureUtil.waitWithDeadline;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Hazelcast {@link javax.cache.CacheManager} for server implementation. This
 * subclass of {@link AbstractHazelcastCacheManager} is managed by
 * {@link HazelcastServerCachingProvider}.
 * <p>
 * As it lives on a node JVM, it has reference to {@link CacheService} and
 * {@link NodeEngine} where this manager makes calls.
 * <p>
 * When JCache server implementation is configured, an instance of this class
 * will be returned when {@link javax.cache.spi.CachingProvider#getCacheManager()}
 * is called.
 */
public class HazelcastServerCacheManager extends AbstractHazelcastCacheManager {

    private final HazelcastInstanceImpl instance;
    private final NodeEngine nodeEngine;
    private final CacheService cacheService;

    public HazelcastServerCacheManager(HazelcastServerCachingProvider cachingProvider, HazelcastInstance hazelcastInstance,
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
        ensureOpen();
        checkNotNull(cacheName, "cacheName cannot be null");
        String cacheNameWithPrefix = getCacheNameWithPrefix(cacheName);
        cacheService.setManagementEnabled(null, cacheNameWithPrefix, enabled);
        enableStatisticManagementOnOtherNodes(cacheName, false, enabled);
    }

    @Override
    public void enableStatistics(String cacheName, boolean enabled) {
        ensureOpen();
        checkNotNull(cacheName, "cacheName cannot be null");
        String cacheNameWithPrefix = getCacheNameWithPrefix(cacheName);
        cacheService.setStatisticsEnabled(null, cacheNameWithPrefix, enabled);
        enableStatisticManagementOnOtherNodes(cacheName, true, enabled);
    }

    private void enableStatisticManagementOnOtherNodes(String cacheName, boolean statOrMan, boolean enabled) {
        String cacheNameWithPrefix = getCacheNameWithPrefix(cacheName);
        OperationService operationService = nodeEngine.getOperationService();
        Collection<Future> futures = new ArrayList<Future>();
        for (Member member : nodeEngine.getClusterService().getMembers()) {
            if (!member.localMember()) {
                CacheManagementConfigOperation op = new CacheManagementConfigOperation(cacheNameWithPrefix, statOrMan, enabled);
                Future future = operationService.invokeOnTarget(CacheService.SERVICE_NAME, op, member.getAddress());
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
    @SuppressWarnings("unchecked")
    protected <K, V> CacheConfig<K, V> findCacheConfig(String cacheName, String simpleCacheName) {
        CacheConfig<K, V> config = cacheService.getCacheConfig(cacheName);
        if (config == null) {
            config = cacheService.findCacheConfig(simpleCacheName);
            if (config != null) {
                config.setManagerPrefix(cacheName.substring(0, cacheName.lastIndexOf(simpleCacheName)));
            } else {
                // if cache config is still not found, try to get it from a partition
                config = getCacheConfig(cacheName, simpleCacheName);
            }
        }
        if (config != null) {
            /*
             * Also create the cache config on other nodes to be sure that the
             * cache config is exist on all nodes. This is needed because even
             * though the cache config exists on this node (for example added
             * by an in-flight cache config creation operation), it still might
             * not exist on other nodes yet (but will be created eventually).
             */
            createCacheConfig(cacheName, config);
        }
        return config;
    }

    @Override
    protected <K, V> void createCacheConfig(String cacheName, CacheConfig<K, V> config) {
        cacheService.createCacheConfigOnAllMembers(PreJoinCacheConfig.of(config));
    }

    @Override
    protected <K, V> ICacheInternal<K, V> createCacheProxy(CacheConfig<K, V> cacheConfig) {
        HazelcastInstanceCacheManager cacheManager = instance.getCacheManager();
        CacheProxy<K, V> cacheProxy = (CacheProxy<K, V>) cacheManager.getCacheByFullName(cacheConfig.getNameWithPrefix());
        cacheProxy.setCacheManager(this);
        return cacheProxy;
    }

    @Override
    protected <K, V> CacheConfig<K, V> getCacheConfig(String cacheNameWithPrefix, String cacheName) {
        CacheGetConfigOperation op = new CacheGetConfigOperation(cacheNameWithPrefix, cacheName);
        int partitionId = nodeEngine.getPartitionService().getPartitionId(cacheNameWithPrefix);
        InvocationFuture<CacheConfig<K, V>> f = nodeEngine.getOperationService()
                .invokeOnPartition(CacheService.SERVICE_NAME, op, partitionId);
        return f.joinInternal();
    }

    @Override
    protected void removeCacheConfigFromLocal(String cacheNameWithPrefix) {
        cacheService.deleteCacheConfig(cacheNameWithPrefix);
        super.removeCacheConfigFromLocal(cacheNameWithPrefix);
    }

    @Override
    protected <K, V> void validateCacheConfig(CacheConfig<K, V> cacheConfig) {
        SplitBrainMergePolicyProvider mergePolicyProvider = nodeEngine.getSplitBrainMergePolicyProvider();
        checkCacheConfig(cacheConfig, mergePolicyProvider);
    }

    @Override
    @SuppressWarnings("unchecked")
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
