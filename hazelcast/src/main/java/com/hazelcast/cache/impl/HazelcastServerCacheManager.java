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

package com.hazelcast.cache.impl;

import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.operation.CacheCreateConfigOperation;
import com.hazelcast.cache.impl.operation.CacheGetConfigOperation;
import com.hazelcast.cache.impl.operation.CacheManagementConfigOperation;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.FutureUtil.waitWithDeadline;

/**
 * Hazelcast {@link javax.cache.CacheManager} for server implementation. This subclass of
 * {@link HazelcastCacheManager} is managed by {@link HazelcastServerCachingProvider}.
 * <p>As it lives on a node JVM, it has reference to {@link CacheService} and {@link NodeEngine} where this
 * manager make calls.</p>
 * <p>When JCache server implementation is configured, an instance of this class will be returned when
 * {@link javax.cache.spi.CachingProvider#getCacheManager()} is called.</p>
 */
public class HazelcastServerCacheManager
        extends HazelcastCacheManager {

    private NodeEngineImpl nodeEngine;
    private ICacheService cacheService;

    public HazelcastServerCacheManager(HazelcastServerCachingProvider cachingProvider, HazelcastInstance hazelcastInstance,
                                       URI uri, ClassLoader classLoader, Properties properties) {
        super(cachingProvider, uri, classLoader, properties);
        if (hazelcastInstance == null) {
            throw new NullPointerException("hazelcastInstance missing");
        }
        this.hazelcastInstance = hazelcastInstance;
        //just to get a reference to nodeEngine and cacheService
        final CacheDistributedObject setupRef = hazelcastInstance.getDistributedObject(CacheService.SERVICE_NAME, "setupRef");
        nodeEngine = (NodeEngineImpl) setupRef.getNodeEngine();
        cacheService = setupRef.getService();

        //TODO: should we destroy the ref ?
        //setupRef.destroy();
        logger = nodeEngine.getLogger(getClass());
    }

    @Override
    public void enableManagement(String cacheName, boolean enabled) {
        if (isClosed()) {
            throw new IllegalStateException();
        }
        if (cacheName == null) {
            throw new NullPointerException();
        }
        final String cacheNameWithPrefix = getCacheNameWithPrefix(cacheName);
        cacheService.setManagementEnabled(cacheNameWithPrefix, enabled);
        //ENABLE OTHER NODES
        enableStatisticManagementOnOtherNodes(cacheName, false, enabled);
    }

    @Override
    public void enableStatistics(String cacheName, boolean enabled) {
        if (isClosed()) {
            throw new IllegalStateException();
        }
        if (cacheName == null) {
            throw new NullPointerException();
        }
        final String cacheNameWithPrefix = getCacheNameWithPrefix(cacheName);
        cacheService.setStatisticsEnabled(cacheNameWithPrefix, enabled);
        //ENABLE OTHER NODES
        enableStatisticManagementOnOtherNodes(cacheName, true, enabled);
    }

    private void enableStatisticManagementOnOtherNodes(String cacheName, boolean statOrMan, boolean enabled) {
        final String cacheNameWithPrefix = getCacheNameWithPrefix(cacheName);
        final Collection<MemberImpl> members = nodeEngine.getClusterService().getMemberList();
        final Collection<Future> futures = new ArrayList<Future>();
        for (MemberImpl member : members) {
            if (!member.localMember()) {
                final CacheManagementConfigOperation op = new CacheManagementConfigOperation(cacheNameWithPrefix, statOrMan,
                        enabled);
                final Future future = nodeEngine.getOperationService()
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
    protected <K, V> boolean createConfigOnPartition(CacheConfig<K, V> cacheConfig) {
        //CREATE THE CONFIG ON PARTITION BY cacheNamePrefix using a request
        final CacheCreateConfigOperation cacheCreateConfigOperation = new CacheCreateConfigOperation(cacheConfig);
        final OperationService operationService = nodeEngine.getOperationService();

        int partitionId = nodeEngine.getPartitionService().getPartitionId(cacheConfig.getNameWithPrefix());
        final InternalCompletableFuture<Boolean> f = operationService
                .invokeOnPartition(CacheService.SERVICE_NAME, cacheCreateConfigOperation, partitionId);
        return f.getSafely();
    }

    @Override
    protected <K, V> void addCacheConfigIfAbsentToLocal(CacheConfig<K, V> cacheConfig) {
        cacheService.createCacheConfigIfAbsent(cacheConfig, false);
    }

    @Override
    protected <K, V> ICache<K, V> createCacheProxy(CacheConfig<K, V> cacheConfig) {
        return new CacheProxy<K, V>(cacheConfig, nodeEngine, cacheService, this);
    }

    @Override
    protected <K, V> CacheConfig<K, V> getCacheConfigFromPartition(String cacheNameWithPrefix) {
        //remote check
        final CacheGetConfigOperation op = new CacheGetConfigOperation(cacheNameWithPrefix);
        int partitionId = nodeEngine.getPartitionService().getPartitionId(cacheNameWithPrefix);
        final InternalCompletableFuture<CacheConfig> f = nodeEngine.getOperationService()
                .invokeOnPartition(CacheService.SERVICE_NAME, op, partitionId);
        return f.getSafely();
    }

    protected void postClose() {
    }
}
