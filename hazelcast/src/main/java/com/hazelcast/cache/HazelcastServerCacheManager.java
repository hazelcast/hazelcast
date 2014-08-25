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

package com.hazelcast.cache;

import com.hazelcast.cache.operation.CacheCreateConfigOperation;
import com.hazelcast.cache.operation.CacheGetConfigOperation;
import com.hazelcast.cache.operation.CacheManagementConfigOperation;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;

import javax.cache.configuration.CacheEntryListenerConfiguration;
import java.net.URI;
import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;

public class HazelcastServerCacheManager
        extends HazelcastCacheManager {

    //    protected HazelcastInstanceImpl hazelcastInstance;
    //    private HazelcastServerCachingProvider cachingProvider;
    private NodeEngine nodeEngine;
    private CacheService cacheService;

    public HazelcastServerCacheManager(HazelcastServerCachingProvider cachingProvider, HazelcastInstance hazelcastInstance,
                                       URI uri, ClassLoader classLoader, Properties properties) {
        super(cachingProvider, uri, classLoader, properties);
        if (hazelcastInstance == null) {
            throw new NullPointerException("hazelcastInstance missing");
        }
        this.hazelcastInstance = hazelcastInstance;
        //just to get a reference to nodeEngine and cacheService
        final CacheDistributedObject setupRef = hazelcastInstance.getDistributedObject(CacheService.SERVICE_NAME, "setupRef");
        nodeEngine = setupRef.getNodeEngine();
        cacheService = setupRef.getService();
        //        setupRef.destroy();
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
        cacheService.enableManagement(cacheNameWithPrefix, enabled);
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
        cacheService.enableStatistics(cacheNameWithPrefix, enabled);
        //ENABLE OTHER NODES
        enableStatisticManagementOnOtherNodes(cacheName, true, enabled);
    }

    private void enableStatisticManagementOnOtherNodes(String cacheName, boolean statOrMan, boolean enabled) {
        final String cacheNameWithPrefix = getCacheNameWithPrefix(cacheName);
        final Collection<MemberImpl> members = nodeEngine.getClusterService().getMemberList();
        for (MemberImpl member : members) {
            if (!member.localMember()) {
                final CacheManagementConfigOperation op = new CacheManagementConfigOperation(cacheNameWithPrefix, statOrMan,
                        enabled);
                nodeEngine.getOperationService().invokeOnTarget(CacheService.SERVICE_NAME, op, member.getAddress());
            }
        }
    }

    @Override
    protected <K, V> CacheConfig<K, V> getCacheConfigLocal(String cacheName) {
        return cacheService.getCacheConfig(cacheName);
    }

    @Override
    protected <K, V> boolean createConfigOnPartition(CacheConfig<K, V> cacheConfig) {
        //CREATE THE CONFIG ON PARTITION BY cacheNamePrefix using a request
        final CacheCreateConfigOperation cacheCreateConfigOperation = new CacheCreateConfigOperation(cacheConfig, true);
        final OperationService operationService = nodeEngine.getOperationService();

        int partitionId = nodeEngine.getPartitionService().getPartitionId(cacheConfig.getNameWithPrefix());
        final InternalCompletableFuture<Boolean> f = operationService
                .invokeOnPartition(CacheService.SERVICE_NAME, cacheCreateConfigOperation, partitionId);
        return f.getSafely();
    }

    @Override
    protected <K, V> void addCacheConfigIfAbsentToLocal(CacheConfig<K, V> cacheConfig) {
        cacheService.createCacheConfigIfAbsent(cacheConfig);
    }

    @Override
    protected <K, V> void createConfigOnAllMembers(CacheConfig<K, V> cacheConfig) {
        final OperationService operationService = nodeEngine.getOperationService();
        final Collection<MemberImpl> members = nodeEngine.getClusterService().getMemberList();
        for (MemberImpl member : members) {
            if (!member.localMember()) {
                final CacheCreateConfigOperation op = new CacheCreateConfigOperation(cacheConfig, true);
                final InternalCompletableFuture<Object> f2 = operationService
                        .invokeOnTarget(CacheService.SERVICE_NAME, op, member.getAddress());
                f2.getSafely();//make sure all configs are created
            }
        }
    }

    @Override
    protected <K, V> ICache<K, V> createCacheProxy(CacheConfig<K, V> cacheConfig) {
        final CacheDistributedObject cacheDistributedObject = hazelcastInstance
                .getDistributedObject(CacheService.SERVICE_NAME, cacheConfig.getNameWithPrefix());
        return new CacheProxy<K, V>(cacheConfig, cacheDistributedObject, this);
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

    @Override
    protected <K, V> void registerListeners(CacheConfig<K, V> cacheConfig, ICache<K, V> source) {
        //REGISTER LISTENERS
        final Iterator<CacheEntryListenerConfiguration<K, V>> iterator = cacheConfig.getCacheEntryListenerConfigurations()
                                                                                    .iterator();
        while (iterator.hasNext()) {
            final CacheEntryListenerConfiguration<K, V> listenerConfig = iterator.next();
            cacheService.registerCacheEntryListener(cacheConfig.getNameWithPrefix(), source, listenerConfig);
        }
    }

}
