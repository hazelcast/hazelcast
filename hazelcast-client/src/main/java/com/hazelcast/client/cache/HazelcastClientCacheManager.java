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

import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.HazelcastCacheManager;
import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.client.CacheCreateConfigRequest;
import com.hazelcast.cache.impl.client.CacheGetConfigRequest;
import com.hazelcast.cache.impl.client.CacheManagementConfigRequest;
import com.hazelcast.client.impl.client.ClientRequest;
import com.hazelcast.client.proxy.ClientCacheDistributedObject;
import com.hazelcast.client.proxy.ClientCacheProxy;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.ClientInvocationService;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.util.ExceptionUtil;

import java.net.URI;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;

public final class HazelcastClientCacheManager
        extends HazelcastCacheManager {

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

    }

    @Override
    public void enableManagement(String cacheName, boolean enabled) {
        if (isClosed()) {
            throw new IllegalStateException();
        }
        if (cacheName == null) {
            throw new NullPointerException();
        }
        final ClientInvocationService invocationService = clientContext.getInvocationService();
        final Collection<MemberImpl> members = clientContext.getClusterService().getMemberList();
        for (MemberImpl member : members) {
            try {
                CacheManagementConfigRequest request = new CacheManagementConfigRequest(getCacheNameWithPrefix(cacheName), false,
                        enabled, member.getAddress());
                final Future future = invocationService.invokeOnTarget(request, member.getAddress());
                //make sure all configs are created
                future.get();
            } catch (Exception e) {
                ExceptionUtil.sneakyThrow(e);
            }
        }
    }

    @Override
    public void enableStatistics(String cacheName, boolean enabled) {
        if (isClosed()) {
            throw new IllegalStateException();
        }
        if (cacheName == null) {
            throw new NullPointerException();
        }
        final ClientInvocationService invocationService = clientContext.getInvocationService();
        final Collection<MemberImpl> members = clientContext.getClusterService().getMemberList();
        for (MemberImpl member : members) {
            try {
                CacheManagementConfigRequest request = new CacheManagementConfigRequest(getCacheNameWithPrefix(cacheName), true,
                        enabled, member.getAddress());
                final Future future = invocationService.invokeOnTarget(request, member.getAddress());
                //make sure all configs are created
                future.get();
            } catch (Exception e) {
                ExceptionUtil.sneakyThrow(e);
            }
        }
    }

    @Override
    protected <K, V> CacheConfig<K, V> getCacheConfigLocal(String cacheName) {
        System.out.println("GET FROM LOCAL CACHE name:"+ cacheName + " status:" + configs.containsKey(cacheName));
        return configs.get(cacheName);
    }

    @Override
    protected <K, V> void addCacheConfigIfAbsentToLocal(CacheConfig<K, V> cacheConfig) {
        System.out.println("ADDING LOCAL CACHE name:"+ cacheConfig.getNameWithPrefix() );
        configs.putIfAbsent(cacheConfig.getNameWithPrefix(), cacheConfig);
    }

    @Override
    protected void removeCacheConfigFromLocal(String cacheName) {
//        System.out.println("REMOVING LOCAL CACHE name:"+ cacheName );
        configs.remove(cacheName);
    }

    @Override
    protected <K, V> boolean createConfigOnPartition(CacheConfig<K, V> cacheConfig) {
        try {
            int partitionId = clientContext.getPartitionService().getPartitionId(cacheConfig.getNameWithPrefix());
            CacheCreateConfigRequest request = new CacheCreateConfigRequest(cacheConfig, true, partitionId);
            final Future future = clientContext.getInvocationService().invokeOnKeyOwner(request, cacheConfig.getNameWithPrefix());
            return clientContext.getSerializationService().toObject(future.get());
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    protected <K, V> void createConfigOnAllMembers(CacheConfig<K, V> cacheConfig) {
        final ClientInvocationService invocationService = clientContext.getInvocationService();
        final Collection<MemberImpl> members = clientContext.getClusterService().getMemberList();
        for (MemberImpl member : members) {
            try {
                ClientRequest request = new CacheCreateConfigRequest(cacheConfig, true, member.getAddress());
                final Future future = invocationService.invokeOnTarget(request, member.getAddress());
                //make sure all configs are created
                future.get();
            } catch (Exception e) {
                ExceptionUtil.sneakyThrow(e);
            }
        }
    }

    @Override
    protected <K, V> ICache<K, V> createCacheProxy(CacheConfig<K, V> cacheConfig) {
        final ClientCacheDistributedObject cacheDistributedObject = hazelcastInstance
                .getDistributedObject(CacheService.SERVICE_NAME, cacheConfig.getNameWithPrefix());
        return new ClientCacheProxy<K, V>(cacheConfig, cacheDistributedObject, this);
    }

    @Override
    protected <K, V> CacheConfig<K, V> getCacheConfigFromPartition(String cacheName) {
        ClientRequest request = new CacheGetConfigRequest(cacheName);
        try {
            final Future future = clientContext.getInvocationService().invokeOnKeyOwner(request, cacheName);
            return clientContext.getSerializationService().toObject(future.get());
        } catch (Exception e) {
            //throw ExceptionUtil.rethrow(e);
        }
        return null;
    }

    //    @Override
    //    protected <K, V> void registerListeners(CacheConfig<K, V> cacheConfig, ICache<K, V> source) {
    //        //throw new UnsupportedOperationException("");
    //        //FIXME REGISTER LISTENERS
    //    }

}
