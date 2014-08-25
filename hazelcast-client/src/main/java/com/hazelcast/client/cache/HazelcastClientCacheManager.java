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

import com.hazelcast.cache.CacheService;
import com.hazelcast.cache.HazelcastCacheManager;
import com.hazelcast.cache.ICache;
import com.hazelcast.cache.client.CacheCreateConfigRequest;
import com.hazelcast.cache.client.CacheGetConfigRequest;
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
import java.util.concurrent.Future;

public final class HazelcastClientCacheManager
        extends HazelcastCacheManager {

    private final ClientContext clientContext;

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
        //FIXME enableManagement
        throw new UnsupportedOperationException("enableManagement");
    }

    @Override
    public void enableStatistics(String cacheName, boolean enabled) {
        //FIXME enableStatistics
        throw new UnsupportedOperationException("enableStatistics");
    }

    @Override
    protected <K, V> CacheConfig<K, V> getCacheConfigLocal(String cacheName) {
        return null;
    }

    @Override
    protected <K, V> boolean createConfigOnPartition(CacheConfig<K, V> cacheConfig) {
        try {
            int partitionId = clientContext.getPartitionService().getPartitionId(cacheConfig.getNameWithPrefix());
            ClientRequest request = new CacheCreateConfigRequest(cacheConfig, true,partitionId);
            final Future future = clientContext.getInvocationService().invokeOnKeyOwner(request, cacheConfig.getNameWithPrefix());
            return clientContext.getSerializationService().toObject(future.get());
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    protected <K, V> void addCacheConfigIfAbsentToLocal(CacheConfig<K, V> cacheConfig) {
        //TODO implemenent it if we cache configs locally
    }

    @Override
    protected <K, V> void createConfigOnAllMembers(CacheConfig<K, V> cacheConfig) {
        final ClientInvocationService invocationService = clientContext.getInvocationService();
        final Collection<MemberImpl> members = clientContext.getClusterService().getMemberList();
        for (MemberImpl member : members) {
            try {
                ClientRequest request = new CacheCreateConfigRequest(cacheConfig, true, member.getAddress());
                final Future future = invocationService.invokeOnTarget(request, member.getAddress());
                future.get();//make sure all configs are created
            } catch (Exception e) {
                ExceptionUtil.sneakyThrow(e);
            }
        }
    }

    @Override
    protected <K, V> ICache<K, V> createCacheProxy(CacheConfig<K, V> cacheConfig) {
        final ClientCacheDistributedObject cacheDistributedObject = hazelcastInstance
                .getDistributedObject(CacheService.SERVICE_NAME, cacheConfig.getNameWithPrefix());
        return new ClientCacheProxy<K, V>(cacheConfig,cacheDistributedObject, this);
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

    @Override
    protected <K, V> void registerListeners(CacheConfig<K, V> cacheConfig, ICache<K, V> source) {
        //FIXME REGISTER LISTENERS
    }

}
