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

package com.hazelcast.client.cache.impl;

import com.hazelcast.cache.HazelcastCachingProvider;
import com.hazelcast.cache.impl.AbstractHazelcastCacheManager;
import com.hazelcast.cache.impl.CacheProxyUtil;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.ICacheInternal;
import com.hazelcast.cache.impl.nearcache.NearCacheManager;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.HazelcastClientProxy;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CacheCreateConfigCodec;
import com.hazelcast.client.impl.protocol.codec.CacheGetConfigCodec;
import com.hazelcast.client.impl.protocol.codec.CacheManagementConfigCodec;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
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

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * CacheManager implementation for client side
 * <p/>
 * Provides client side cacheManager functionality
 */
public final class HazelcastClientCacheManager
        extends AbstractHazelcastCacheManager {

    private final ClientContext clientContext;
    private final ConcurrentMap<String, CacheConfig> configs = new ConcurrentHashMap<String, CacheConfig>();

    public HazelcastClientCacheManager(HazelcastClientCachingProvider cachingProvider, HazelcastInstance hazelcastInstance,
                                       URI uri, ClassLoader classLoader, Properties properties) {
        super(cachingProvider, hazelcastInstance, uri, classLoader, properties);

        final ClientCacheDistributedObject setupRef = hazelcastInstance
                .getDistributedObject(CacheService.SERVICE_NAME, "setupRef");
        this.clientContext = setupRef.getClientContext();
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
        checkIfManagerNotClosed();
        checkNotNull(cacheName, "cacheName cannot be null");
        Collection<Member> members = clientContext.getClusterService().getMemberList();
        Collection<Future> futures = new ArrayList<Future>();
        HazelcastClientInstanceImpl client = (HazelcastClientInstanceImpl) clientContext.getHazelcastInstance();
        for (Member member : members) {
            try {
                Address address = member.getAddress();

                ClientMessage request = CacheManagementConfigCodec
                        .encodeRequest(getCacheNameWithPrefix(cacheName), statOrMan, enabled, address);
                ClientInvocation clientInvocation = new ClientInvocation(client, request, address);
                Future<ClientMessage> future = clientInvocation.invoke();
                futures.add(future);
            } catch (Exception e) {
                ExceptionUtil.sneakyThrow(e);
            }
        }
        // Make sure all configs are created
        FutureUtil.waitWithDeadline(futures, CacheProxyUtil.AWAIT_COMPLETION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    @Override
    protected <K, V> CacheConfig<K, V> getCacheConfigLocal(String cacheName) {
        return configs.get(cacheName);
    }

    @Override
    protected <K, V> void addCacheConfigIfAbsent(CacheConfig<K, V> cacheConfig) {
        configs.putIfAbsent(cacheConfig.getNameWithPrefix(), cacheConfig);
    }

    @Override
    protected void removeCacheConfigFromLocal(String cacheName) {
        configs.remove(cacheName);
    }

    @Override
    protected <K, V> CacheConfig<K, V> getCacheConfigFromPartition(String cacheName, String simpleCacheName) {
        ClientMessage request = CacheGetConfigCodec.encodeRequest(cacheName, simpleCacheName);
        HazelcastClientInstanceImpl client = (HazelcastClientInstanceImpl) clientContext.getHazelcastInstance();
        try {
            int partitionId = clientContext.getPartitionService().getPartitionId(cacheName);
            ClientInvocation clientInvocation = new ClientInvocation(client, request, partitionId);
            Future<ClientMessage> future = clientInvocation.invoke();
            ClientMessage responseMessage = future.get();
            SerializationService serializationService = clientContext.getSerializationService();
            return serializationService.toObject(CacheGetConfigCodec.decodeResponse(responseMessage).response);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    protected <K, V> CacheConfig<K, V> createConfigOnPartition(CacheConfig<K, V> cacheConfig) {
        HazelcastClientInstanceImpl client = (HazelcastClientInstanceImpl) clientContext.getHazelcastInstance();
        try {
            int partitionId = clientContext.getPartitionService().getPartitionId(cacheConfig.getNameWithPrefix());
            Data cacheConfigData = clientContext.getSerializationService().toData(cacheConfig);
            ClientMessage request = CacheCreateConfigCodec.encodeRequest(cacheConfigData, false);
            ClientInvocation clientInvocation = new ClientInvocation(client, request, partitionId);

            Future<ClientMessage> future = clientInvocation.invoke();
            ClientMessage responseMessage = future.get();
            final Data result = CacheCreateConfigCodec.decodeResponse(responseMessage).response;
            return (CacheConfig<K, V>) clientContext.getSerializationService().toObject(result);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    protected <K, V> ICacheInternal<K, V> createCacheProxy(CacheConfig<K, V> cacheConfig) {
        return new ClientCacheProxy<K, V>(cacheConfig, clientContext, this);
    }

    @Override
    protected <K, V> CacheConfig<K, V> findConfig(String cacheName, String simpleCacheName, boolean createAlsoOnOthers,
                                                  boolean syncCreate) {
        CacheConfig<K, V> config = configs.get(cacheName);
        if (config == null) {
            // If cache config not found, try to find it from partition
            config = getCacheConfigFromPartition(cacheName, simpleCacheName);
            if (config != null) {
                // Cache config possibly is not exist on other nodes, so create also on them if absent
                createConfig(cacheName, config, createAlsoOnOthers, syncCreate);
            }
        }
        return config;
    }

    @Override
    protected <K, V> CacheConfig<K, V> createConfig(String cacheName, CacheConfig<K, V> config, boolean createAlsoOnOthers,
                                                    boolean syncCreate) {
        CacheConfig<K, V> currentCacheConfig = configs.get(cacheName);
        HazelcastClientInstanceImpl client = (HazelcastClientInstanceImpl) clientContext.getHazelcastInstance();
        try {
            int partitionId = clientContext.getPartitionService().getPartitionId(config.getNameWithPrefix());
            Data cacheConfigData = clientContext.getSerializationService().toData(config);
            ClientMessage request = CacheCreateConfigCodec.encodeRequest(cacheConfigData, createAlsoOnOthers);
            ClientInvocation clientInvocation = new ClientInvocation(client, request, partitionId);

            Future<ClientMessage> future = clientInvocation.invoke();
            if (syncCreate) {
                final ClientMessage response = future.get();
                final Data data = CacheCreateConfigCodec.decodeResponse(response).response;
                return (CacheConfig<K, V>) clientContext.getSerializationService().toObject(data);
            } else {
                return currentCacheConfig;
            }
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        if (HazelcastClientCacheManager.class.isAssignableFrom(clazz)) {
            return (T) this;
        }
        throw new IllegalArgumentException();
    }

    protected void postClose() {
        if (properties.getProperty(HazelcastCachingProvider.HAZELCAST_CONFIG_LOCATION) != null) {
            hazelcastInstance.shutdown();
        }
    }

    /**
     * Gets the related {@link NearCacheManager} with the underlying client instance.
     *
     * @return the related {@link NearCacheManager} with the underlying client instance
     */
    public NearCacheManager getNearCacheManager() {
        if (hazelcastInstance instanceof HazelcastClientInstanceImpl) {
            return ((HazelcastClientInstanceImpl) hazelcastInstance).getNearCacheManager();
        } else if (hazelcastInstance instanceof HazelcastClientProxy) {
            HazelcastClientInstanceImpl clientInstance = ((HazelcastClientProxy) hazelcastInstance).client;
            if (clientInstance != null) {
                return clientInstance.getNearCacheManager();
            } else {
                return null;
            }
        } else {
            return null;
        }
    }

}
