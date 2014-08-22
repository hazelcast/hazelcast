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
import com.hazelcast.client.proxy.ClientCacheProxy;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;

import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.Configuration;
import java.net.URI;
import java.util.Properties;

public final class HazelcastClientCacheManager extends HazelcastCacheManager{

    public HazelcastClientCacheManager(HazelcastClientCachingProvider cachingProvider, HazelcastInstance hazelcastInstance, URI uri, ClassLoader classLoader, Properties properties) {
        super(cachingProvider, uri, classLoader, properties);
        if (hazelcastInstance == null) {
            throw new NullPointerException("hazelcastInstance missing");
        }
        this.hazelcastInstance = hazelcastInstance;

    }

    @Override
    public <K, V, C extends Configuration<K, V>> Cache<K, V> createCache(String cacheName, C configuration) throws IllegalArgumentException {
        if (isClosed()) {
            throw new IllegalStateException();
        }
        if (cacheName == null) {
            throw new NullPointerException("cacheName must not be null");
        }
        if (configuration == null) {
            throw new NullPointerException("configuration must not be null");
        }
        synchronized (caches) {
            final String cacheNameWithPrefix = getCacheNameWithPrefix(cacheName);
            final CacheConfig _cacheConfig = getCacheConfig(cacheNameWithPrefix);
            if(_cacheConfig == null){
                final CacheConfig<K,V> cacheConfig;
                if (configuration instanceof CompleteConfiguration) {
                    cacheConfig = new CacheConfig<K, V>((CompleteConfiguration) configuration);
                } else {
                    cacheConfig = new CacheConfig<K, V>();
                    cacheConfig.setStoreByValue(configuration.isStoreByValue());
                    cacheConfig.setTypes(configuration.getKeyType(), configuration.getValueType());
                }
                cacheConfig.setName(cacheName);
                cacheConfig.setManagerPrefix(this.cacheNamePrefix);
                cacheConfig.setUriString(getURI().toString());


                //CREATE THE CONFIG ON PARTITION BY cacheNamePrefix using a request
                boolean created = createConfigOnPartition(cacheConfig,cacheNameWithPrefix);

                //CREATE CONFIG ON OTHER MEMBERS TOO
                createConfigOnAllMembers(cacheConfig,cacheNameWithPrefix);


                //ADD CONFIG TO CLIENT LOCAL
                createCacheConfigIfAbsent(cacheConfig,cacheNameWithPrefix);

                //Create cache client Proxy
                final ClientProxy distributedObject = hazelcastInstance.getDistributedObject(CacheService.SERVICE_NAME, cacheNameWithPrefix);
                final ClientCacheProxy<K, V> cacheProxy = new ClientCacheProxy<K, V>(cacheName, cacheConfig, distributedObject, this);

                caches.put(cacheNameWithPrefix,cacheProxy);
                if (created) {
                    if(cacheConfig.isStatisticsEnabled()){
                        enableStatistics(cacheName,true);
                    }
                    if(cacheConfig.isManagementEnabled()){
                        enableManagement(cacheName,true);
                    }
                    //REGISTER LISTENERS
//                    final Iterator<CacheEntryListenerConfiguration<K, V>> iterator = cacheConfig.getCacheEntryListenerConfigurations().iterator();
//                    while (iterator.hasNext()){
//                        final CacheEntryListenerConfiguration<K, V> listenerConfig = iterator.next();
//                        cacheService.registerCacheEntryListener(cacheProxy,listenerConfig);
//                    }
                    return cacheProxy;
                }
            }
            throw new CacheException("A cache named " + cacheName + " already exists.");
        }
    }

    private <K, V> void createCacheConfigIfAbsent(CacheConfig<K, V> cacheConfig, String cacheNameWithPrefix) {

    }

    private <K, V> void createConfigOnAllMembers(CacheConfig<K, V> cacheConfig, String cacheNameWithPrefix) {

    }

    private <K, V> boolean createConfigOnPartition(CacheConfig<K, V> cacheConfig, String cacheNameWithPrefix) {
        return false;
    }

    private CacheConfig getCacheConfig(String cacheNameWithPrefix) {
        return null;
    }

    @Override
    public <K, V> Cache<K, V> getCache(String cacheName, Class<K> keyType, Class<V> valueType) {
        return null;
    }

    @Override
    public Iterable<String> getCacheNames() {
        return null;
    }

    @Override
    public void destroyCache(String cacheName) {

    }

    @Override
    public void enableManagement(String cacheName, boolean enabled) {

    }

    @Override
    public void enableStatistics(String cacheName, boolean enabled) {

    }

    @Override
    public void close() {

    }


}
