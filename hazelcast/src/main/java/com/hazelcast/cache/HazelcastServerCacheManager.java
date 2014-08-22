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
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;

import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.Configuration;
import javax.management.ObjectName;
import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.Set;

public class HazelcastServerCacheManager extends HazelcastCacheManager {

//    protected HazelcastInstanceImpl hazelcastInstance;
//    private HazelcastServerCachingProvider cachingProvider;
    private NodeEngine nodeEngine;
    private CacheService cacheService;

    public HazelcastServerCacheManager(HazelcastServerCachingProvider cachingProvider, HazelcastInstance hazelcastInstance, URI uri, ClassLoader classLoader, Properties properties) {
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
            final CacheConfig _cacheConfig = cacheService.getCacheConfig(cacheNameWithPrefix);
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

                final CacheCreateConfigOperation cacheCreateConfigOperation = new CacheCreateConfigOperation(cacheNameWithPrefix, cacheConfig,true);
                final OperationService operationService = nodeEngine.getOperationService();

                int partitionId = nodeEngine.getPartitionService().getPartitionId(cacheNameWithPrefix);
                final InternalCompletableFuture<Boolean> f = operationService.invokeOnPartition(CacheService.SERVICE_NAME, cacheCreateConfigOperation, partitionId);
                boolean created = f.getSafely();

                //CREATE ON OTHERS TOO
                final Collection<MemberImpl> members = nodeEngine.getClusterService().getMemberList();
                for(MemberImpl member:members){
                    if(!member.localMember()){
                        final CacheCreateConfigOperation op = new CacheCreateConfigOperation(cacheNameWithPrefix, cacheConfig,true);
                        final InternalCompletableFuture<Object> f2 = operationService.invokeOnTarget(CacheService.SERVICE_NAME, op, member.getAddress());
                        f2.getSafely();//make sure all configs are created
                    }
                }
                //UPDATE LOCAL MEMBER
                cacheService.createCacheConfigIfAbsent(cacheConfig);

                final CacheDistributedObject cacheDistributedObject = hazelcastInstance.getDistributedObject(CacheService.SERVICE_NAME, cacheNameWithPrefix);
                final CacheProxy<K, V> cacheProxy = new CacheProxy<K, V>(cacheName, cacheConfig, cacheDistributedObject, this);
                caches.put(cacheNameWithPrefix,cacheProxy);
                if (created) {
                    if(cacheConfig.isStatisticsEnabled()){
                        enableStatistics(cacheName,true);
                    }
                    if(cacheConfig.isManagementEnabled()){
                        enableManagement(cacheName,true);
                    }
                    //REGISTER LISTENERS
                    final Iterator<CacheEntryListenerConfiguration<K, V>> iterator = cacheConfig.getCacheEntryListenerConfigurations().iterator();
                    while (iterator.hasNext()){
                        final CacheEntryListenerConfiguration<K, V> listenerConfig = iterator.next();
                        cacheService.registerCacheEntryListener(cacheProxy,listenerConfig);
                    }
                    return cacheProxy;
                }
            }
            throw new CacheException("A cache named " + cacheName + " already exists.");
        }
    }

    @Override
    public <K, V> ICache<K, V> getCache(String cacheName, Class<K> keyType, Class<V> valueType) {
        if (isClosed()) {
            throw new IllegalStateException();
        }
        if (keyType == null) {
            throw new NullPointerException("keyType can not be null");
        }
        if (valueType == null) {
            throw new NullPointerException("valueType can not be null");
        }
        synchronized (caches) {
            final String cacheNameWithPrefix = getCacheNameWithPrefix(cacheName);
            ICache<?, ?> cache = caches.get(cacheNameWithPrefix);

            Configuration<?, ?> configuration = null;
            if(cache != null) {
                //local check
                configuration = cache.getConfiguration(CacheConfig.class);
            } else {
                //remote check
                final CacheGetConfigOperation cacheCreateConfigOperation = new CacheGetConfigOperation(cacheNameWithPrefix);
                int partitionId = nodeEngine.getPartitionService().getPartitionId(cacheNameWithPrefix);
                final InternalCompletableFuture<CacheConfig> f = nodeEngine.getOperationService()
                        .invokeOnPartition(CacheService.SERVICE_NAME, cacheCreateConfigOperation, partitionId);
                configuration = f.getSafely();
            }
            if (configuration == null){
                //no cache found
                return null;
            }
            if (configuration.getKeyType() != null && configuration.getKeyType().equals(keyType)) {
                if (configuration.getValueType() != null && configuration.getValueType().equals(valueType)) {
                    return (ICache<K, V>) cache;
                } else {
                    throw new ClassCastException("Incompatible cache value types specified, expected " +
                            configuration.getValueType() + " but " + valueType + " was specified");
                }
            } else {
                throw new ClassCastException("Incompatible cache key types specified, expected " +
                        configuration.getKeyType() + " but " + keyType + " was specified");
            }
        }
    }

    @Override
    public void destroyCache(String cacheName) {
        if (isClosed()) {
            throw new IllegalStateException();
        }
        if (cacheName == null) {
            throw new NullPointerException();
        }

        final String cacheNameWithPrefix = getCacheNameWithPrefix(cacheName);
        synchronized (caches) {
            final ICache<?, ?> destroy = caches.remove(cacheNameWithPrefix);
            if(destroy != null){
                destroy.close();
            }
        }

//        HazelcastInstance hz = hazelcastInstance;
//        DistributedObject cache = hz.getDistributedObject(CacheService.SERVICE_NAME, cacheNameWithPrefix);
//        cache.destroy();
    }

    @Override
    public Iterable<String> getCacheNames() {
        Set<String> names;
        if (isClosed()) {
            names = Collections.emptySet();
        } else {
            names = new LinkedHashSet<String>();
            for(String nameWithPrefix:cacheService.getCacheNames()){
                final String name = nameWithPrefix.substring(nameWithPrefix.indexOf(cacheNamePrefix)+cacheNamePrefix.length());
                names.add(name);
            }
        }
        return Collections.unmodifiableCollection(names);
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
        enableStatisticManagementOnOtherNodes(uri, cacheName, false, enabled);
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
        enableStatisticManagementOnOtherNodes(uri, cacheName, true, enabled);
    }

    private void enableStatisticManagementOnOtherNodes(URI uri, String cacheName, boolean statOrMan, boolean enabled){
        final String cacheNameWithPrefix = getCacheNameWithPrefix(cacheName);
        final Collection<MemberImpl> members = nodeEngine.getClusterService().getMemberList();
        for(MemberImpl member:members){
            if(!member.localMember()){
                final CacheManagementConfigOperation op = new CacheManagementConfigOperation(cacheNameWithPrefix, statOrMan, enabled);
                nodeEngine.getOperationService().invokeOnTarget(CacheService.SERVICE_NAME, op, member.getAddress());
            }
        }
    }

    @Override
    public void close() {
        if(!closeTriggered){
            releaseCacheManager(uri, classLoaderReference.get());

            HazelcastInstance hz = hazelcastInstance;
            Collection<DistributedObject> distributedObjects = hz.getDistributedObjects();
            for (DistributedObject distributedObject : distributedObjects) {
                if (distributedObject instanceof CacheDistributedObject) {
                    distributedObject.destroy();
                }
            }
            for(ICache cache:caches.values()){
                cache.close();
            }
            caches.clear();
        }
        closeTriggered=true;
    }

    protected void  releaseCacheManager(URI uri, ClassLoader classLoader){
        ((HazelcastAbstractCachingProvider)cachingProvider).releaseCacheManager(uri,classLoader);
    }

    public HazelcastServerCachingProvider getCachingProvider(){
        return (HazelcastServerCachingProvider) cachingProvider;
    }


}
