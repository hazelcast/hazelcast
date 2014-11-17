/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.EventPublishingService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.RemoteService;

import java.util.Collection;

public interface ICacheService extends ManagedService, RemoteService, MigrationAwareService,
            EventPublishingService<Object, CacheEventListener> {

    /**
     * Service name
     */
    String SERVICE_NAME = "hz:impl:cacheService";

    ICacheRecordStore getOrCreateCache(String name, int partitionId);

    ICacheRecordStore getCacheRecordStore(String name, int partitionId);

    CachePartitionSegment getSegment(int partitionId);

    void destroyCache(String objectName, boolean isLocal, String callerUuid);

    CacheConfig createCacheConfigIfAbsent(CacheConfig config, boolean isLocal);

    CacheConfig deleteCacheConfig(String name);

    CacheStatisticsImpl createCacheStatIfAbsent(String name);

    void deleteCacheStat(String name);

    void setStatisticsEnabled(CacheConfig cacheConfig, String cacheNameWithPrefix, boolean enabled);

    void setManagementEnabled(CacheConfig cacheConfig, String cacheNameWithPrefix, boolean enabled);

    CacheConfig getCacheConfig(String name);

    Collection<CacheConfig> getCacheConfigs();

    void publishEvent(String cacheName, CacheEventType eventType, Data dataKey, Data dataValue, Data dataOldValue,
            boolean isOldValueAvailable, int orderKey, int completionId);

    void publishEvent(String cacheName, CacheEventSet eventSet, int orderKey);

    NodeEngine getNodeEngine();

    String registerListener(String distributedObjectName, CacheEventListener listener);

    boolean deregisterListener(String name, String registrationId);

    void deregisterAllListener(String name);

    CacheStatisticsImpl getStatistics(String name);

    /**
     * Creates cache operations according to the storage-type of the cache
     */
    CacheOperationProvider getCacheOperationProvider(String nameWithPrefix, InMemoryFormat storageType);
}
