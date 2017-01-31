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

import com.hazelcast.cache.CacheStatistics;
import com.hazelcast.cache.impl.event.CacheWanEventPublisher;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.EventFilter;
import com.hazelcast.spi.EventPublishingService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.RemoteService;

import java.util.Collection;

public interface ICacheService
        extends ManagedService,
                RemoteService,
                MigrationAwareService,
                EventPublishingService<Object, CacheEventListener> {

    String CACHE_SUPPORT_NOT_AVAILABLE_ERROR_MESSAGE =
            "There is no valid JCache API library at classpath. "
            + "Please be sure that there is a JCache API library in your classpath "
            + "and it is newer than `0.x` and `1.0.0-PFD` versions!";

    String SERVICE_NAME = "hz:impl:cacheService";

    ICacheRecordStore getOrCreateRecordStore(String name, int partitionId);

    ICacheRecordStore getRecordStore(String name, int partitionId);

    CachePartitionSegment getSegment(int partitionId);

    CacheConfig putCacheConfigIfAbsent(CacheConfig config);

    CacheConfig getCacheConfig(String name);

    CacheConfig findCacheConfig(String simpleName);

    Collection<CacheConfig> getCacheConfigs();

    CacheConfig deleteCacheConfig(String name);

    CacheStatisticsImpl createCacheStatIfAbsent(String name);

    CacheContext getOrCreateCacheContext(String name);

    void deleteCache(String name, String callerUuid, boolean destroy);

    void deleteCacheStat(String name);

    void setStatisticsEnabled(CacheConfig cacheConfig, String cacheNameWithPrefix, boolean enabled);

    void setManagementEnabled(CacheConfig cacheConfig, String cacheNameWithPrefix, boolean enabled);

    void publishEvent(CacheEventContext cacheEventContext);

    void publishEvent(String cacheName, CacheEventSet eventSet, int orderKey);

    NodeEngine getNodeEngine();

    String registerListener(String name, CacheEventListener listener, boolean isLocal);

    String registerListener(String name, CacheEventListener listener, EventFilter eventFilter, boolean isLocal);

    boolean deregisterListener(String name, String registrationId);

    void deregisterAllListener(String name);

    CacheStatistics getStatistics(String name);

    /**
     * Creates cache operations according to the storage-type of the cache
     */
    CacheOperationProvider getCacheOperationProvider(String nameWithPrefix, InMemoryFormat storageType);

    String addInvalidationListener(String name, CacheEventListener listener, boolean localOnly);

    void sendInvalidationEvent(String name, Data key, String sourceUuid);

    boolean isWanReplicationEnabled(String cacheName);

    CacheWanEventPublisher getCacheWanEventPublisher();
}
