/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.cache.impl.journal.CacheEventJournal;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.EventFilter;
import com.hazelcast.spi.EventPublishingService;
import com.hazelcast.spi.FragmentedMigrationAwareService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.RemoteService;

import java.util.Collection;

public interface ICacheService
        extends ManagedService, RemoteService, FragmentedMigrationAwareService,
        EventPublishingService<Object, CacheEventListener> {

    String CACHE_SUPPORT_NOT_AVAILABLE_ERROR_MESSAGE =
            "There is no valid JCache API library at classpath. "
                    + "Please be sure that there is a JCache API library in your classpath "
                    + "and it is newer than `0.x` and `1.0.0-PFD` versions!";

    String SERVICE_NAME = "hz:impl:cacheService";

    /**
     * Maximum retries for adding cache config cluster-wide on stable cluster
     */
    int MAX_ADD_CACHE_CONFIG_RETRIES = 100;

    /**
     * Gets or creates a cache record store with the prefixed {@code cacheNameWithPrefix}
     * and partition ID.
     *
     * @param cacheNameWithPrefix the full name of the {@link com.hazelcast.cache.ICache}, including the manager scope prefix
     * @param partitionId         the record store partition ID
     * @return the cache partition record store
     */
    ICacheRecordStore getOrCreateRecordStore(String cacheNameWithPrefix, int partitionId);

    /**
     * Returns a cache record store with the prefixed {@code cacheNameWithPrefix} and partition ID
     * or {@code null} if one doesn't exist.
     *
     * @param cacheNameWithPrefix the full name of the {@link com.hazelcast.cache.ICache}, including the manager scope prefix
     * @param partitionId         the record store partition ID
     * @return the cache partition record store or {@code null} if it doesn't exist
     */
    ICacheRecordStore getRecordStore(String cacheNameWithPrefix, int partitionId);

    CachePartitionSegment getSegment(int partitionId);

    CacheConfig putCacheConfigIfAbsent(CacheConfig config);

    CacheConfig getCacheConfig(String cacheNameWithPrefix);

    CacheConfig findCacheConfig(String simpleName);

    Collection<CacheConfig> getCacheConfigs();

    CacheConfig deleteCacheConfig(String cacheNameWithPrefix);

    CacheStatisticsImpl createCacheStatIfAbsent(String cacheNameWithPrefix);

    CacheContext getOrCreateCacheContext(String cacheNameWithPrefix);

    void deleteCache(String cacheNameWithPrefix, String callerUuid, boolean destroy);

    void deleteCacheStat(String cacheNameWithPrefix);

    void setStatisticsEnabled(CacheConfig cacheConfig, String cacheNameWithPrefix, boolean enabled);

    void setManagementEnabled(CacheConfig cacheConfig, String cacheNameWithPrefix, boolean enabled);

    void publishEvent(CacheEventContext cacheEventContext);

    void publishEvent(String cacheNameWithPrefix, CacheEventSet eventSet, int orderKey);

    NodeEngine getNodeEngine();

    String registerListener(String cacheNameWithPrefix, CacheEventListener listener, boolean isLocal);

    String registerListener(String cacheNameWithPrefix, CacheEventListener listener, EventFilter eventFilter, boolean isLocal);

    boolean deregisterListener(String cacheNameWithPrefix, String registrationId);

    void deregisterAllListener(String cacheNameWithPrefix);

    CacheStatistics getStatistics(String cacheNameWithPrefix);

    /**
     * Creates cache operations according to the storage-type of the cache
     */
    CacheOperationProvider getCacheOperationProvider(String cacheNameWithPrefix, InMemoryFormat storageType);

    String addInvalidationListener(String cacheNameWithPrefix, CacheEventListener listener, boolean localOnly);

    void sendInvalidationEvent(String cacheNameWithPrefix, Data key, String sourceUuid);

    /**
     * Returns {@code true} if WAN replication is enabled for the cache named {@code cacheNameWithPrefix}.
     *
     * @param cacheNameWithPrefix the full name of the {@link com.hazelcast.cache.ICache}, including the manager scope prefix
     */
    boolean isWanReplicationEnabled(String cacheNameWithPrefix);

    /**
     * Returns the WAN event publisher responsible for publishing
     * primary and backup WAN events for caches.
     */
    CacheWanEventPublisher getCacheWanEventPublisher();

    /**
     * Returns an interface for interacting with the cache event journals.
     */
    CacheEventJournal getEventJournal();

    /**
     * Creates the given CacheConfig on all members of the cluster synchronously. When used with
     * cluster version 3.10 or greater, the cluster-wide invocation ensures that all members of
     * the cluster will receive the cache config even in the face of cluster membership changes.
     *
     * @param cacheConfig   the cache config to create on all members of the cluster
     * @param <K>           key type parameter
     * @param <V>           value type parameter
     * @since 3.10
     */
    <K, V> void createCacheConfigOnAllMembers(PreJoinCacheConfig<K, V> cacheConfig);
}
