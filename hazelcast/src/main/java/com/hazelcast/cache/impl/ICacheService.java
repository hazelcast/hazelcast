/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cache.impl.event.CacheWanEventPublisher;
import com.hazelcast.cache.impl.journal.CacheEventJournal;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.internal.eviction.ExpirationManager;
import com.hazelcast.internal.metrics.DynamicMetricsProvider;
import com.hazelcast.internal.monitor.LocalCacheStats;
import com.hazelcast.internal.partition.ChunkedMigrationAwareService;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.services.ManagedService;
import com.hazelcast.internal.services.RemoteService;
import com.hazelcast.internal.services.StatisticsAwareService;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.eventservice.EventFilter;
import com.hazelcast.spi.impl.eventservice.EventPublishingService;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@SuppressWarnings({"checkstyle:methodcount"})
public interface ICacheService
        extends ManagedService, RemoteService, ChunkedMigrationAwareService,
        EventPublishingService<Object, CacheEventListener>,
        StatisticsAwareService<LocalCacheStats>, DynamicMetricsProvider {

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

    CachePartitionSegment[] getPartitionSegments();

    CacheStatisticsImpl createCacheStatIfAbsent(String cacheNameWithPrefix);

    CacheContext getOrCreateCacheContext(String cacheNameWithPrefix);

    void deleteCache(String cacheNameWithPrefix, UUID callerUuid, boolean destroy);

    void deleteCacheStat(String cacheNameWithPrefix);

    void setStatisticsEnabled(CacheConfig cacheConfig, String cacheNameWithPrefix, boolean enabled);

    void setManagementEnabled(CacheConfig cacheConfig, String cacheNameWithPrefix, boolean enabled);

    void publishEvent(CacheEventContext cacheEventContext);

    void publishEvent(String cacheNameWithPrefix, CacheEventSet eventSet, int orderKey);

    NodeEngine getNodeEngine();

    UUID registerLocalListener(String cacheNameWithPrefix, CacheEventListener listener);

    UUID registerLocalListener(String cacheNameWithPrefix, CacheEventListener listener, EventFilter eventFilter);

    UUID registerListener(String cacheNameWithPrefix, CacheEventListener listener);

    UUID registerListener(String cacheNameWithPrefix, CacheEventListener listener, EventFilter eventFilter);

    CompletableFuture<UUID> registerListenerAsync(String cacheNameWithPrefix, CacheEventListener listener);

    CompletableFuture<UUID> registerListenerAsync(String cacheNameWithPrefix, CacheEventListener listener,
                                                  EventFilter eventFilter);

    boolean deregisterListener(String cacheNameWithPrefix, UUID registrationId);

    CompletableFuture<Boolean> deregisterListenerAsync(String cacheNameWithPrefix, UUID registrationId);

    void deregisterAllListener(String cacheNameWithPrefix);

    ExpirationManager getExpirationManager();

    /**
     * Creates cache operations according to the storage-type of the cache
     */
    CacheOperationProvider getCacheOperationProvider(String cacheNameWithPrefix, InMemoryFormat storageType);

    void sendInvalidationEvent(String cacheNameWithPrefix, Data key, UUID sourceUuid);

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
     * @param cacheNameWithPrefix the full name of the {@link
     *                            com.hazelcast.cache.ICache}, including the manager scope prefix
     */
    void doPrepublicationChecks(String cacheNameWithPrefix);

    /**
     * Returns an interface for interacting with the cache event journals.
     */
    CacheEventJournal getEventJournal();

    /**
     * Creates the given CacheConfig on all members of the cluster synchronously. When used with
     * cluster version 3.10 or greater, the cluster-wide invocation ensures that all members of
     * the cluster will receive the cache config even in the face of cluster membership changes.
     *
     * @param cacheConfig the cache config to create on all members of the cluster
     * @param <K>         key type parameter
     * @param <V>         value type parameter
     * @since 3.10
     */
    <K, V> void createCacheConfigOnAllMembers(PreJoinCacheConfig<K, V> cacheConfig);
}
