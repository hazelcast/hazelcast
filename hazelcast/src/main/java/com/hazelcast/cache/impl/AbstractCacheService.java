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

import com.hazelcast.cache.CacheNotExistsException;
import com.hazelcast.cache.HazelcastCacheManager;
import com.hazelcast.cache.impl.event.CachePartitionLostEventFilter;
import com.hazelcast.cache.impl.eviction.CacheClearExpiredRecordsTask;
import com.hazelcast.cache.impl.journal.CacheEventJournal;
import com.hazelcast.cache.impl.journal.RingbufferCacheEventJournalImpl;
import com.hazelcast.cache.impl.operation.AddCacheConfigOperationSupplier;
import com.hazelcast.cache.impl.operation.OnJoinCacheOperation;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.cluster.Member;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.CacheConfigAccessor;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.internal.cluster.ClusterStateListener;
import com.hazelcast.internal.eviction.ExpirationManager;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.internal.monitor.LocalCacheStats;
import com.hazelcast.internal.monitor.impl.LocalCacheStatsImpl;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.partition.IPartitionLostEvent;
import com.hazelcast.internal.partition.MigrationEndpoint;
import com.hazelcast.internal.partition.PartitionAwareService;
import com.hazelcast.internal.partition.PartitionMigrationEvent;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.services.PreJoinAwareService;
import com.hazelcast.internal.services.SplitBrainHandlerService;
import com.hazelcast.internal.services.SplitBrainProtectionAwareService;
import com.hazelcast.internal.services.TenantContextAwareService;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.internal.util.ConcurrencyUtil;
import com.hazelcast.internal.util.ConstructorFunction;
import com.hazelcast.internal.util.ContextMutexFactory;
import com.hazelcast.internal.util.FutureUtil;
import com.hazelcast.internal.util.InvocationUtil;
import com.hazelcast.internal.util.MapUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.eventservice.EventFilter;
import com.hazelcast.spi.impl.eventservice.EventRegistration;
import com.hazelcast.spi.impl.eventservice.EventService;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergePolicyProvider;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.wan.impl.WanReplicationService;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.cache.CacheException;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.event.CacheEntryListener;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.cache.impl.AbstractCacheRecordStore.SOURCE_NOT_AVAILABLE;
import static com.hazelcast.cache.impl.PreJoinCacheConfig.asCacheConfig;
import static com.hazelcast.internal.config.ConfigValidator.checkCacheConfig;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CACHE_PREFIX;
import static com.hazelcast.internal.metrics.impl.ProviderHelper.provide;
import static com.hazelcast.internal.util.ConcurrencyUtil.CALLER_RUNS;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.FutureUtil.RETHROW_EVERYTHING;
import static com.hazelcast.internal.util.MapUtil.createHashMap;
import static java.util.Collections.newSetFromMap;
import static java.util.Collections.singleton;

@SuppressWarnings("checkstyle:classdataabstractioncoupling")
public abstract class AbstractCacheService implements ICacheService,
        PreJoinAwareService, PartitionAwareService,
        SplitBrainProtectionAwareService, SplitBrainHandlerService,
        ClusterStateListener, TenantContextAwareService {
    /**
     * Map from full prefixed cache name to {@link CacheConfig}
     */
    protected final ConcurrentMap<String, CompletableFuture<CacheConfig>> configs = new ConcurrentHashMap<>();

    /**
     * Map from full prefixed cache name to {@link CacheContext}
     */
    protected final ConcurrentMap<String, CacheContext> cacheContexts = new ConcurrentHashMap<>();

    /**
     * Map from full prefixed cache name to {@link CacheStatisticsImpl}
     */
    protected final ConcurrentMap<String, CacheStatisticsImpl> statistics = new ConcurrentHashMap<>();

    /**
     * Map from full prefixed cache name to set of {@link Closeable} resources
     */
    protected final ConcurrentMap<String, Set<Closeable>> resources = new ConcurrentHashMap<>();
    protected final ConcurrentMap<UUID, Closeable> closeableListeners = new ConcurrentHashMap<>();
    protected final ConcurrentMap<String, CacheOperationProvider> operationProviderCache =
            new ConcurrentHashMap<>();

    protected final ConstructorFunction<String, CacheContext> cacheContextsConstructorFunction = name -> new CacheContext();
    protected final ConstructorFunction<String, CacheStatisticsImpl> cacheStatisticsConstructorFunction =
            name -> new CacheStatisticsImpl(
                    Clock.currentTimeMillis(),
                    CacheEntryCountResolver.createEntryCountResolver(getOrCreateCacheContext(name)));

    protected final ConstructorFunction<String, Set<Closeable>> cacheResourcesConstructorFunction =
            name -> newSetFromMap(new ConcurrentHashMap<Closeable, Boolean>());

    // mutex factory ensures each Set<Closeable> of cache resources is only constructed and inserted in resources map once
    protected final ContextMutexFactory cacheResourcesMutexFactory = new ContextMutexFactory();

    protected ILogger logger;
    protected NodeEngine nodeEngine;
    protected CachePartitionSegment[] segments;
    protected CacheEventHandler cacheEventHandler;
    protected RingbufferCacheEventJournalImpl eventJournal;
    protected SplitBrainMergePolicyProvider mergePolicyProvider;
    protected CacheSplitBrainHandlerService splitBrainHandlerService;
    protected CacheClearExpiredRecordsTask clearExpiredRecordsTask;
    protected ExpirationManager expirationManager;

    @Override
    public final void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = nodeEngine;
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        this.segments = new CachePartitionSegment[partitionCount];
        for (int i = 0; i < partitionCount; i++) {
            segments[i] = newPartitionSegment(i);
        }
        this.clearExpiredRecordsTask = new CacheClearExpiredRecordsTask(this.segments, nodeEngine);
        this.expirationManager = new ExpirationManager(this.clearExpiredRecordsTask, nodeEngine);
        this.cacheEventHandler = new CacheEventHandler(nodeEngine);
        this.splitBrainHandlerService = new CacheSplitBrainHandlerService(nodeEngine, segments);
        this.logger = nodeEngine.getLogger(getClass());
        this.eventJournal = new RingbufferCacheEventJournalImpl(nodeEngine);
        this.mergePolicyProvider = nodeEngine.getSplitBrainMergePolicyProvider();

        boolean dsMetricsEnabled = nodeEngine.getProperties().getBoolean(ClusterProperty.METRICS_DATASTRUCTURES);
        postInit(nodeEngine, properties, dsMetricsEnabled);
    }

    public SplitBrainMergePolicyProvider getMergePolicyProvider() {
        return mergePolicyProvider;
    }

    public SplitBrainMergePolicy getMergePolicy(String dataStructureName) {
        CacheConfig cacheConfig = getCacheConfig(dataStructureName);
        String mergePolicyName = cacheConfig.getMergePolicyConfig().getPolicy();
        return mergePolicyProvider.getMergePolicy(mergePolicyName);
    }

    public ConcurrentMap<String, CacheConfig> getConfigs() {
        ConcurrentMap<String, CacheConfig> cacheConfigs = MapUtil.createConcurrentHashMap(configs.size());
        for (Map.Entry<String, CompletableFuture<CacheConfig>> config : configs.entrySet()) {
            cacheConfigs.put(config.getKey(), config.getValue().join());
        }
        return cacheConfigs;
    }

    protected void postInit(NodeEngine nodeEngine, Properties properties, boolean metricsEnabled) {
        if (metricsEnabled) {
            ((NodeEngineImpl) nodeEngine).getMetricsRegistry().registerDynamicMetricsProvider(this);
        }
    }

    protected abstract CachePartitionSegment newPartitionSegment(int partitionId);

    protected abstract ICacheRecordStore createNewRecordStore(String cacheNameWithPrefix, int partitionId);

    @Override
    public void reset() {
        reset(false);
    }

    private void reset(boolean onShutdown) {
        for (String objectName : configs.keySet()) {
            deleteCache(objectName, null, false);
        }
        CachePartitionSegment[] partitionSegments = segments;
        for (CachePartitionSegment partitionSegment : partitionSegments) {
            if (partitionSegment != null) {
                if (onShutdown) {
                    partitionSegment.shutdown();
                } else {
                    partitionSegment.reset();
                    partitionSegment.init();
                }
            }
        }

        for (String objectName : configs.keySet()) {
            sendInvalidationEvent(objectName, null, SOURCE_NOT_AVAILABLE);
        }
    }

    @Override
    public void shutdown(boolean terminate) {
        if (!terminate) {
            expirationManager.onShutdown();
            cacheEventHandler.shutdown();
            reset(true);
        }
    }

    @Override
    @SuppressFBWarnings({"EI_EXPOSE_REP"})
    public CachePartitionSegment[] getPartitionSegments() {
        return segments;
    }

    @Override
    public DistributedObject createDistributedObject(String cacheNameWithPrefix, UUID source, boolean local) {
        try {
            /*
             * In here, cacheNameWithPrefix is the full cache name.
             * Full cache name contains, Hazelcast prefix, cache name prefix and pure cache name.
             */
            // At first, lookup cache name in the created cache configs.
            CacheConfig cacheConfig = getCacheConfig(cacheNameWithPrefix);
            if (cacheConfig == null) {
                /*
                 * Prefixed cache name contains cache name prefix and pure cache name, but not Hazelcast prefix (`/hz/`).
                 * Cache name prefix is generated by using specified URI and classloader scopes.
                 * This means, if there is no specified URI and classloader, prefixed cache name is pure cache name.
                 * This means, if there is no specified URI and classloader, prefixed cache name is pure cache name.
                 */
                // If cache config is not created yet, remove Hazelcast prefix and get prefixed cache name.
                String cacheName = cacheNameWithPrefix.substring(HazelcastCacheManager.CACHE_MANAGER_PREFIX.length());
                // Lookup prefixed cache name in the config.
                cacheConfig = findCacheConfig(cacheName);
                if (cacheConfig == null) {
                    throw new CacheNotExistsException("Couldn't find cache config with name " + cacheNameWithPrefix);
                }
                cacheConfig.setManagerPrefix(HazelcastCacheManager.CACHE_MANAGER_PREFIX);
            }

            checkCacheConfig(cacheConfig, mergePolicyProvider);

            if (putCacheConfigIfAbsent(cacheConfig) == null && !local) {
                // if the cache config was not previously known, ensure the new cache config
                // becomes available on all members before the proxy is returned to the caller
                createCacheConfigOnAllMembers(PreJoinCacheConfig.of(cacheConfig));
            }

            return new CacheProxy(cacheConfig, nodeEngine, this);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    @Override
    public void destroyDistributedObject(String objectName, boolean local) {
        deleteCache(objectName, null, true);
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent event) {
    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
            clearCachesHavingLesserBackupCountThan(event.getPartitionId(), event.getNewReplicaIndex());
        }
        initPartitionReplica(event.getPartitionId());
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            clearCachesHavingLesserBackupCountThan(event.getPartitionId(), event.getCurrentReplicaIndex());
        }
        initPartitionReplica(event.getPartitionId());
    }

    private void clearCachesHavingLesserBackupCountThan(int partitionId, int thresholdReplicaIndex) {
        if (thresholdReplicaIndex == -1) {
            clearPartitionReplica(partitionId);
            return;
        }

        CachePartitionSegment segment = segments[partitionId];
        segment.clearHavingLesserBackupCountThan(thresholdReplicaIndex);
    }

    private void initPartitionReplica(int partitionId) {
        segments[partitionId].init();
    }

    private void clearPartitionReplica(int partitionId) {
        segments[partitionId].reset();
    }

    @Override
    public ICacheRecordStore getOrCreateRecordStore(String cacheNameWithPrefix, int partitionId) {
        return segments[partitionId].getOrCreateRecordStore(cacheNameWithPrefix);
    }

    @Override
    public ICacheRecordStore getRecordStore(String cacheNameWithPrefix, int partitionId) {
        return segments[partitionId].getRecordStore(cacheNameWithPrefix);
    }

    @Override
    public CachePartitionSegment getSegment(int partitionId) {
        return segments[partitionId];
    }

    protected void destroySegments(CacheConfig cacheConfig) {
        String name = cacheConfig.getNameWithPrefix();
        for (CachePartitionSegment segment : segments) {
            segment.deleteRecordStore(name, true);
        }
    }

    protected void closeSegments(String name) {
        for (CachePartitionSegment segment : segments) {
            segment.deleteRecordStore(name, false);
        }
    }

    @Override
    public void deleteCache(String cacheNameWithPrefix, UUID callerUuid, boolean destroy) {
        CacheConfig config = deleteCacheConfig(cacheNameWithPrefix);
        if (config == null) {
            // Cache is already cleaned up
            return;
        }
        if (destroy) {
            cacheEventHandler.destroy(cacheNameWithPrefix, SOURCE_NOT_AVAILABLE);
            destroySegments(config);
        } else {
            closeSegments(cacheNameWithPrefix);
        }

        WanReplicationService wanService = nodeEngine.getWanReplicationService();
        wanService.removeWanEventCounters(ICacheService.SERVICE_NAME, cacheNameWithPrefix);
        cacheContexts.remove(cacheNameWithPrefix);
        operationProviderCache.remove(cacheNameWithPrefix);
        deregisterAllListener(cacheNameWithPrefix);
        setStatisticsEnabled(config, cacheNameWithPrefix, false);
        setManagementEnabled(config, cacheNameWithPrefix, false);
        deleteCacheStat(cacheNameWithPrefix);
        deleteCacheResources(cacheNameWithPrefix);
    }

    @Override
    public CacheConfig putCacheConfigIfAbsent(CacheConfig config) {
        // ensure all configs registered in CacheService are not PreJoinCacheConfig's
        CacheConfig cacheConfig = asCacheConfig(config);
        CompletableFuture<CacheConfig> future = new CompletableFuture<>();
        CompletableFuture<CacheConfig> localConfigFuture = configs.putIfAbsent(cacheConfig.getNameWithPrefix(), future);
        // if the existing cache config future is not yet fully configured, we block here
        CacheConfig localConfig = localConfigFuture == null ? null : localConfigFuture.join();
        if (localConfigFuture == null) {
            try {
                if (cacheConfig.isStatisticsEnabled()) {
                    setStatisticsEnabled(cacheConfig, cacheConfig.getNameWithPrefix(), true);
                }
                if (cacheConfig.isManagementEnabled()) {
                    setManagementEnabled(cacheConfig, cacheConfig.getNameWithPrefix(), true);
                }
                logger.info("Added cache config: " + cacheConfig);
                additionalCacheConfigSetup(config, false);
                // now it is safe for others to obtain the new cache config
                future.complete(cacheConfig);
            } catch (Throwable e) {
                configs.remove(cacheConfig.getNameWithPrefix(), future);
                future.completeExceptionally(e);
                throw rethrow(e);
            }
        } else {
            additionalCacheConfigSetup(localConfig, true);
        }
        return localConfig;
    }

    protected void additionalCacheConfigSetup(CacheConfig config, boolean existingConfig) {
        // overridden in other context
    }

    @Override
    public CacheConfig deleteCacheConfig(String cacheNameWithPrefix) {
        CompletableFuture<CacheConfig> cacheConfigFuture = configs.remove(cacheNameWithPrefix);
        CacheConfig cacheConfig = null;
        if (cacheConfigFuture != null) {
            cacheConfig = cacheConfigFuture.join();
            logger.info("Removed cache config: " + cacheConfig);
        }
        return cacheConfig;
    }

    @Override
    public ExpirationManager getExpirationManager() {
        return expirationManager;
    }

    @Override
    public CacheStatisticsImpl createCacheStatIfAbsent(String cacheNameWithPrefix) {
        return ConcurrencyUtil.getOrPutIfAbsent(statistics, cacheNameWithPrefix, cacheStatisticsConstructorFunction);
    }

    public CacheContext getCacheContext(String name) {
        return cacheContexts.get(name);
    }

    @Override
    public CacheContext getOrCreateCacheContext(String cacheNameWithPrefix) {
        return ConcurrencyUtil.getOrPutIfAbsent(cacheContexts, cacheNameWithPrefix, cacheContextsConstructorFunction);
    }

    @Override
    public void deleteCacheStat(String cacheNameWithPrefix) {
        statistics.remove(cacheNameWithPrefix);
    }

    @Override
    public void setStatisticsEnabled(CacheConfig cacheConfig, String cacheNameWithPrefix, boolean enabled) {
        cacheConfig = cacheConfig != null ? cacheConfig : getCacheConfig(cacheNameWithPrefix);
        if (cacheConfig != null) {
            String cacheManagerName = cacheConfig.getUriString();
            cacheConfig.setStatisticsEnabled(enabled);
            if (enabled) {
                CacheStatisticsImpl cacheStatistics = createCacheStatIfAbsent(cacheNameWithPrefix);
                CacheStatisticsMXBeanImpl mxBean = new CacheStatisticsMXBeanImpl(cacheStatistics);
                MXBeanUtil.registerCacheObject(mxBean, cacheManagerName, cacheConfig.getName(), true);
            } else {
                MXBeanUtil.unregisterCacheObject(cacheManagerName, cacheConfig.getName(), true);
                deleteCacheStat(cacheNameWithPrefix);
            }
        }
    }

    @Override
    public void setManagementEnabled(CacheConfig cacheConfig, String cacheNameWithPrefix, boolean enabled) {
        cacheConfig = cacheConfig != null ? cacheConfig : getCacheConfig(cacheNameWithPrefix);
        if (cacheConfig != null) {
            String cacheManagerName = cacheConfig.getUriString();
            cacheConfig.setManagementEnabled(enabled);
            if (enabled) {
                CacheMXBeanImpl mxBean = new CacheMXBeanImpl(cacheConfig);
                MXBeanUtil.registerCacheObject(mxBean, cacheManagerName, cacheConfig.getName(), false);
            } else {
                MXBeanUtil.unregisterCacheObject(cacheManagerName, cacheConfig.getName(), false);
                deleteCacheStat(cacheNameWithPrefix);
            }
        }
    }

    @Override
    public CacheConfig getCacheConfig(String cacheNameWithPrefix) {
        CompletableFuture<CacheConfig> future = configs.get(cacheNameWithPrefix);
        return future == null ? null : future.join();
    }

    @Override
    public CacheConfig findCacheConfig(String simpleName) {
        if (simpleName == null) {
            return null;
        }
        CacheSimpleConfig cacheSimpleConfig = nodeEngine.getConfig().findCacheConfigOrNull(simpleName);
        if (cacheSimpleConfig == null) {
            return null;
        }
        try {
            // Set name explicitly, because found config might have a wildcard name.
            CacheConfig cacheConfig = new CacheConfig(cacheSimpleConfig).setName(simpleName);
            CacheConfigAccessor.setSerializationService(cacheConfig,
                    nodeEngine.getSerializationService());
            return cacheConfig;
        } catch (Exception e) {
            throw new CacheException(e);
        }
    }

    public <K, V> CacheConfig<K, V> reSerializeCacheConfig(CacheConfig<K, V> cacheConfig) {
        CacheConfig<K, V> serializedCacheConfig = PreJoinCacheConfig.of(cacheConfig).asCacheConfig();

        CompletableFuture<CacheConfig> future = new CompletableFuture<>();
        future.complete(serializedCacheConfig);
        configs.replace(cacheConfig.getNameWithPrefix(), future);

        return serializedCacheConfig;
    }

    @Override
    public Collection<CacheConfig> getCacheConfigs() {
        List<CacheConfig> cacheConfigs = new ArrayList<>(configs.size());
        for (CompletableFuture<CacheConfig> future : configs.values()) {
            cacheConfigs.add(future.join());
        }
        return cacheConfigs;
    }

    public Object toObject(Object data) {
        if (data == null) {
            return null;
        }
        if (data instanceof Data) {
            return nodeEngine.toObject(data);
        } else {
            return data;
        }
    }

    public Data toData(Object object) {
        if (object == null) {
            return null;
        }
        if (object instanceof Data) {
            return (Data) object;
        } else {
            return nodeEngine.getSerializationService().toData(object);
        }
    }

    @Override
    public void publishEvent(CacheEventContext cacheEventContext) {
        cacheEventHandler.publishEvent(cacheEventContext);
    }

    @Override
    public void publishEvent(String cacheNameWithPrefix, CacheEventSet eventSet, int orderKey) {
        cacheEventHandler.publishEvent(cacheNameWithPrefix, eventSet, orderKey);
    }

    @Override
    public NodeEngine getNodeEngine() {
        return nodeEngine;
    }

    @Override
    public void dispatchEvent(Object event, CacheEventListener listener) {
        listener.handleEvent(event);
    }

    @Override
    public UUID registerLocalListener(String cacheNameWithPrefix, CacheEventListener listener) {
        EventService eventService = getNodeEngine().getEventService();

        EventRegistration registration = eventService
                .registerLocalListener(AbstractCacheService.SERVICE_NAME, cacheNameWithPrefix, listener);
        if (registration == null) {
            return null;
        }
        return updateRegisteredListeners(listener, registration);
    }

    @Override
    public UUID registerLocalListener(String cacheNameWithPrefix, CacheEventListener listener, EventFilter eventFilter) {
        EventService eventService = getNodeEngine().getEventService();

        EventRegistration registration = eventService
                .registerLocalListener(AbstractCacheService.SERVICE_NAME, cacheNameWithPrefix, eventFilter, listener);
        if (registration == null) {
            return null;
        }
        return updateRegisteredListeners(listener, registration);
    }

    @Override
    public CompletableFuture<UUID> registerListenerAsync(String cacheNameWithPrefix, CacheEventListener listener) {
        EventService eventService = getNodeEngine().getEventService();

        return eventService.registerListenerAsync(AbstractCacheService.SERVICE_NAME, cacheNameWithPrefix, listener)
                .thenApplyAsync((eventRegistration) -> updateRegisteredListeners(listener, eventRegistration),
                        CALLER_RUNS);
    }

    @Override
    public CompletableFuture<UUID> registerListenerAsync(String cacheNameWithPrefix, CacheEventListener listener,
                                                         EventFilter eventFilter) {
        EventService eventService = getNodeEngine().getEventService();

        return eventService.registerListenerAsync(AbstractCacheService.SERVICE_NAME, cacheNameWithPrefix, eventFilter, listener)
                .thenApplyAsync((eventRegistration) -> updateRegisteredListeners(listener, eventRegistration),
                        CALLER_RUNS);
    }

    private UUID updateRegisteredListeners(CacheEventListener listener, EventRegistration eventRegistration) {
        UUID id = eventRegistration.getId();
        if (listener instanceof Closeable) {
            closeableListeners.put(id, (Closeable) listener);
        } else if (listener instanceof CacheEntryListenerProvider) {
            CacheEntryListener cacheEntryListener = ((CacheEntryListenerProvider) listener)
                    .getCacheEntryListener();
            if (cacheEntryListener instanceof Closeable) {
                closeableListeners.put(id, (Closeable) cacheEntryListener);
            }
        }
        return id;
    }

    @Override
    public UUID registerListener(String cacheNameWithPrefix, CacheEventListener listener) {
        EventService eventService = getNodeEngine().getEventService();

        EventRegistration registration = eventService
                .registerListener(AbstractCacheService.SERVICE_NAME, cacheNameWithPrefix, listener);

        return updateRegisteredListeners(listener, registration);
    }

    @Override
    public UUID registerListener(String cacheNameWithPrefix, CacheEventListener listener, EventFilter eventFilter) {
        EventService eventService = getNodeEngine().getEventService();

        EventRegistration registration = eventService
                .registerListener(AbstractCacheService.SERVICE_NAME, cacheNameWithPrefix, eventFilter, listener);

        return updateRegisteredListeners(listener, registration);
    }

    @Override
    public CompletableFuture<Boolean> deregisterListenerAsync(String cacheNameWithPrefix, UUID registrationId) {
        EventService eventService = getNodeEngine().getEventService();

        return eventService.deregisterListenerAsync(AbstractCacheService.SERVICE_NAME, cacheNameWithPrefix, registrationId)
                .thenApplyAsync(result -> {
                    removeFromLocalResources(registrationId);
                    return result;
                }, CALLER_RUNS);
    }

    private void removeFromLocalResources(UUID registrationId) {
        Closeable listener = closeableListeners.remove(registrationId);
        if (listener != null) {
            IOUtil.closeResource(listener);
        }
    }

    @Override
    public boolean deregisterListener(String cacheNameWithPrefix, UUID registrationId) {
        EventService eventService = getNodeEngine().getEventService();

        if (eventService.deregisterListener(AbstractCacheService.SERVICE_NAME, cacheNameWithPrefix, registrationId)) {
            removeFromLocalResources(registrationId);
            return true;
        }

        return false;
    }

    @Override
    public void deregisterAllListener(String cacheNameWithPrefix) {
        EventService eventService = getNodeEngine().getEventService();
        Collection<EventRegistration> registrations = eventService.getRegistrations(SERVICE_NAME, cacheNameWithPrefix);
        if (registrations != null) {
            for (EventRegistration registration : registrations) {
                removeFromLocalResources(registration.getId());
            }
        }
        eventService.deregisterAllListeners(AbstractCacheService.SERVICE_NAME, cacheNameWithPrefix);
        CacheContext cacheContext = cacheContexts.get(cacheNameWithPrefix);
        if (cacheContext != null) {
            cacheContext.resetCacheEntryListenerCount();
            cacheContext.resetInvalidationListenerCount();
        }
    }

    @Override
    public Map<String, LocalCacheStats> getStats() {
        Map<String, LocalCacheStats> stats = createHashMap(statistics.size());
        for (Map.Entry<String, CacheStatisticsImpl> entry : statistics.entrySet()) {
            stats.put(entry.getKey(), new LocalCacheStatsImpl(entry.getValue()));
        }
        return stats;
    }

    @Override
    public CacheOperationProvider getCacheOperationProvider(String cacheNameWithPrefix, InMemoryFormat inMemoryFormat) {
        if (InMemoryFormat.NATIVE.equals(inMemoryFormat)) {
            throw new IllegalArgumentException("Native memory is available only in Hazelcast Enterprise."
                    + "Make sure you have Hazelcast Enterprise JARs on your classpath!");
        }
        CacheOperationProvider cacheOperationProvider = operationProviderCache.get(cacheNameWithPrefix);
        if (cacheOperationProvider != null) {
            return cacheOperationProvider;
        }
        cacheOperationProvider = createOperationProvider(cacheNameWithPrefix, inMemoryFormat);
        CacheOperationProvider current = operationProviderCache.putIfAbsent(cacheNameWithPrefix, cacheOperationProvider);
        return current == null ? cacheOperationProvider : current;
    }

    protected abstract CacheOperationProvider createOperationProvider(String nameWithPrefix, InMemoryFormat inMemoryFormat);

    public void addCacheResource(String cacheNameWithPrefix, Closeable resource) {
        Set<Closeable> cacheResources = ConcurrencyUtil.getOrPutSynchronized(
                resources, cacheNameWithPrefix, cacheResourcesMutexFactory, cacheResourcesConstructorFunction);
        cacheResources.add(resource);
    }

    protected void deleteCacheResources(String name) {
        Set<Closeable> cacheResources;
        try (ContextMutexFactory.Mutex mutex = cacheResourcesMutexFactory.mutexFor(name)) {
            synchronized (mutex) {
                cacheResources = resources.remove(name);
            }
        }

        if (cacheResources != null) {
            for (Closeable resource : cacheResources) {
                IOUtil.closeResource(resource);
            }
            cacheResources.clear();
        }
    }

    @Override
    public Operation getPreJoinOperation() {
        OnJoinCacheOperation preJoinCacheOperation;
        preJoinCacheOperation = new OnJoinCacheOperation();
        for (Map.Entry<String, CompletableFuture<CacheConfig>> cacheConfigEntry : configs.entrySet()) {
            CacheConfig cacheConfig = new PreJoinCacheConfig(cacheConfigEntry.getValue().join(), false);
            preJoinCacheOperation.addCacheConfig(cacheConfig);
        }
        return preJoinCacheOperation;
    }

    protected void publishCachePartitionLostEvent(String cacheName, int partitionId) {
        Collection<EventRegistration> registrations = new LinkedList<>();
        for (EventRegistration registration : getRegistrations(cacheName)) {
            if (registration.getFilter() instanceof CachePartitionLostEventFilter) {
                registrations.add(registration);
            }
        }

        if (registrations.isEmpty()) {
            return;
        }
        Member member = nodeEngine.getLocalMember();
        CacheEventData eventData = new CachePartitionEventData(cacheName, partitionId, member);
        EventService eventService = nodeEngine.getEventService();

        eventService.publishEvent(SERVICE_NAME, registrations, eventData, partitionId);

    }

    Collection<EventRegistration> getRegistrations(String cacheName) {
        EventService eventService = nodeEngine.getEventService();
        return eventService.getRegistrations(SERVICE_NAME, cacheName);
    }

    @Override
    public void onPartitionLost(IPartitionLostEvent partitionLostEvent) {
        int partitionId = partitionLostEvent.getPartitionId();
        for (CacheConfig config : getCacheConfigs()) {
            final String cacheName = config.getName();
            if (config.getTotalBackupCount() <= partitionLostEvent.getLostReplicaIndex()) {
                publishCachePartitionLostEvent(cacheName, partitionId);
            }
        }
    }

    public void cacheEntryListenerRegistered(String name,
                                             CacheEntryListenerConfiguration cacheEntryListenerConfiguration) {
        CacheConfig cacheConfig = getCacheConfig(name);
        if (cacheConfig == null) {
            throw new IllegalStateException("CacheConfig does not exist for cache " + name);
        }
        cacheConfig.addCacheEntryListenerConfiguration(cacheEntryListenerConfiguration);
    }

    public void cacheEntryListenerDeregistered(String name,
                                               CacheEntryListenerConfiguration cacheEntryListenerConfiguration) {
        CacheConfig cacheConfig = getCacheConfig(name);
        if (cacheConfig == null) {
            throw new IllegalStateException("CacheConfig does not exist for cache " + name);
        }
        cacheConfig.removeCacheEntryListenerConfiguration(cacheEntryListenerConfiguration);
    }

    /**
     * Gets the name of the split brain protection associated with specified cache
     *
     * @param cacheName name of the cache
     * @return name of the associated split brain protection
     * null if there is no associated split brain protection
     */
    @Override
    public String getSplitBrainProtectionName(String cacheName) {
        CacheConfig cacheConfig = getCacheConfig(cacheName);
        if (cacheConfig == null) {
            return null;
        }
        return cacheConfig.getSplitBrainProtectionName();
    }

    /**
     * Sends an invalidation event for given <code>cacheName</code> with specified <code>key</code>
     * from mentioned source with <code>sourceUuid</code>.
     *
     * @param cacheNameWithPrefix the name of the cache that invalidation event is sent for
     * @param key                 the {@link Data} represents the invalidation event
     * @param sourceUuid          an ID that represents the source for invalidation event
     */
    @Override
    public void sendInvalidationEvent(String cacheNameWithPrefix, Data key, UUID sourceUuid) {
        cacheEventHandler.sendInvalidationEvent(cacheNameWithPrefix, key, sourceUuid);
    }

    @Override
    public Runnable prepareMergeRunnable() {
        return splitBrainHandlerService.prepareMergeRunnable();
    }

    public CacheEventHandler getCacheEventHandler() {
        return cacheEventHandler;
    }

    @Override
    public CacheEventJournal getEventJournal() {
        return eventJournal;
    }

    @Override
    public <K, V> void createCacheConfigOnAllMembers(PreJoinCacheConfig<K, V> cacheConfig) {
        InternalCompletableFuture future = createCacheConfigOnAllMembersAsync(cacheConfig);
        FutureUtil.waitForever(singleton(future), RETHROW_EVERYTHING);
    }

    public <K, V> InternalCompletableFuture<Object> createCacheConfigOnAllMembersAsync(PreJoinCacheConfig<K, V> cacheConfig) {
        return InvocationUtil.invokeOnStableClusterSerial(getNodeEngine(),
                new AddCacheConfigOperationSupplier(cacheConfig),
                MAX_ADD_CACHE_CONFIG_RETRIES);
    }

    @Override
    public void onClusterStateChange(ClusterState newState) {
        ExpirationManager expManager = expirationManager;
        if (expManager != null) {
            expManager.onClusterStateChange(newState);
        }
    }

    @Override
    public void provideDynamicMetrics(MetricDescriptor descriptor, MetricsCollectionContext context) {
        provide(descriptor, context, CACHE_PREFIX, getStats());
    }
}
