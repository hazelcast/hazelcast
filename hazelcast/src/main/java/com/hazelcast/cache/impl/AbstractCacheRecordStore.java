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

import com.hazelcast.cache.CacheEventType;
import com.hazelcast.cache.CacheNotExistsException;
import com.hazelcast.cache.impl.maxsize.impl.EntryCountCacheEvictionChecker;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.cache.impl.record.CacheRecordFactory;
import com.hazelcast.cache.impl.record.SampleableCacheRecordMap;
import com.hazelcast.cluster.Address;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.config.WanConsumerConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.internal.diagnostics.StoreLatencyPlugin;
import com.hazelcast.internal.eviction.ClearExpiredRecordsTask;
import com.hazelcast.internal.eviction.EvictionCandidate;
import com.hazelcast.internal.eviction.EvictionChecker;
import com.hazelcast.internal.eviction.EvictionListener;
import com.hazelcast.internal.eviction.EvictionPolicyEvaluatorProvider;
import com.hazelcast.internal.eviction.ExpiredKey;
import com.hazelcast.internal.eviction.impl.evaluator.EvictionPolicyEvaluator;
import com.hazelcast.internal.eviction.impl.strategy.sampling.SamplingEvictionStrategy;
import com.hazelcast.internal.iteration.IterationPointer;
import com.hazelcast.internal.nearcache.impl.invalidation.InvalidationQueue;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.internal.util.Timer;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.internal.util.comparators.ValueComparator;
import com.hazelcast.internal.util.comparators.ValueComparatorUtil;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.spi.eviction.EvictionPolicyComparator;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.eventservice.EventRegistration;
import com.hazelcast.spi.impl.eventservice.EventService;
import com.hazelcast.spi.impl.tenantcontrol.TenantContextual;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.CacheMergeTypes;
import com.hazelcast.spi.tenantcontrol.TenantControl;
import com.hazelcast.wan.impl.CallerProvenance;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nonnull;
import javax.cache.configuration.Factory;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriter;
import javax.cache.integration.CacheWriterException;
import javax.cache.processor.EntryProcessor;
import java.io.Closeable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;

import static com.hazelcast.cache.impl.CacheEventContextUtil.createBaseEventContext;
import static com.hazelcast.cache.impl.CacheEventContextUtil.createCacheCompleteEvent;
import static com.hazelcast.cache.impl.CacheEventContextUtil.createCacheCreatedEvent;
import static com.hazelcast.cache.impl.CacheEventContextUtil.createCacheExpiredEvent;
import static com.hazelcast.cache.impl.CacheEventContextUtil.createCacheRemovedEvent;
import static com.hazelcast.cache.impl.CacheEventContextUtil.createCacheUpdatedEvent;
import static com.hazelcast.cache.impl.operation.MutableOperation.IGNORE_COMPLETION;
import static com.hazelcast.cache.impl.record.CacheRecord.TIME_NOT_AVAILABLE;
import static com.hazelcast.cache.impl.record.CacheRecordFactory.isExpiredAt;
import static com.hazelcast.internal.config.ConfigValidator.checkCacheEvictionConfig;
import static com.hazelcast.internal.util.EmptyStatement.ignore;
import static com.hazelcast.internal.util.MapUtil.createHashMap;
import static com.hazelcast.internal.util.SetUtil.createHashSet;
import static com.hazelcast.internal.util.ThreadUtil.assertRunningOnPartitionThread;
import static com.hazelcast.spi.impl.merge.MergingValueFactory.createMergingEntry;
import static java.util.Collections.emptySet;

@SuppressWarnings({"checkstyle:methodcount", "checkstyle:classfanoutcomplexity",
        "checkstyle:classdataabstractioncoupling"})
public abstract class AbstractCacheRecordStore<R extends CacheRecord, CRM extends SampleableCacheRecordMap<Data, R>>
        implements ICacheRecordStore, EvictionListener<Data, R> {

    public static final UUID SOURCE_NOT_AVAILABLE = UuidUtil.NIL_UUID;
    protected static final int DEFAULT_INITIAL_CAPACITY = 256;

    protected final int partitionId;
    protected final int partitionCount;
    protected final boolean wanReplicationEnabled;
    protected final boolean persistWanReplicatedData;
    protected final boolean disablePerEntryInvalidationEvents;
    /**
     * the full name of the cache, including the manager scope prefix
     */
    protected final String name;
    protected final NodeEngine nodeEngine;
    protected final CacheConfig cacheConfig;
    protected final SerializationService ss;
    protected final EvictionConfig evictionConfig;
    protected final ValueComparator valueComparator;
    protected final EvictionChecker evictionChecker;
    protected final ObjectNamespace objectNamespace;
    protected final AbstractCacheService cacheService;
    protected final CacheRecordFactory cacheRecordFactory;
    protected final EventJournalConfig eventJournalConfig;
    protected final ClearExpiredRecordsTask clearExpiredRecordsTask;
    protected final SamplingEvictionStrategy<Data, R, CRM> evictionStrategy;
    protected final EvictionPolicyEvaluator<Data, R> evictionPolicyEvaluator;
    protected final Map<CacheEventType, Set<CacheEventData>> batchEvent = new HashMap<CacheEventType, Set<CacheEventData>>();
    protected final CompositeCacheRSMutationObserver compositeCacheRSMutationObserver;

    protected boolean primary;
    protected boolean eventsEnabled = true;
    protected boolean eventsBatchingEnabled;
    protected CRM records;
    protected TenantContextual<CacheLoader> cacheLoader;
    protected TenantContextual<CacheWriter> cacheWriter;
    protected CacheContext cacheContext;
    protected CacheStatisticsImpl statistics;
    protected TenantContextual<ExpiryPolicy> defaultExpiryPolicy;
    protected Iterator<Map.Entry<Data, R>> expirationIterator;
    protected InvalidationQueue<ExpiredKey> expiredKeys = new InvalidationQueue<ExpiredKey>();
    protected boolean hasEntryWithExpiration;

    @SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:executablestatementcount", "checkstyle:methodlength"})
    public AbstractCacheRecordStore(String cacheNameWithPrefix, int partitionId, NodeEngine nodeEngine,
                                    AbstractCacheService cacheService) {
        this.name = cacheNameWithPrefix;
        this.partitionId = partitionId;
        this.nodeEngine = nodeEngine;
        this.ss = nodeEngine.getSerializationService();
        this.partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        this.cacheService = cacheService;
        this.cacheConfig = cacheService.getCacheConfig(cacheNameWithPrefix);
        if (cacheConfig == null) {
            throw new CacheNotExistsException("Cache " + cacheNameWithPrefix + " is already destroyed or not created yet, on "
                    + nodeEngine.getLocalMember());
        }
        this.eventJournalConfig = cacheConfig.getEventJournalConfig();
        this.evictionConfig = cacheConfig.getEvictionConfig();
        if (evictionConfig == null) {
            throw new IllegalStateException("Eviction config cannot be null!");
        }
        this.wanReplicationEnabled = cacheService.isWanReplicationEnabled(cacheNameWithPrefix);
        this.disablePerEntryInvalidationEvents = cacheConfig.isDisablePerEntryInvalidationEvents();

        EvictionPolicyComparator evictionPolicyComparator = createEvictionPolicyComparator(evictionConfig);
        evictionPolicyComparator = injectDependencies(evictionPolicyComparator);
        this.evictionPolicyEvaluator = new EvictionPolicyEvaluator<>(evictionPolicyComparator);
        this.cacheContext = cacheService.getOrCreateCacheContext(cacheNameWithPrefix);
        this.records = createRecordCacheMap();
        this.evictionChecker = createCacheEvictionChecker(evictionConfig.getSize(), evictionConfig.getMaxSizePolicy());
        this.evictionStrategy = createEvictionStrategy(evictionConfig);
        this.objectNamespace = CacheService.getObjectNamespace(cacheNameWithPrefix);
        this.persistWanReplicatedData = canPersistWanReplicatedData(cacheConfig, nodeEngine);
        this.cacheRecordFactory = new CacheRecordFactory(cacheConfig.getInMemoryFormat(), ss);
        this.valueComparator = getValueComparatorOf(cacheConfig.getInMemoryFormat());
        this.clearExpiredRecordsTask = cacheService.getExpirationManager().getTask();
        this.compositeCacheRSMutationObserver = new CompositeCacheRSMutationObserver();

        if (cacheConfig.isStatisticsEnabled()) {
            statistics = cacheService.createCacheStatIfAbsent(cacheNameWithPrefix);
        }

        TenantControl tenantControl = nodeEngine
                .getTenantControlService()
                .getTenantControl(ICacheService.SERVICE_NAME, cacheNameWithPrefix);
        cacheLoader = TenantContextual.create(this::initCacheLoader,
                () -> cacheConfig.getCacheLoaderFactory() != null, tenantControl);
        cacheWriter = TenantContextual.create(this::initCacheWriter,
                () -> cacheConfig.getCacheWriterFactory() != null, tenantControl);
        defaultExpiryPolicy = TenantContextual.create(this::initDefaultExpiryPolicy,
                this::defaultExpiryPolicyExists, tenantControl);
        init();
    }

    public SerializationService getSerializationService() {
        return ss;
    }

    private CacheLoader initCacheLoader() {
        Factory<CacheLoader> cacheLoaderFactory = cacheConfig.getCacheLoaderFactory();
        cacheLoaderFactory = injectDependencies(cacheLoaderFactory);
        CacheLoader cacheLoader = cacheLoaderFactory.create();
        cacheLoader = injectDependencies(cacheLoader);
        registerResourceIfItIsClosable(cacheLoader);
        return cacheLoader;
    }

    private CacheWriter initCacheWriter() {
        Factory<CacheWriter> cacheWriterFactory = cacheConfig.getCacheWriterFactory();
        cacheWriterFactory = injectDependencies(cacheWriterFactory);
        CacheWriter cacheWriter = cacheWriterFactory.create();
        cacheWriter = injectDependencies(cacheWriter);
        registerResourceIfItIsClosable(cacheWriter);
        return cacheWriter;
    }

    private ExpiryPolicy initDefaultExpiryPolicy() {
        Factory<ExpiryPolicy> expiryPolicyFactory = cacheConfig.getExpiryPolicyFactory();
        expiryPolicyFactory = injectDependencies(expiryPolicyFactory);
        ExpiryPolicy defaultExpiryPolicy = expiryPolicyFactory.create();
        defaultExpiryPolicy = injectDependencies(defaultExpiryPolicy);
        registerResourceIfItIsClosable(defaultExpiryPolicy);
        return defaultExpiryPolicy;
    }

    private Boolean defaultExpiryPolicyExists() {
        if (cacheConfig.getExpiryPolicyFactory() != null) {
            return true;
        } else {
            throw new IllegalStateException("Expiry policy factory cannot be null!");
        }
    }

    // Overridden in EE
    protected ValueComparator getValueComparatorOf(InMemoryFormat inMemoryFormat) {
        return ValueComparatorUtil.getValueComparatorOf(inMemoryFormat);
    }

    private boolean canPersistWanReplicatedData(CacheConfig cacheConfig, NodeEngine nodeEngine) {
        boolean persistWanReplicatedData = false;
        WanReplicationRef wanReplicationRef = cacheConfig.getWanReplicationRef();
        if (wanReplicationRef != null) {
            String wanReplicationRefName = wanReplicationRef.getName();
            Config config = nodeEngine.getConfig();
            WanReplicationConfig wanReplicationConfig = config.getWanReplicationConfig(wanReplicationRefName);
            if (wanReplicationConfig != null) {
                WanConsumerConfig wanConsumerConfig = wanReplicationConfig.getConsumerConfig();
                if (wanConsumerConfig != null) {
                    persistWanReplicatedData = wanConsumerConfig.isPersistWanReplicatedData();
                }
            }
        }
        return persistWanReplicatedData;
    }

    private boolean persistenceEnabledFor(@Nonnull CallerProvenance provenance) {
        switch (provenance) {
            case WAN:
                return persistWanReplicatedData;
            case NOT_WAN:
                return true;
            default:
                throw new IllegalArgumentException("Unexpected provenance: `" + provenance + "`");
        }
    }

    public void instrument(NodeEngine nodeEngine) {
        StoreLatencyPlugin plugin = ((NodeEngineImpl) nodeEngine).getDiagnostics().getPlugin(StoreLatencyPlugin.class);
        if (plugin == null) {
            return;
        }

        cacheLoader = cacheLoader.delegate(new LatencyTrackingCacheLoader(cacheLoader, plugin, cacheConfig.getName()));
        cacheWriter = cacheWriter.delegate(new LatencyTrackingCacheWriter(cacheWriter, plugin, cacheConfig.getName()));
    }

    private boolean isPrimary() {
        final Address owner = nodeEngine.getPartitionService().getPartition(partitionId, false).getOwnerOrNull();
        final Address thisAddress = nodeEngine.getThisAddress();
        return owner != null && owner.equals(thisAddress);
    }

    @SuppressWarnings("unchecked")
    private <T> T injectDependencies(T obj) {
        ManagedContext managedContext = ss.getManagedContext();
        return (T) managedContext.initialize(obj);
    }

    private void registerResourceIfItIsClosable(Object resource) {
        if (resource instanceof Closeable) {
            cacheService.addCacheResource(name, (Closeable) resource);
        }
    }

    @Override
    public void init() {
        primary = isPrimary();
        records.setEntryCounting(primary);
        markExpirable(TIME_NOT_AVAILABLE);
        addMutationObservers();
    }

    // Overridden in EE.
    protected void addMutationObservers() {
        if (eventJournalConfig != null && eventJournalConfig.isEnabled()) {
            compositeCacheRSMutationObserver.add(
                    new EventJournalRSMutationObserver(cacheService, eventJournalConfig, objectNamespace, partitionId));
        }
    }

    protected boolean isReadThrough() {
        return cacheConfig.isReadThrough();
    }

    protected boolean isWriteThrough() {
        return cacheConfig.isWriteThrough();
    }

    protected boolean isStatisticsEnabled() {
        return statistics != null;
    }

    protected abstract CRM createRecordCacheMap();

    protected abstract CacheEntryProcessorEntry createCacheEntryProcessorEntry(Data key, R record,
                                                                               long now, int completionId);

    protected abstract R createRecord(Object value, long creationTime, long expiryTime);

    protected abstract Data valueToData(Object value);

    protected abstract Object dataToValue(Data data);

    protected abstract Object recordToValue(R record);

    protected abstract Data recordToData(R record);

    protected abstract Data toHeapData(Object obj);

    /**
     * Creates an instance for checking if the maximum cache size has been reached. Supports only the
     * {@link MaxSizePolicy#ENTRY_COUNT} policy. Returns null if other {@code maxSizePolicy} is used.
     *
     * @param size          the maximum number of entries
     * @param maxSizePolicy the way in which the size is interpreted, only the {@link MaxSizePolicy#ENTRY_COUNT} policy is
     *                      supported.
     * @return the instance which will check if the maximum number of entries has been reached or null if the
     * {@code maxSizePolicy} is not {@link MaxSizePolicy#ENTRY_COUNT}
     * @throws IllegalArgumentException if the {@code maxSizePolicy} is null
     */
    protected EvictionChecker createCacheEvictionChecker(int size, MaxSizePolicy maxSizePolicy) {
        if (maxSizePolicy == null) {
            throw new IllegalArgumentException("Max-Size policy cannot be null");
        }
        if (maxSizePolicy == MaxSizePolicy.ENTRY_COUNT) {
            return new EntryCountCacheEvictionChecker(size, records, partitionCount);
        }
        return null;
    }

    protected EvictionPolicyComparator createEvictionPolicyComparator(EvictionConfig evictionConfig) {
        checkCacheEvictionConfig(evictionConfig);

        return EvictionPolicyEvaluatorProvider.getEvictionPolicyComparator(evictionConfig, nodeEngine.getConfigClassLoader());
    }

    protected SamplingEvictionStrategy<Data, R, CRM> createEvictionStrategy(EvictionConfig cacheEvictionConfig) {
        return SamplingEvictionStrategy.INSTANCE;
    }

    protected boolean isEvictionEnabled() {
        return evictionStrategy != null && evictionPolicyEvaluator != null;
    }

    protected boolean isEventsEnabled() {
        return eventsEnabled && (cacheContext.getCacheEntryListenerCount() > 0 || wanReplicationEnabled);
    }

    protected boolean isInvalidationEnabled() {
        return primary && cacheContext.getInvalidationListenerCount() > 0;
    }

    @Override
    public boolean evictIfRequired() {
        if (!isEvictionEnabled()) {
            return false;
        }

        boolean evicted = evictionStrategy.evict(records, evictionPolicyEvaluator, evictionChecker, this);
        if (isStatisticsEnabled() && evicted && primary) {
            statistics.increaseCacheEvictions(1);
        }
        return evicted;
    }

    @Override
    public void sampleAndForceRemoveEntries(int entryCountToRemove) {
        assertRunningOnPartitionThread();

        Queue<Data> keysToRemove = new LinkedList<Data>();
        Iterable<EvictionCandidate<Data, R>> entries = records.sample(entryCountToRemove);
        for (EvictionCandidate<Data, R> entry : entries) {
            keysToRemove.add(entry.getAccessor());
        }

        Data dataKey;
        while ((dataKey = keysToRemove.poll()) != null) {
            forceRemoveRecord(dataKey);
        }
    }

    protected void forceRemoveRecord(Data key) {
        // overridden in other context
        removeRecord(key);
    }

    protected Data toData(Object obj) {
        if (obj instanceof Data) {
            return (Data) obj;
        } else if (obj instanceof CacheRecord) {
            return recordToData((R) obj);
        } else {
            return valueToData(obj);
        }
    }

    protected Object toValue(Object obj) {
        if (obj instanceof Data) {
            return dataToValue((Data) obj);
        } else if (obj instanceof CacheRecord) {
            return recordToValue((R) obj);
        } else {
            return obj;
        }
    }

    protected Object toStorageValue(Object obj) {
        if (obj instanceof Data) {
            if (cacheConfig.getInMemoryFormat() == InMemoryFormat.OBJECT) {
                return dataToValue((Data) obj);
            } else {
                return obj;
            }
        } else if (obj instanceof CacheRecord) {
            return recordToValue((R) obj);
        } else {
            return obj;
        }
    }

    public Data toEventData(Object obj) {
        return isEventsEnabled() ? toHeapData(obj) : null;
    }

    private long getAdjustedExpireTime(Duration duration, long now) {
        return duration.getAdjustedTime(now);
    }

    protected ExpiryPolicy getExpiryPolicy(CacheRecord record, ExpiryPolicy expiryPolicy) {
        if (expiryPolicy != null) {
            return expiryPolicy;
        }

        if (record != null && record.getExpiryPolicy() != null) {
            return (ExpiryPolicy) toValue(record.getExpiryPolicy());
        }

        return defaultExpiryPolicy.get();
    }

    protected boolean evictIfExpired(Data key, R record, long now) {
        return processExpiredEntry(key, record, now);
    }

    protected boolean processExpiredEntry(Data key, R record, long now) {
        return processExpiredEntry(key, record, now, SOURCE_NOT_AVAILABLE);
    }

    protected boolean processExpiredEntry(Data key, R record, long now, UUID source) {
        return processExpiredEntry(key, record, now, source, null);
    }

    protected boolean processExpiredEntry(Data key, R record, long now, UUID source, UUID origin) {
        // The event journal will get REMOVED instead of EXPIRED
        boolean isExpired = record != null && record.isExpiredAt(now);
        if (!isExpired) {
            return false;
        }
        if (isStatisticsEnabled()) {
            statistics.increaseCacheExpiries(1);
        }
        R removedRecord = doRemoveRecord(key, source);
        Data keyEventData = toEventData(key);
        Data recordEventData = toEventData(removedRecord);
        if (removedRecord != null) {
            onProcessExpiredEntry(key, removedRecord, removedRecord.getExpirationTime(), now, source, origin);
            if (isEventsEnabled()) {
                publishEvent(createCacheExpiredEvent(keyEventData, recordEventData,
                        TIME_NOT_AVAILABLE, origin, IGNORE_COMPLETION));
            }
        }
        return true;
    }

    protected R processExpiredEntry(Data key, R record, long expiryTime, long now, UUID source) {
        return processExpiredEntry(key, record, expiryTime, now, source, null);
    }

    protected R processExpiredEntry(Data key, R record, long expiryTime, long now, UUID source, UUID origin) {
        // The event journal will get REMOVED instead of EXPIRED
        if (!isExpiredAt(expiryTime, now)) {
            return record;
        }
        if (isStatisticsEnabled()) {
            statistics.increaseCacheExpiries(1);
        }
        R removedRecord = doRemoveRecord(key, source);
        Data keyEventData = toEventData(key);
        Data recordEventData = toEventData(removedRecord);
        onProcessExpiredEntry(key, removedRecord, expiryTime, now, source, origin);
        if (isEventsEnabled()) {
            publishEvent(createCacheExpiredEvent(keyEventData, recordEventData, TIME_NOT_AVAILABLE,
                    origin, IGNORE_COMPLETION));
        }
        return null;
    }

    protected void onProcessExpiredEntry(Data key, R record, long expiryTime, long now, UUID source, UUID origin) {
        accumulateOrSendExpiredKeysToBackup(key, record);
    }

    protected void accumulateOrSendExpiredKeysToBackup(Data key, R record) {
        if (cacheConfig.getTotalBackupCount() == 0) {
            return;
        }
        if (key != null && record != null) {
            this.expiredKeys.offer(new ExpiredKey(toHeapData(key), record.getCreationTime()));
        }

        clearExpiredRecordsTask.tryToSendBackupExpiryOp(this, true);
    }

    @Override
    public boolean isExpirable() {
        return hasEntryWithExpiration || getConfig().getExpiryPolicyFactory() != null;
    }

    public R accessRecord(Data key, R record, ExpiryPolicy expiryPolicy, long now) {
        onRecordAccess(key, record, getExpiryPolicy(record, expiryPolicy), now);
        return record;
    }

    @Override
    public void onEvict(Data key, R record, boolean wasExpired) {
        if (wasExpired) {
            compositeCacheRSMutationObserver.onExpire(key, record.getValue());
        } else {
            compositeCacheRSMutationObserver.onEvict(key, record.getValue());
        }
        invalidateEntry(key);
    }

    protected void invalidateEntry(Data key, UUID source) {
        if (isInvalidationEnabled()) {
            if (key == null) {
                cacheService.sendInvalidationEvent(name, null, source);
            } else if (!disablePerEntryInvalidationEvents) {
                cacheService.sendInvalidationEvent(name, toHeapData(key), source);
            }
        }
    }

    protected void invalidateEntry(Data key) {
        invalidateEntry(key, SOURCE_NOT_AVAILABLE);
    }

    protected void updateGetAndPutStat(boolean isPutSucceed, boolean getValue, boolean oldValueNull, long startNanos) {
        if (isStatisticsEnabled()) {
            if (isPutSucceed) {
                statistics.increaseCachePuts(1);
                statistics.addPutTimeNanos(Timer.nanosElapsed(startNanos));
            }
            if (getValue) {
                if (oldValueNull) {
                    statistics.increaseCacheMisses(1);
                } else {
                    statistics.increaseCacheHits(1);
                }
                statistics.addGetTimeNanos(Timer.nanosElapsed(startNanos));
            }
        }
    }

    protected long updateAccessDuration(Data key, R record, ExpiryPolicy expiryPolicy, long now) {
        long expiryTime = TIME_NOT_AVAILABLE;
        try {
            Duration expiryDuration = expiryPolicy.getExpiryForAccess();
            if (expiryDuration != null) {
                expiryTime = getAdjustedExpireTime(expiryDuration, now);
                record.setExpirationTime(expiryTime);
                if (isEventsEnabled()) {
                    CacheEventContext cacheEventContext =
                            createBaseEventContext(CacheEventType.EXPIRATION_TIME_UPDATED, toEventData(key),
                                    toEventData(record.getValue()), expiryTime, null, IGNORE_COMPLETION);
                    cacheEventContext.setAccessHit(record.getHits());
                    publishEvent(cacheEventContext);
                }
            }
        } catch (Exception e) {
            ignore(e);
        }
        return expiryTime;
    }

    protected long onRecordAccess(Data key, R record, ExpiryPolicy expiryPolicy, long now) {
        record.setLastAccessTime(now);
        record.incrementHits();
        return updateAccessDuration(key, record, expiryPolicy, now);
    }

    protected void updateReplaceStat(boolean result, boolean isHit, long startNanos) {
        if (isStatisticsEnabled()) {
            if (result) {
                statistics.increaseCachePuts(1);
                statistics.addPutTimeNanos(Timer.nanosElapsed(startNanos));
            }
            if (isHit) {
                statistics.increaseCacheHits(1);
            } else {
                statistics.increaseCacheMisses(1);
            }
        }
    }

    protected void publishEvent(CacheEventContext cacheEventContext) {
        if (isEventsEnabled()) {
            cacheEventContext.setCacheName(name);
            if (eventsBatchingEnabled) {
                CacheEventDataImpl cacheEventData =
                        new CacheEventDataImpl(name, cacheEventContext.getEventType(), cacheEventContext.getDataKey(),
                                cacheEventContext.getDataValue(), cacheEventContext.getDataOldValue(),
                                cacheEventContext.isOldValueAvailable());
                Set<CacheEventData> cacheEventDataSet = batchEvent.remove(cacheEventContext.getEventType());
                if (cacheEventDataSet == null) {
                    cacheEventDataSet = new HashSet<CacheEventData>();
                    batchEvent.put(cacheEventContext.getEventType(), cacheEventDataSet);
                }
                cacheEventDataSet.add(cacheEventData);
            } else {
                cacheService.publishEvent(cacheEventContext);
            }
        }
    }

    protected void publishBatchedEvents(String cacheName, CacheEventType cacheEventType, int orderKey) {
        if (isEventsEnabled()) {
            Set<CacheEventData> cacheEventDatas = batchEvent.remove(cacheEventType);
            if (cacheEventDatas != null) {
                cacheService.publishEvent(cacheName, new CacheEventSet(cacheEventType, cacheEventDatas), orderKey);
            }
        }
    }

    protected boolean compare(Object v1, Object v2) {
        if (v1 == null && v2 == null) {
            return true;
        }
        if (v1 == null || v2 == null) {
            return false;
        }
        return v1.equals(v2);
    }

    protected R createRecord(long expiryTime) {
        return createRecord(null, Clock.currentTimeMillis(), expiryTime);
    }

    protected R createRecord(Object value, long expiryTime) {
        return createRecord(value, Clock.currentTimeMillis(), expiryTime);
    }

    protected R createRecord(Data keyData, Object value, long expirationTime, int completionId) {
        R record = createRecord(value, expirationTime);
        if (isEventsEnabled()) {
            publishEvent(createCacheCreatedEvent(toEventData(keyData), toEventData(value),
                    expirationTime, null, completionId));
        }
        return record;
    }

    @SuppressWarnings("checkstyle:parameternumber")
    protected void onCreateRecordError(Data key, Object value, long expiryTime, long now, boolean disableWriteThrough,
                                       int completionId, UUID origin, R record, Throwable error) {
    }

    protected R createRecord(Data key, Object value, long expiryTime, long now,
                             boolean disableWriteThrough, int completionId, UUID origin) {
        R record = createRecord(value, now, expiryTime);
        try {
            doPutRecord(key, record, origin, true);
        } catch (Throwable error) {
            onCreateRecordError(key, value, expiryTime, now, disableWriteThrough,
                    completionId, origin, record, error);
            throw ExceptionUtil.rethrow(error);
        }
        try {
            if (!disableWriteThrough) {
                writeThroughCache(key, value);
            }
        } catch (Throwable error) {
            // Writing to `CacheWriter` failed, so we should revert entry (remove added record).
            final R removed = records.remove(key);
            if (removed != null) {
                compositeCacheRSMutationObserver.onRemove(
                        key, removed.getValue());
            }
            // Disposing key/value/record should be handled inside `onCreateRecordWithExpiryError`.
            onCreateRecordError(key, value, expiryTime, now, disableWriteThrough,
                    completionId, origin, record, error);
            throw ExceptionUtil.rethrow(error);
        }
        if (isEventsEnabled()) {
            publishEvent(createCacheCreatedEvent(toEventData(key), toEventData(value),
                    expiryTime, origin, completionId));
        }
        return record;
    }

    protected R createRecordWithExpiry(Data key, Object value, long expiryTime,
                                       long now, boolean disableWriteThrough, int completionId, UUID origin) {
        if (!isExpiredAt(expiryTime, now)) {
            return createRecord(key, value, expiryTime, now, disableWriteThrough, completionId, origin);
        }
        if (isEventsEnabled()) {
            publishEvent(createCacheCompleteEvent(toEventData(key), TIME_NOT_AVAILABLE,
                    origin, completionId));
        }
        return null;
    }

    protected R createRecordWithExpiry(Data key, Object value, long expiryTime,
                                       long now, boolean disableWriteThrough, int completionId) {
        return createRecordWithExpiry(key, value, expiryTime, now, disableWriteThrough, completionId, SOURCE_NOT_AVAILABLE);
    }

    protected R createRecordWithExpiry(Data key, Object value, ExpiryPolicy expiryPolicy,
                                       long now, boolean disableWriteThrough, int completionId) {
        return createRecordWithExpiry(key, value, expiryPolicy, now, disableWriteThrough, completionId, SOURCE_NOT_AVAILABLE);
    }

    protected R createRecordWithExpiry(Data key, Object value, ExpiryPolicy expiryPolicy,
                                       long now, boolean disableWriteThrough, int completionId, UUID origin) {
        expiryPolicy = getExpiryPolicy(null, expiryPolicy);
        Duration expiryDuration;
        try {
            expiryDuration = expiryPolicy.getExpiryForCreation();
        } catch (Exception e) {
            expiryDuration = Duration.ETERNAL;
        }
        long expiryTime = getAdjustedExpireTime(expiryDuration, now);
        return createRecordWithExpiry(key, value, expiryTime, now, disableWriteThrough, completionId, origin);
    }

    protected void onUpdateRecord(Data key, R record, Object value, Data oldDataValue) {
        compositeCacheRSMutationObserver.onUpdate(key, oldDataValue, value);
    }

    protected void onUpdateRecordError(Data key, R record, Object value, Data newDataValue,
                                       Data oldDataValue, Throwable error) {
    }

    protected void onUpdateExpiryPolicy(Data key, R record, Data oldDataExpiryPolicy) {

    }

    protected void onUpdateExpiryPolicyError(Data key, R record, Data oldDataExpiryPolicy) {

    }

    protected void updateRecord(Data key, CacheRecord record, long expiryTime, long now, UUID origin) {
        record.setExpirationTime(expiryTime);
        invalidateEntry(key, origin);
    }

    @SuppressWarnings("checkstyle:parameternumber")
    protected void updateRecord(Data key, R record, Object value, long expiryTime, long now,
                                boolean disableWriteThrough, int completionId, UUID source, UUID origin) {
        Data dataOldValue = null;
        Data dataValue = null;
        Object recordValue = value;
        try {
            updateExpiryTime(record, expiryTime);
            if (isExpiredAt(expiryTime, now)) {
                // No need to update record value if it is expired
                if (!disableWriteThrough) {
                    writeThroughCache(key, value);
                }
            } else {
                switch (cacheConfig.getInMemoryFormat()) {
                    case BINARY:
                        recordValue = toData(value);
                        dataValue = (Data) recordValue;
                        dataOldValue = toData(record);
                        break;
                    case OBJECT:
                        if (value instanceof Data) {
                            recordValue = dataToValue((Data) value);
                            dataValue = (Data) value;
                        } else {
                            dataValue = valueToData(value);
                        }
                        dataOldValue = toData(record);
                        break;
                    case NATIVE:
                        recordValue = toData(value);
                        dataValue = (Data) recordValue;
                        dataOldValue = toData(record);
                        break;
                    default:
                        throw new IllegalArgumentException("Invalid storage format: " + cacheConfig.getInMemoryFormat());
                }

                if (!disableWriteThrough) {
                    writeThroughCache(key, value);
                    // If writing to `CacheWriter` fails no need to revert. Because we have not update record value yet
                    // with its new value but just converted new value to required storage type.
                }

                Data eventDataKey = toEventData(key);
                Data eventDataValue = toEventData(dataValue);
                Data eventDataOldValue = toEventData(dataOldValue);
                Data eventDataExpiryPolicy = toEventData(record.getExpiryPolicy());

                updateRecordValue(record, recordValue);
                onUpdateRecord(key, record, value, dataOldValue);
                invalidateEntry(key, source);

                if (isEventsEnabled()) {
                    publishEvent(createCacheUpdatedEvent(eventDataKey, eventDataValue, eventDataOldValue,
                            record.getCreationTime(), record.getExpirationTime(),
                            record.getLastAccessTime(), record.getHits(),
                            origin, completionId, eventDataExpiryPolicy));
                }
            }
        } catch (Throwable error) {
            onUpdateRecordError(key, record, value, dataValue, dataOldValue, error);
            throw ExceptionUtil.rethrow(error);
        }
    }

    private void updateExpiryTime(R record, long expiryTime) {
        if (expiryTime == TIME_NOT_AVAILABLE) {
            return;
        }

        markExpirable(expiryTime);
        record.setExpirationTime(expiryTime);
    }

    protected void updateExpiryPolicyOfRecord(Data key, R record, Object expiryPolicy) {
        Object inMemoryExpiryPolicy;
        Data dataOldExpiryPolicy = null;
        switch (cacheConfig.getInMemoryFormat()) {
            case OBJECT:
                inMemoryExpiryPolicy = toValue(expiryPolicy);
                dataOldExpiryPolicy = toData(getExpiryPolicyOrNull(record));
                break;
            case BINARY:
            case NATIVE:
                inMemoryExpiryPolicy = toData(expiryPolicy);
                dataOldExpiryPolicy = toData(getExpiryPolicyOrNull(record));
                break;
            default:
                throw new IllegalArgumentException("Invalid storage format: " + cacheConfig.getInMemoryFormat());
        }
        try {
            onUpdateExpiryPolicy(key, record, dataOldExpiryPolicy);
            record.setExpiryPolicy(inMemoryExpiryPolicy);
        } catch (Throwable error) {
            onUpdateExpiryPolicyError(key, record, dataOldExpiryPolicy);
            throw ExceptionUtil.rethrow(error);
        }
    }

    protected Object extractExpiryPolicyOfRecord(CacheRecord record) {
        Object policyData = record.getExpiryPolicy();
        if (policyData == null) {
            return null;
        }
        switch (cacheConfig.getInMemoryFormat()) {
            case NATIVE:
            case BINARY:
                return policyData;
            case OBJECT:
                return toValue(policyData);
            default:
                throw new IllegalArgumentException("Invalid storage format: " + cacheConfig.getInMemoryFormat());
        }
    }

    protected void updateRecordValue(R record, Object recordValue) {
        record.setValue(recordValue);
    }

    @SuppressWarnings("checkstyle:parameternumber")
    protected boolean updateRecordWithExpiry(Data key, Object value, R record, long expiryTime, long now,
                                             boolean disableWriteThrough, int completionId,
                                             UUID source, UUID origin) {
        updateRecord(key, record, value, expiryTime, now, disableWriteThrough, completionId, source, origin);
        return processExpiredEntry(key, record, expiryTime, now, source) != null;
    }

    protected boolean updateRecordWithExpiry(Data key, Object value, R record, long expiryTime,
                                             long now, boolean disableWriteThrough, int completionId) {
        return updateRecordWithExpiry(key, value, record, expiryTime, now, disableWriteThrough,
                completionId, SOURCE_NOT_AVAILABLE);
    }

    protected boolean updateRecordWithExpiry(Data key, Object value, R record, long expiryTime,
                                             long now, boolean disableWriteThrough, int completionId, UUID source) {
        return updateRecordWithExpiry(key, value, record, expiryTime, now,
                disableWriteThrough, completionId, source, null);
    }

    protected boolean updateRecordWithExpiry(Data key, Object value, R record, ExpiryPolicy expiryPolicy,
                                             long now, boolean disableWriteThrough, int completionId) {
        return updateRecordWithExpiry(key, value, record, expiryPolicy, now,
                disableWriteThrough, completionId, SOURCE_NOT_AVAILABLE);
    }

    protected boolean updateRecordWithExpiry(Data key, Object value, R record, ExpiryPolicy expiryPolicy,
                                             long now, boolean disableWriteThrough, int completionId, UUID source) {
        return updateRecordWithExpiry(key, value, record, expiryPolicy, now,
                disableWriteThrough, completionId, source, null);
    }

    @SuppressWarnings("checkstyle:parameternumber")
    protected boolean updateRecordWithExpiry(Data key, Object value, R record, ExpiryPolicy expiryPolicy, long now,
                                             boolean disableWriteThrough, int completionId, UUID source, UUID origin) {
        expiryPolicy = getExpiryPolicy(record, expiryPolicy);
        long expiryTime = TIME_NOT_AVAILABLE;
        try {
            Duration expiryDuration = expiryPolicy.getExpiryForUpdate();
            if (expiryDuration != null) {
                expiryTime = getAdjustedExpireTime(expiryDuration, now);
            }
        } catch (Exception e) {
            ignore(e);
        }
        return updateRecordWithExpiry(key, value, record, expiryTime, now,
                disableWriteThrough, completionId, source, origin);
    }

    protected void updateRecordWithExpiry(Data key, CacheRecord record, ExpiryPolicy expiryPolicy, long now, UUID source) {
        expiryPolicy = getExpiryPolicy(record, expiryPolicy);
        long expiryTime = TIME_NOT_AVAILABLE;
        try {
            Duration expiryDuration = expiryPolicy.getExpiryForUpdate();
            if (expiryDuration != null) {
                expiryTime = getAdjustedExpireTime(expiryDuration, now);
            }
        } catch (Exception e) {
            ignore(e);
        }
        updateRecord(key, record, expiryTime, now, source);
    }

    protected void onDeleteRecord(Data key, R record, boolean deleted) {
    }

    protected boolean deleteRecord(Data key, int completionId) {
        return deleteRecord(key, completionId, SOURCE_NOT_AVAILABLE);
    }

    protected boolean deleteRecord(Data key, int completionId, UUID source) {
        return deleteRecord(key, completionId, source, null);
    }

    protected boolean deleteRecord(Data key, int completionId, UUID source, UUID origin) {
        R removedRecord = null;
        try {
            removedRecord = doRemoveRecord(key, source);
            if (isEventsEnabled()) {
                Data eventDataKey = toEventData(key);
                Data eventDataValue = toEventData(removedRecord);

                publishEvent(createCacheRemovedEvent(eventDataKey, eventDataValue,
                        TIME_NOT_AVAILABLE, origin, completionId));
            }
        } finally {
            onDeleteRecord(key, removedRecord, removedRecord != null);
        }
        return removedRecord != null;
    }

    public R readThroughRecord(Data key, long now) {
        Object value = readThroughCache(key);
        if (value == null) {
            return null;
        }
        Duration expiryDuration;
        try {
            expiryDuration = defaultExpiryPolicy.get().getExpiryForCreation();
        } catch (Exception e) {
            expiryDuration = Duration.ETERNAL;
        }
        long expiryTime = getAdjustedExpireTime(expiryDuration, now);
        if (isExpiredAt(expiryTime, now)) {
            return null;
        }
        return createRecord(key, value, expiryTime, IGNORE_COMPLETION);
    }

    public Object readThroughCache(Data key) throws CacheLoaderException {
        if (isReadThrough()) {
            try (TenantControl.Closeable ctx = cacheLoader.getTenantControl().setTenant()) {
                if (cacheLoader.exists()) {
                    Object o = dataToValue(key);
                    return cacheLoader.get().load(o);
                }
            } catch (Exception e) {
                if (!(e instanceof CacheLoaderException)) {
                    throw new CacheLoaderException("Exception in CacheLoader during load", e);
                } else {
                    throw (CacheLoaderException) e;
                }
            }
        }
        return null;
    }

    public void writeThroughCache(Data key, Object value) throws CacheWriterException {
        if (isWriteThrough()) {
            try (TenantControl.Closeable ctx = cacheWriter.getTenantControl().setTenant()) {
                if (cacheWriter.exists()) {
                    Object objKey = dataToValue(key);
                    Object objValue = toValue(value);
                    cacheWriter.get().write(new CacheEntry<Object, Object>(objKey, objValue));
                }
            } catch (Exception e) {
                if (!(e instanceof CacheWriterException)) {
                    throw new CacheWriterException("Exception in CacheWriter during write", e);
                } else {
                    throw (CacheWriterException) e;
                }
            }
        }
    }

    protected void deleteCacheEntry(Data key) {
        deleteCacheEntry(key, CallerProvenance.NOT_WAN);
    }

    protected void deleteCacheEntry(Data key, CallerProvenance provenance) {
        if (persistenceEnabledFor(provenance) && isWriteThrough()) {
            try (TenantControl.Closeable ctx = cacheWriter.getTenantControl().setTenant()) {
                if (cacheWriter.exists()) {
                    Object objKey = dataToValue(key);
                    cacheWriter.get().delete(objKey);
                }
            } catch (Exception e) {
                if (!(e instanceof CacheWriterException)) {
                    throw new CacheWriterException("Exception in CacheWriter during delete", e);
                } else {
                    throw (CacheWriterException) e;
                }
            }
        }
    }

    @SuppressFBWarnings("WMI_WRONG_MAP_ITERATOR")
    protected void deleteAllCacheEntry(Set<Data> keys) {
        if (isWriteThrough() && keys != null && !keys.isEmpty()) {
            try (TenantControl.Closeable ctx = cacheWriter.getTenantControl().setTenant()) {
                if (cacheWriter.exists()) {
                    Map<Object, Data> keysToDelete = createHashMap(keys.size());
                    for (Data key : keys) {
                        Object localKeyObj = dataToValue(key);
                        keysToDelete.put(localKeyObj, key);
                    }
                    Set<Object> keysObject = keysToDelete.keySet();
                    try {
                        cacheWriter.get().deleteAll(keysObject);
                    } catch (Exception e) {
                        if (!(e instanceof CacheWriterException)) {
                            throw new CacheWriterException("Exception in CacheWriter during deleteAll", e);
                        } else {
                            throw (CacheWriterException) e;
                        }
                    } finally {
                        for (Object undeletedKey : keysObject) {
                            Data undeletedKeyData = keysToDelete.get(undeletedKey);
                            keys.remove(undeletedKeyData);
                        }
                    }
                }
            }
        }
    }

    protected Map<Data, Object> loadAllCacheEntry(Set<Data> keys) {
        try (TenantControl.Closeable ctx = cacheLoader.getTenantControl().setTenant()) {
            if (cacheLoader.exists()) {
                Map<Object, Data> keysToLoad = createHashMap(keys.size());
                for (Data key : keys) {
                    Object localKeyObj = dataToValue(key);
                    keysToLoad.put(localKeyObj, key);
                }
                Map<Object, Object> loaded;
                try {
                    loaded = cacheLoader.get().loadAll(keysToLoad.keySet());
                } catch (Throwable e) {
                    if (!(e instanceof CacheLoaderException)) {
                        throw new CacheLoaderException("Exception in CacheLoader during loadAll", e);
                    } else {
                        throw (CacheLoaderException) e;
                    }
                }
                Map<Data, Object> result = createHashMap(keysToLoad.size());
                for (Map.Entry<Object, Data> entry : keysToLoad.entrySet()) {
                    Object keyObj = entry.getKey();
                    Object valueObject = loaded.get(keyObj);
                    Data keyData = entry.getValue();
                    result.put(keyData, valueObject);
                }
                return result;
            }
            return null;
        }
    }

    @Override
    public CacheRecord getRecord(Data key) {
        return records.get(key);
    }

    @Override
    public void putRecord(Data key, CacheRecord record, boolean updateJournal) {
        evictIfRequired();
        doPutRecord(key, (R) record, SOURCE_NOT_AVAILABLE, updateJournal);
    }

    protected R doPutRecord(Data key, R record, UUID source, boolean updateJournal) {
        markExpirable(record.getExpirationTime());
        R oldRecord = records.put(key, record);
        if (updateJournal) {
            if (oldRecord != null) {
                compositeCacheRSMutationObserver.onUpdate(
                        key, oldRecord.getValue(), record.getValue());
            } else {
                compositeCacheRSMutationObserver.onCreate(
                        key, record.getValue());
            }
        }
        invalidateEntry(key, source);
        return oldRecord;
    }

    @Override
    public CacheRecord removeRecord(Data key) {
        return doRemoveRecord(key, SOURCE_NOT_AVAILABLE);
    }

    protected R doRemoveRecord(Data key, UUID source) {
        R removedRecord = records.remove(key);
        if (removedRecord != null) {
            compositeCacheRSMutationObserver.onRemove(
                    key, removedRecord.getValue());
            invalidateEntry(key, source);
        }
        return removedRecord;
    }

    protected void onGet(Data key, ExpiryPolicy expiryPolicy, Object value, R record) {
    }

    protected void onGetError(Data key, ExpiryPolicy expiryPolicy, Object value, R record, Throwable error) {
    }

    @Override
    public Object get(Data key, ExpiryPolicy expiryPolicy) {
        long startNanos = isStatisticsEnabled() ? Timer.nanos() : 0;
        long now = Clock.currentTimeMillis();
        Object value = null;
        R record = records.get(key);
        expiryPolicy = getExpiryPolicy(record, expiryPolicy);
        boolean isExpired = processExpiredEntry(key, record, now);
        try {
            if (recordNotExistOrExpired(record, isExpired)) {
                if (isStatisticsEnabled()) {
                    statistics.increaseCacheMisses(1);
                }
                value = readThroughCache(key);
                if (value == null) {
                    if (isStatisticsEnabled()) {
                        statistics.addGetTimeNanos(Timer.nanosElapsed(startNanos));
                    }
                    return null;
                }
                record = createRecordWithExpiry(key, value, expiryPolicy, now, true, IGNORE_COMPLETION);
            } else {
                value = recordToValue(record);
                onRecordAccess(key, record, expiryPolicy, now);
                if (isStatisticsEnabled()) {
                    statistics.increaseCacheHits(1);
                }
            }
            if (isStatisticsEnabled()) {
                statistics.addGetTimeNanos(Timer.nanosElapsed(startNanos));
            }
            onGet(key, expiryPolicy, value, record);
            return value;
        } catch (Throwable error) {
            onGetError(key, expiryPolicy, value, record, error);
            throw ExceptionUtil.rethrow(error);
        }
    }

    public void evictExpiredEntries(int expirationPercentage) {
        long now = Clock.currentTimeMillis();
        int maxIterationCount = getMaxIterationCount(size(), expirationPercentage);
        int evictedCount = 0;
        int maxRetry = 3;
        int loop = 0;
        while (loop < maxRetry) {
            evictedCount += evictExpiredInternal(maxIterationCount, now);
            if (evictedCount >= maxIterationCount) {
                break;
            }
            loop++;
        }
    }

    protected void initExpirationIterator() {
        if (expirationIterator == null || !expirationIterator.hasNext()) {
            expirationIterator = records.entrySet().iterator();
        }
    }

    private int evictExpiredInternal(int maxIterationCount, long now) {
        initExpirationIterator();
        int processedCount = 0;
        LinkedList<Map.Entry<Data, R>> records = new LinkedList<Map.Entry<Data, R>>();
        while (expirationIterator.hasNext() && processedCount < maxIterationCount) {
            Map.Entry<Data, R> record = expirationIterator.next();
            records.add(record);
            processedCount++;
        }
        int evictedCount = 0;
        while (!records.isEmpty()) {
            Map.Entry<Data, R> record = records.poll();
            Data key = record.getKey();
            R value = record.getValue();
            boolean expired = evictIfExpired(key, value, now);
            if (expired) {
                accumulateOrSendExpiredKeysToBackup(key, value);
                evictedCount++;
            }
        }
        return evictedCount;
    }

    private int getMaxIterationCount(int size, int percentage) {
        final int defaultMaxIterationCount = 100;
        final float oneHundred = 100F;
        float maxIterationCount = size * (percentage / oneHundred);
        if (maxIterationCount <= defaultMaxIterationCount) {
            return defaultMaxIterationCount;
        }
        return Math.round(maxIterationCount);
    }

    @Override
    public boolean contains(Data key) {
        long now = Clock.currentTimeMillis();
        R record = records.get(key);
        boolean isExpired = processExpiredEntry(key, record, now);
        return record != null && !isExpired;
    }

    @SuppressWarnings("checkstyle:parameternumber")
    protected void onPut(Data key, Object value, ExpiryPolicy expiryPolicy, UUID source,
                         boolean getValue, boolean disableWriteThrough, R record, Object oldValue,
                         boolean isExpired, boolean isNewPut, boolean isSaveSucceed) {
    }

    @SuppressWarnings("checkstyle:parameternumber")
    protected void onPutError(Data key, Object value, ExpiryPolicy expiryPolicy, UUID source,
                              boolean getValue, boolean disableWriteThrough, R record,
                              Object oldValue, boolean wouldBeNewPut, Throwable error) {
    }

    protected Object put(Data key, Object value, ExpiryPolicy expiryPolicy, UUID source,
                         boolean getValue, boolean disableWriteThrough, int completionId) {
        long now = Clock.currentTimeMillis();
        long startNanos = isStatisticsEnabled() ? Timer.nanos() : 0;
        boolean isOnNewPut = false;
        boolean isSaveSucceed;
        Object oldValue = null;
        R record = records.get(key);
        expiryPolicy = getExpiryPolicy(record, expiryPolicy);
        boolean isExpired = processExpiredEntry(key, record, now, source);
        try {
            // Check that new entry is not already expired, in which case it should
            // not be added to the cache or listeners called or writers called.
            if (recordNotExistOrExpired(record, isExpired)) {
                isOnNewPut = true;
                record = createRecordWithExpiry(key, value, expiryPolicy, now, disableWriteThrough, completionId, source);
                isSaveSucceed = record != null;
            } else {
                if (getValue) {
                    oldValue = toValue(record);
                }
                isSaveSucceed = updateRecordWithExpiry(key, value, record, expiryPolicy,
                        now, disableWriteThrough, completionId, source);
            }
            onPut(key, value, expiryPolicy, source, getValue, disableWriteThrough,
                    record, oldValue, isExpired, isOnNewPut, isSaveSucceed);
            updateGetAndPutStat(isSaveSucceed, getValue, oldValue == null, startNanos);
            if (getValue) {
                return oldValue;
            } else {
                return record;
            }
        } catch (Throwable error) {
            onPutError(key, value, expiryPolicy, source, getValue, disableWriteThrough,
                    record, oldValue, isOnNewPut, error);
            throw ExceptionUtil.rethrow(error);
        }
    }

    protected Object put(Data key, Object value, ExpiryPolicy expiryPolicy, UUID source,
                         boolean getValue, int completionId) {
        return put(key, value, expiryPolicy, source, getValue, false, completionId);
    }

    @Override
    public R put(Data key, Object value, ExpiryPolicy expiryPolicy, UUID source, int completionId) {
        return (R) put(key, value, expiryPolicy, source, false, false, completionId);
    }

    @Override
    public Object getAndPut(Data key, Object value, ExpiryPolicy expiryPolicy, UUID source, int completionId) {
        return put(key, value, expiryPolicy, source, true, false, completionId);
    }

    protected void onPutIfAbsent(Data key, Object value, ExpiryPolicy expiryPolicy, UUID source,
                                 boolean disableWriteThrough, R record, boolean isExpired, boolean isSaveSucceed) {
    }

    protected void onPutIfAbsentError(Data key, Object value, ExpiryPolicy expiryPolicy,
                                      UUID source, boolean disableWriteThrough, R record, Throwable error) {
    }

    protected boolean putIfAbsent(Data key, Object value, ExpiryPolicy expiryPolicy, UUID source,
                                  boolean disableWriteThrough, int completionId) {
        long now = Clock.currentTimeMillis();
        long startNanos = isStatisticsEnabled() ? Timer.nanos() : 0;
        boolean saved = false;
        R record = records.get(key);
        expiryPolicy = getExpiryPolicy(record, expiryPolicy);
        boolean isExpired = processExpiredEntry(key, record, now, source);
        boolean cacheMiss = recordNotExistOrExpired(record, isExpired);
        try {
            if (cacheMiss) {
                saved = createRecordWithExpiry(key, value, expiryPolicy, now,
                        disableWriteThrough, completionId, source) != null;
            } else {
                if (isEventsEnabled()) {
                    publishEvent(createCacheCompleteEvent(toEventData(key), completionId));
                }
            }
            onPutIfAbsent(key, value, expiryPolicy, source, disableWriteThrough, record, isExpired, saved);
            if (isStatisticsEnabled()) {
                if (saved) {
                    statistics.increaseCachePuts();
                    statistics.addPutTimeNanos(Timer.nanosElapsed(startNanos));
                }
                if (cacheMiss) {
                    statistics.increaseCacheMisses();
                } else {
                    statistics.increaseCacheHits();
                }
            }
            return saved;
        } catch (Throwable error) {
            onPutIfAbsentError(key, value, expiryPolicy, source, disableWriteThrough, record, error);
            throw ExceptionUtil.rethrow(error);
        }
    }

    @Override
    public boolean putIfAbsent(Data key, Object value, ExpiryPolicy expiryPolicy,
                               UUID source, int completionId) {
        return putIfAbsent(key, value, expiryPolicy, source, false, completionId);
    }

    @SuppressWarnings("checkstyle:parameternumber")
    protected void onReplace(Data key, Object oldValue, Object newValue, ExpiryPolicy expiryPolicy,
                             UUID source, boolean getValue, R record, boolean isExpired, boolean replaced) {
    }

    @SuppressWarnings("checkstyle:parameternumber")
    protected void onReplaceError(Data key, Object oldValue, Object newValue, ExpiryPolicy expiryPolicy, UUID source,
                                  boolean getValue, R record, boolean isExpired, boolean replaced, Throwable error) {
    }

    @Override
    public boolean replace(Data key, Object value, ExpiryPolicy expiryPolicy, UUID source, int completionId) {
        long now = Clock.currentTimeMillis();
        long startNanos = isStatisticsEnabled() ? Timer.nanos() : 0;
        boolean replaced = false;
        R record = records.get(key);
        expiryPolicy = getExpiryPolicy(record, expiryPolicy);
        boolean isExpired = record != null && record.isExpiredAt(now);
        try {
            if (recordNotExistOrExpired(record, isExpired)) {
                if (isEventsEnabled()) {
                    publishEvent(createCacheCompleteEvent(toEventData(key), completionId));
                }
            } else {
                replaced = updateRecordWithExpiry(key, value, record, expiryPolicy, now, false, completionId, source);
            }
            onReplace(key, null, value, expiryPolicy, source, false, record, isExpired, replaced);
            if (isStatisticsEnabled()) {
                if (replaced) {
                    statistics.increaseCachePuts(1);
                    statistics.increaseCacheHits(1);
                    statistics.addPutTimeNanos(Timer.nanosElapsed(startNanos));
                } else {
                    statistics.increaseCacheMisses(1);
                }
            }
            return replaced;
        } catch (Throwable error) {
            onReplaceError(key, null, value, expiryPolicy, source, false, record, isExpired, replaced, error);
            throw ExceptionUtil.rethrow(error);
        }
    }

    @Override
    public boolean replace(Data key, Object oldValue, Object newValue, ExpiryPolicy expiryPolicy,
                           UUID source, int completionId) {
        long now = Clock.currentTimeMillis();
        long startNanos = isStatisticsEnabled() ? Timer.nanos() : 0;
        boolean isHit = false;
        boolean replaced = false;
        R record = records.get(key);
        expiryPolicy = getExpiryPolicy(record, expiryPolicy);
        boolean isExpired = record != null && record.isExpiredAt(now);
        try {
            if (record != null && !isExpired) {
                isHit = true;
                Object currentValue = toStorageValue(record);
                if (compare(currentValue, toStorageValue(oldValue))) {
                    replaced = updateRecordWithExpiry(key, newValue, record, expiryPolicy,
                            now, false, completionId, source);
                } else {
                    onRecordAccess(key, record, expiryPolicy, now);
                }
            }
            if (!replaced) {
                if (isEventsEnabled()) {
                    publishEvent(createCacheCompleteEvent(toEventData(key), completionId));
                }
            }
            onReplace(key, oldValue, newValue, expiryPolicy, source, false, record, isExpired, replaced);
            updateReplaceStat(replaced, isHit, startNanos);
            return replaced;
        } catch (Throwable error) {
            onReplaceError(key, oldValue, newValue, expiryPolicy, source, false,
                    record, isExpired, replaced, error);
            throw ExceptionUtil.rethrow(error);
        }
    }

    @Override
    public Object getAndReplace(Data key, Object value, ExpiryPolicy expiryPolicy, UUID source, int completionId) {
        long now = Clock.currentTimeMillis();
        long startNanos = isStatisticsEnabled() ? Timer.nanos() : 0;
        boolean replaced = false;
        R record = records.get(key);
        expiryPolicy = getExpiryPolicy(record, expiryPolicy);
        boolean isExpired = record != null && record.isExpiredAt(now);
        try {
            Object obj = toValue(record);
            if (recordNotExistOrExpired(record, isExpired)) {
                obj = null;
                if (isEventsEnabled()) {
                    publishEvent(createCacheCompleteEvent(toEventData(key), completionId));
                }
            } else {
                replaced = updateRecordWithExpiry(key, value, record, expiryPolicy, now, false, completionId, source);
            }
            onReplace(key, null, value, expiryPolicy, source, false, record, isExpired, replaced);
            if (isStatisticsEnabled()) {
                statistics.addGetTimeNanos(Timer.nanosElapsed(startNanos));
                if (obj != null) {
                    statistics.increaseCacheHits(1);
                    statistics.increaseCachePuts(1);
                    statistics.addPutTimeNanos(Timer.nanosElapsed(startNanos));
                } else {
                    statistics.increaseCacheMisses(1);
                }
            }
            return obj;
        } catch (Throwable error) {
            onReplaceError(key, null, value, expiryPolicy, source, false, record, isExpired, replaced, error);
            throw ExceptionUtil.rethrow(error);
        }
    }

    @Override
    public boolean setExpiryPolicy(Collection<Data> keys, Object expiryPolicy, UUID source) {
        ExpiryPolicy expiryPolicyInstance = null;
        if (expiryPolicy instanceof Data) {
            expiryPolicyInstance = (ExpiryPolicy) toValue(expiryPolicy);
        }
        boolean atLeastOneKey = false;
        long now = System.currentTimeMillis();
        for (Data key : keys) {
            R record = records.get(key);
            if (record != null && !processExpiredEntry(key, record, now)) {
                updateExpiryPolicyOfRecord(key, record, expiryPolicy);
                updateRecordWithExpiry(key, record, expiryPolicyInstance, System.currentTimeMillis(), source);
                atLeastOneKey = true;
            }
        }
        return atLeastOneKey;
    }

    @Override
    public Object getExpiryPolicy(Data key) {
        CacheRecord record = getRecord(key);
        if (record != null) {
            return extractExpiryPolicyOfRecord(record);
        }
        return null;
    }

    protected void onRemove(Data key, Object value, UUID source, boolean getValue, R record, boolean removed) {
    }


    protected void onRemoveError(Data key, Object value, UUID source, boolean getValue,
                                 R record, boolean removed, Throwable error) {
    }

    @Override
    public boolean remove(Data key, UUID source, UUID origin, int completionId) {
        return remove(key, source, origin, completionId, CallerProvenance.NOT_WAN);
    }

    @Override
    public boolean remove(Data key, UUID source, UUID origin,
                          int completionId, CallerProvenance provenance) {
        long now = Clock.currentTimeMillis();
        long startNanos = isStatisticsEnabled() ? Timer.nanos() : 0;

        deleteCacheEntry(key, provenance);

        R record = records.get(key);
        boolean removed = false;
        try {
            if (recordNotExistOrExpired(record, now)) {
                if (isEventsEnabled()) {
                    publishEvent(createCacheCompleteEvent(toEventData(key), TIME_NOT_AVAILABLE,
                            origin, completionId));
                }
            } else {
                removed = deleteRecord(key, completionId, source, origin);
            }
            onRemove(key, null, source, false, record, removed);
            if (removed && isStatisticsEnabled()) {
                statistics.increaseCacheRemovals(1);
                statistics.addRemoveTimeNanos(Timer.nanosElapsed(startNanos));
            }
            return removed;
        } catch (Throwable error) {
            onRemoveError(key, null, source, false, record, removed, error);
            throw ExceptionUtil.rethrow(error);
        }
    }

    @Override
    public InvalidationQueue<ExpiredKey> getExpiredKeysQueue() {
        return this.expiredKeys;
    }

    @Override
    public boolean remove(Data key, Object value, UUID source, UUID origin, int completionId) {
        long now = Clock.currentTimeMillis();
        long startNanos = Timer.nanos();
        R record = records.get(key);
        int hitCount = 0;
        boolean removed = false;
        try {
            if (recordNotExistOrExpired(record, now)) {
                if (isStatisticsEnabled()) {
                    statistics.increaseCacheMisses(1);
                }
            } else {
                hitCount++;
                if (compare(toStorageValue(record), toStorageValue(value))) {
                    deleteCacheEntry(key);
                    removed = deleteRecord(key, completionId, source, origin);
                } else {
                    long expiryTime = onRecordAccess(key, record, defaultExpiryPolicy.get(), now);
                    processExpiredEntry(key, record, expiryTime, now, source, origin);
                }
            }
            if (!removed) {
                if (isEventsEnabled()) {
                    publishEvent(createCacheCompleteEvent(toEventData(key), TIME_NOT_AVAILABLE,
                            origin, completionId));
                }
            }
            onRemove(key, value, source, false, record, removed);
            updateRemoveStatistics(removed, hitCount, startNanos);
            return removed;
        } catch (Throwable error) {
            onRemoveError(key, null, source, false, record, removed, error);
            throw ExceptionUtil.rethrow(error);
        }
    }

    private void updateRemoveStatistics(boolean result, int hitCount, long startNanos) {
        if (result && isStatisticsEnabled()) {
            statistics.increaseCacheRemovals(1);
            statistics.addRemoveTimeNanos(Timer.nanosElapsed(startNanos));
            if (hitCount == 1) {
                statistics.increaseCacheHits(hitCount);
            } else {
                statistics.increaseCacheMisses(1);
            }
        }
    }

    /**
     * This method marks current replica as expirable and also starts expiration task if necessary.
     * <p>
     * The expiration task runs on only primary replicas. Expiration on backup replicas are dictated by primary replicas. However,
     * it is still important to mark a backup replica as expirable because it might be promoted to be the primary in a later time.
     *
     * @param expiryTime
     */
    protected void markExpirable(long expiryTime) {
        if (expiryTime > 0 && expiryTime < Long.MAX_VALUE) {
            hasEntryWithExpiration = true;
        }

        if (isPrimary() && hasEntryWithExpiration) {
            cacheService.getExpirationManager().scheduleExpirationTask();
        }
    }

    @Override
    public Object getAndRemove(Data key, UUID source, int completionId) {
        return getAndRemove(key, source, completionId, null);
    }

    public Object getAndRemove(Data key, UUID source, int completionId, UUID origin) {
        long now = Clock.currentTimeMillis();
        long startNanos = isStatisticsEnabled() ? Timer.nanos() : 0;

        deleteCacheEntry(key);

        R record = records.get(key);
        Object obj;
        boolean removed = false;
        try {
            if (recordNotExistOrExpired(record, now)) {
                obj = null;
                if (isEventsEnabled()) {
                    publishEvent(createCacheCompleteEvent(toEventData(key), TIME_NOT_AVAILABLE,
                            origin, completionId));
                }
            } else {
                obj = toValue(record);
                removed = deleteRecord(key, completionId, source, origin);
            }
            onRemove(key, null, source, false, record, removed);
            if (isStatisticsEnabled()) {
                statistics.addGetTimeNanos(Timer.nanosElapsed(startNanos));
                if (obj != null) {
                    statistics.increaseCacheHits(1);
                    statistics.increaseCacheRemovals(1);
                    statistics.addRemoveTimeNanos(Timer.nanosElapsed(startNanos));
                } else {
                    statistics.increaseCacheMisses(1);
                }
            }
            return obj;
        } catch (Throwable error) {
            onRemoveError(key, null, source, false, record, removed, error);
            throw ExceptionUtil.rethrow(error);
        }
    }

    @Override
    public MapEntries getAll(Set<Data> keySet, ExpiryPolicy expiryPolicy) {
        MapEntries result = new MapEntries(keySet.size());
        for (Data key : keySet) {
            Object value = get(key, expiryPolicy);
            if (value != null) {
                result.add(key, toHeapData(value));
            }
        }
        return result;
    }

    @Override
    public void removeAll(Set<Data> keys, int completionId) {
        long now = Clock.currentTimeMillis();
        Set<Data> localKeys = new HashSet<Data>(keys.isEmpty() ? records.keySet() : keys);
        try {
            deleteAllCacheEntry(localKeys);
        } finally {
            Set<Data> keysToClean = new HashSet<Data>(keys.isEmpty() ? records.keySet() : keys);
            for (Data key : keysToClean) {
                eventsBatchingEnabled = true;
                R record = records.get(key);
                if (localKeys.contains(key) && record != null) {
                    boolean isExpired = processExpiredEntry(key, record, now);
                    if (!isExpired) {
                        deleteRecord(key, IGNORE_COMPLETION);
                        if (isStatisticsEnabled()) {
                            statistics.increaseCacheRemovals(1);
                        }
                    }
                    keys.add(key);
                } else {
                    keys.remove(key);
                }
                eventsBatchingEnabled = false;
            }
            int orderKey = keys.hashCode();
            publishBatchedEvents(name, CacheEventType.REMOVED, orderKey);
            if (isEventsEnabled()) {
                publishEvent(createCacheCompleteEvent(completionId));
            }
        }
    }

    @Override
    public Set<Data> loadAll(Set<Data> keys, boolean replaceExistingValues) {
        Map<Data, Object> loaded = loadAllCacheEntry(keys);
        if (loaded == null || loaded.isEmpty()) {
            return emptySet();
        }
        Set<Data> keysLoaded = createHashSet(loaded.size());
        if (replaceExistingValues) {
            for (Map.Entry<Data, Object> entry : loaded.entrySet()) {
                Data key = entry.getKey();
                Object value = entry.getValue();
                if (value != null) {
                    put(key, value, null, SOURCE_NOT_AVAILABLE, false, true, IGNORE_COMPLETION);
                    keysLoaded.add(key);
                }
            }
        } else {
            for (Map.Entry<Data, Object> entry : loaded.entrySet()) {
                Data key = entry.getKey();
                Object value = entry.getValue();
                if (value != null) {
                    boolean hasPut = putIfAbsent(key, value, null, SOURCE_NOT_AVAILABLE, true, IGNORE_COMPLETION);
                    if (hasPut) {
                        keysLoaded.add(key);
                    }
                }
            }
        }
        return keysLoaded;
    }

    @Override
    public CacheRecord merge(CacheMergeTypes<Object, Object> mergingEntry,
                             SplitBrainMergePolicy<Object, CacheMergeTypes<Object, Object>, Object> mergePolicy,
                             CallerProvenance callerProvenance) {
        final long now = Clock.currentTimeMillis();
        final long startNanos = isStatisticsEnabled() ? Timer.nanos() : 0;

        mergingEntry = injectDependencies(mergingEntry);
        mergePolicy = injectDependencies(mergePolicy);

        boolean merged = false;
        Data key = (Data) mergingEntry.getRawKey();
        long expiryTime = mergingEntry.getExpirationTime();
        R record = records.get(key);
        boolean isExpired = processExpiredEntry(key, record, now);
        boolean disableWriteThrough = !persistenceEnabledFor(callerProvenance);

        if (record == null || isExpired) {
            Object newValue = mergePolicy.merge(mergingEntry, null);
            if (newValue != null) {
                record = createRecordWithExpiry(key, newValue, expiryTime, now, disableWriteThrough, IGNORE_COMPLETION);
                merged = record != null;
            }
        } else {
            Data oldValue = ss.toData(record.getValue());
            CacheMergeTypes<Object, Object> existingEntry = createMergingEntry(ss, key, oldValue, record);
            Object newValue = mergePolicy.merge(mergingEntry, existingEntry);

            merged = updateWithMergingValue(key, oldValue, newValue, record, expiryTime, now, disableWriteThrough);
        }

        if (merged && isStatisticsEnabled()) {
            statistics.increaseCachePuts(1);
            statistics.addPutTimeNanos(Timer.nanosElapsed(startNanos));
        }

        return merged ? record : null;
    }

    private boolean updateWithMergingValue(Data key, Object existingValue, Object mergingValue,
                                           R record, long expiryTime, long now, boolean disableWriteThrough) {

        if (valueComparator.isEqual(existingValue, mergingValue, ss)) {
            updateExpiryTime(record, expiryTime);
            processExpiredEntry(key, record, now);
            return true;
        }

        return updateRecordWithExpiry(key, mergingValue, record, TIME_NOT_AVAILABLE,
                now, disableWriteThrough, IGNORE_COMPLETION);
    }

    private Object getExpiryPolicyOrNull(R record) {
        if (record != null) {
            return record.getExpiryPolicy();
        }
        return null;
    }

    @Override
    public CacheKeysWithCursor fetchKeys(IterationPointer[] pointers, int size) {
        return records.fetchKeys(pointers, size);
    }

    @Override
    public CacheEntriesWithCursor fetchEntries(IterationPointer[] pointers, int size) {
        return records.fetchEntries(pointers, size);
    }

    @Override
    public Object invoke(Data key, EntryProcessor entryProcessor, Object[] arguments, int completionId) {
        long now = Clock.currentTimeMillis();
        long startNanos = isStatisticsEnabled() ? Timer.nanos() : 0;
        R record = records.get(key);
        boolean isExpired = processExpiredEntry(key, record, now);
        if (isExpired) {
            record = null;
        }
        if (isStatisticsEnabled()) {
            if (recordNotExistOrExpired(record, isExpired)) {
                statistics.increaseCacheMisses(1);
            } else {
                statistics.increaseCacheHits(1);
            }
            statistics.addGetTimeNanos(Timer.nanosElapsed(startNanos));
        }
        CacheEntryProcessorEntry entry = createCacheEntryProcessorEntry(key, record, now, completionId);
        entryProcessor = injectDependencies(entryProcessor);
        Object result = entryProcessor.process(entry, arguments);
        entry.applyChanges();
        return result;
    }

    private boolean recordNotExistOrExpired(R record, boolean isExpired) {
        return record == null || isExpired;
    }

    private boolean recordNotExistOrExpired(R record, long now) {
        return record == null || record.isExpiredAt(now);
    }

    @Override
    public int size() {
        return records.size();
    }

    @Override
    public CacheStatisticsImpl getCacheStats() {
        return statistics;
    }

    @Override
    public CacheConfig getConfig() {
        return cacheConfig;
    }

    /**
     * {@inheritDoc}
     *
     * @return the full name of the cache, including the manager scope prefix
     */
    @Override
    public String getName() {
        return name;
    }

    @Override
    public Map<Data, CacheRecord> getReadOnlyRecords() {
        return (Map<Data, CacheRecord>) Collections.unmodifiableMap(records);
    }

    @Override
    public void clear() {
        reset();
        destroyEventJournal();
    }

    protected void destroyEventJournal() {
        compositeCacheRSMutationObserver.onDestroy();
    }

    @Override
    public void reset() {
        records.clear();
    }

    @Override
    public void close(boolean onShutdown) {
        clear();
        closeListeners();
    }

    @Override
    public void destroy() {
        clear();
        closeListeners();
        onDestroy();
    }

    @Override
    public void destroyInternals() {
        reset();
        closeListeners();
        onDestroy();
    }

    protected void onDestroy() {
    }

    protected void closeListeners() {
        EventService eventService = cacheService.getNodeEngine().getEventService();
        Collection<EventRegistration> candidates = eventService.getRegistrations(ICacheService.SERVICE_NAME, name);
        for (EventRegistration eventRegistration : candidates) {
            eventService.close(eventRegistration);
        }
    }

    @Override
    public boolean isWanReplicationEnabled() {
        return wanReplicationEnabled;
    }

    @Override
    public ObjectNamespace getObjectNamespace() {
        return objectNamespace;
    }

    @Override
    public int getPartitionId() {
        return partitionId;
    }
}
