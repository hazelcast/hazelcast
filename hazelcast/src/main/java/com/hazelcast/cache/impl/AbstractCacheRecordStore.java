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

import com.hazelcast.cache.CacheEntryView;
import com.hazelcast.cache.CacheEventType;
import com.hazelcast.cache.CacheMergePolicy;
import com.hazelcast.cache.CacheNotExistsException;
import com.hazelcast.cache.StorageTypeAwareCacheMergePolicy;
import com.hazelcast.cache.impl.maxsize.impl.EntryCountCacheEvictionChecker;
import com.hazelcast.cache.impl.merge.entry.LazyCacheEntryView;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.cache.impl.record.SampleableCacheRecordMap;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionConfig.MaxSizePolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.internal.diagnostics.StoreLatencyPlugin;
import com.hazelcast.internal.eviction.EvictionChecker;
import com.hazelcast.internal.eviction.EvictionListener;
import com.hazelcast.internal.eviction.EvictionPolicyEvaluatorProvider;
import com.hazelcast.internal.eviction.impl.evaluator.EvictionPolicyEvaluator;
import com.hazelcast.internal.eviction.impl.strategy.sampling.SamplingEvictionStrategy;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.eventservice.InternalEventService;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.CacheMergeTypes;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ExceptionUtil;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

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
import java.util.Map;
import java.util.Set;

import static com.hazelcast.cache.impl.CacheEventContextUtil.createBaseEventContext;
import static com.hazelcast.cache.impl.CacheEventContextUtil.createCacheCompleteEvent;
import static com.hazelcast.cache.impl.CacheEventContextUtil.createCacheCreatedEvent;
import static com.hazelcast.cache.impl.CacheEventContextUtil.createCacheExpiredEvent;
import static com.hazelcast.cache.impl.CacheEventContextUtil.createCacheRemovedEvent;
import static com.hazelcast.cache.impl.CacheEventContextUtil.createCacheUpdatedEvent;
import static com.hazelcast.cache.impl.operation.MutableOperation.IGNORE_COMPLETION;
import static com.hazelcast.cache.impl.record.CacheRecordFactory.isExpiredAt;
import static com.hazelcast.internal.config.ConfigValidator.checkEvictionConfig;
import static com.hazelcast.spi.impl.merge.MergingValueFactory.createMergingEntry;
import static com.hazelcast.util.EmptyStatement.ignore;
import static com.hazelcast.util.MapUtil.createHashMap;
import static com.hazelcast.util.SetUtil.createHashSet;
import static java.util.Collections.emptySet;

@SuppressWarnings({"checkstyle:methodcount", "checkstyle:classfanoutcomplexity"})
public abstract class AbstractCacheRecordStore<R extends CacheRecord, CRM extends SampleableCacheRecordMap<Data, R>>
        implements ICacheRecordStore, EvictionListener<Data, R> {

    public static final String SOURCE_NOT_AVAILABLE = "<NA>";
    protected static final int DEFAULT_INITIAL_CAPACITY = 256;

    /**
     * the full name of the cache, including the manager scope prefix
     */
    protected final String name;
    protected final int partitionId;
    protected final int partitionCount;
    protected final NodeEngine nodeEngine;
    protected final AbstractCacheService cacheService;
    protected final CacheConfig cacheConfig;
    protected final EventJournalConfig eventJournalConfig;
    protected final EvictionConfig evictionConfig;
    protected final Map<CacheEventType, Set<CacheEventData>> batchEvent = new HashMap<CacheEventType, Set<CacheEventData>>();
    protected final EvictionChecker evictionChecker;
    protected final EvictionPolicyEvaluator<Data, R> evictionPolicyEvaluator;
    protected final SamplingEvictionStrategy<Data, R, CRM> evictionStrategy;
    protected final ObjectNamespace objectNamespace;
    protected final boolean wanReplicationEnabled;
    protected final boolean disablePerEntryInvalidationEvents;
    protected CRM records;
    protected CacheContext cacheContext;
    protected CacheStatisticsImpl statistics;
    protected CacheLoader cacheLoader;
    protected CacheWriter cacheWriter;
    protected boolean eventsEnabled = true;
    protected boolean eventsBatchingEnabled;
    protected ExpiryPolicy defaultExpiryPolicy;
    protected boolean primary;

    @SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:executablestatementcount"})
    public AbstractCacheRecordStore(String cacheNameWithPrefix, int partitionId, NodeEngine nodeEngine,
                                    AbstractCacheService cacheService) {
        this.name = cacheNameWithPrefix;
        this.partitionId = partitionId;
        this.nodeEngine = nodeEngine;
        this.partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        this.cacheService = cacheService;
        this.cacheConfig = cacheService.getCacheConfig(cacheNameWithPrefix);
        if (cacheConfig == null) {
            throw new CacheNotExistsException("Cache " + cacheNameWithPrefix + " is already destroyed or not created yet, on "
                    + nodeEngine.getLocalMember());
        }
        this.eventJournalConfig = nodeEngine.getConfig().findCacheEventJournalConfig(cacheConfig.getName());
        evictionConfig = cacheConfig.getEvictionConfig();
        if (evictionConfig == null) {
            throw new IllegalStateException("Eviction config cannot be null!");
        }
        wanReplicationEnabled = cacheService.isWanReplicationEnabled(cacheNameWithPrefix);
        disablePerEntryInvalidationEvents = cacheConfig.isDisablePerEntryInvalidationEvents();
        if (cacheConfig.isStatisticsEnabled()) {
            statistics = cacheService.createCacheStatIfAbsent(cacheNameWithPrefix);
        }
        if (cacheConfig.getCacheLoaderFactory() != null) {
            Factory<CacheLoader> cacheLoaderFactory = cacheConfig.getCacheLoaderFactory();
            injectDependencies(cacheLoaderFactory);
            cacheLoader = cacheLoaderFactory.create();
            injectDependencies(cacheLoader);
        }
        if (cacheConfig.getCacheWriterFactory() != null) {
            Factory<CacheWriter> cacheWriterFactory = cacheConfig.getCacheWriterFactory();
            injectDependencies(cacheWriterFactory);
            cacheWriter = cacheWriterFactory.create();
            injectDependencies(cacheWriter);
        }
        if (cacheConfig.getExpiryPolicyFactory() != null) {
            Factory<ExpiryPolicy> expiryPolicyFactory = cacheConfig.getExpiryPolicyFactory();
            injectDependencies(expiryPolicyFactory);
            defaultExpiryPolicy = expiryPolicyFactory.create();
            injectDependencies(defaultExpiryPolicy);
        } else {
            throw new IllegalStateException("Expiry policy factory cannot be null!");
        }

        cacheContext = cacheService.getOrCreateCacheContext(cacheNameWithPrefix);
        records = createRecordCacheMap();
        evictionChecker = createCacheEvictionChecker(evictionConfig.getSize(), evictionConfig.getMaximumSizePolicy());
        evictionPolicyEvaluator = createEvictionPolicyEvaluator(evictionConfig);
        evictionStrategy = createEvictionStrategy(evictionConfig);
        objectNamespace = CacheService.getObjectNamespace(cacheNameWithPrefix);

        injectDependencies(evictionPolicyEvaluator.getEvictionPolicyComparator());
        registerResourceIfItIsClosable(cacheWriter);
        registerResourceIfItIsClosable(cacheLoader);
        registerResourceIfItIsClosable(defaultExpiryPolicy);
        init();
    }

    public void instrument(NodeEngine nodeEngine) {
        StoreLatencyPlugin plugin = ((NodeEngineImpl) nodeEngine).getDiagnostics().getPlugin(StoreLatencyPlugin.class);
        if (plugin == null) {
            return;
        }

        if (cacheLoader != null) {
            cacheLoader = new LatencyTrackingCacheLoader(cacheLoader, plugin, cacheConfig.getName());
        }

        if (cacheWriter != null) {
            cacheWriter = new LatencyTrackingCacheWriter(cacheWriter, plugin, cacheConfig.getName());
        }
    }

    private boolean isPrimary() {
        final Address owner = nodeEngine.getPartitionService().getPartition(partitionId, false).getOwnerOrNull();
        final Address thisAddress = nodeEngine.getThisAddress();
        return owner != null && owner.equals(thisAddress);
    }

    private void injectDependencies(Object obj) {
        ManagedContext managedContext = nodeEngine.getSerializationService().getManagedContext();
        managedContext.initialize(obj);
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

    protected EvictionPolicyEvaluator<Data, R> createEvictionPolicyEvaluator(EvictionConfig evictionConfig) {
        checkEvictionConfig(evictionConfig, false);
        return EvictionPolicyEvaluatorProvider.getEvictionPolicyEvaluator(evictionConfig, nodeEngine.getConfigClassLoader());
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

    protected ExpiryPolicy getExpiryPolicy(ExpiryPolicy expiryPolicy) {
        return expiryPolicy != null ? expiryPolicy : defaultExpiryPolicy;
    }

    protected boolean processExpiredEntry(Data key, R record, long now) {
        return processExpiredEntry(key, record, now, SOURCE_NOT_AVAILABLE);
    }

    protected boolean processExpiredEntry(Data key, R record, long now, String source) {
        return processExpiredEntry(key, record, now, source, null);
    }

    protected boolean processExpiredEntry(Data key, R record, long now, String source, String origin) {
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
        onProcessExpiredEntry(key, removedRecord, removedRecord.getExpirationTime(), now, source, origin);
        if (isEventsEnabled()) {
            publishEvent(createCacheExpiredEvent(keyEventData, recordEventData,
                    CacheRecord.TIME_NOT_AVAILABLE, origin, IGNORE_COMPLETION));
        }
        return true;
    }

    protected R processExpiredEntry(Data key, R record, long expiryTime, long now, String source) {
        return processExpiredEntry(key, record, expiryTime, now, source, null);
    }

    protected R processExpiredEntry(Data key, R record, long expiryTime, long now, String source, String origin) {
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
            publishEvent(createCacheExpiredEvent(keyEventData, recordEventData, CacheRecord.TIME_NOT_AVAILABLE,
                    origin, IGNORE_COMPLETION));
        }
        return null;
    }

    protected void onProcessExpiredEntry(Data key, R record, long expiryTime, long now, String source, String origin) {
    }

    public R accessRecord(Data key, R record, ExpiryPolicy expiryPolicy, long now) {
        onRecordAccess(key, record, getExpiryPolicy(expiryPolicy), now);
        return record;
    }

    @Override
    public void onEvict(Data key, R record, boolean wasExpired) {
        if (wasExpired) {
            cacheService.eventJournal.writeExpiredEvent(eventJournalConfig, objectNamespace, partitionId, key, record.getValue());
        } else {
            cacheService.eventJournal.writeEvictEvent(eventJournalConfig, objectNamespace, partitionId, key, record.getValue());
        }
        invalidateEntry(key);
    }

    protected void invalidateEntry(Data key, String source) {
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

    protected void updateGetAndPutStat(boolean isPutSucceed, boolean getValue, boolean oldValueNull, long start) {
        if (isStatisticsEnabled()) {
            if (isPutSucceed) {
                statistics.increaseCachePuts(1);
                statistics.addPutTimeNanos(System.nanoTime() - start);
            }
            if (getValue) {
                if (oldValueNull) {
                    statistics.increaseCacheMisses(1);
                } else {
                    statistics.increaseCacheHits(1);
                }
                statistics.addGetTimeNanos(System.nanoTime() - start);
            }
        }
    }

    protected long updateAccessDuration(Data key, R record, ExpiryPolicy expiryPolicy, long now) {
        long expiryTime = CacheRecord.TIME_NOT_AVAILABLE;
        try {
            Duration expiryDuration = expiryPolicy.getExpiryForAccess();
            if (expiryDuration != null) {
                expiryTime = getAdjustedExpireTime(expiryDuration, now);
                record.setExpirationTime(expiryTime);
                if (isEventsEnabled()) {
                    CacheEventContext cacheEventContext =
                            createBaseEventContext(CacheEventType.EXPIRATION_TIME_UPDATED, toEventData(key),
                                    toEventData(record.getValue()), expiryTime, null, IGNORE_COMPLETION);
                    cacheEventContext.setAccessHit(record.getAccessHit());
                    publishEvent(cacheEventContext);
                }
            }
        } catch (Exception e) {
            ignore(e);
        }
        return expiryTime;
    }

    protected long onRecordAccess(Data key, R record, ExpiryPolicy expiryPolicy, long now) {
        record.setAccessTime(now);
        record.incrementAccessHit();
        return updateAccessDuration(key, record, expiryPolicy, now);
    }

    protected void updateReplaceStat(boolean result, boolean isHit, long start) {
        if (isStatisticsEnabled()) {
            if (result) {
                statistics.increaseCachePuts(1);
                statistics.addPutTimeNanos(System.nanoTime() - start);
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
                                       int completionId, String origin, R record, Throwable error) {
    }

    protected R createRecord(Data key, Object value, long expiryTime, long now,
                             boolean disableWriteThrough, int completionId, String origin) {
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
                cacheService.eventJournal.writeRemoveEvent(eventJournalConfig, objectNamespace, partitionId,
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
                                       long now, boolean disableWriteThrough, int completionId, String origin) {
        if (!isExpiredAt(expiryTime, now)) {
            return createRecord(key, value, expiryTime, now, disableWriteThrough, completionId, origin);
        }
        if (isEventsEnabled()) {
            publishEvent(createCacheCompleteEvent(toEventData(key), CacheRecord.TIME_NOT_AVAILABLE,
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
                                       long now, boolean disableWriteThrough, int completionId, String origin) {
        expiryPolicy = getExpiryPolicy(expiryPolicy);
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
        cacheService.eventJournal.writeUpdateEvent(eventJournalConfig, objectNamespace, partitionId, key, oldDataValue, value);
    }

    protected void onUpdateRecordError(Data key, R record, Object value, Data newDataValue,
                                       Data oldDataValue, Throwable error) {
    }

    @SuppressWarnings("checkstyle:parameternumber")
    protected void updateRecord(Data key, R record, Object value, long expiryTime, long now,
                                boolean disableWriteThrough, int completionId, String source, String origin) {
        Data dataOldValue = null;
        Data dataValue = null;
        Object recordValue = value;
        try {
            if (expiryTime != CacheRecord.TIME_NOT_AVAILABLE) {
                record.setExpirationTime(expiryTime);
            }
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

                updateRecordValue(record, recordValue);
                onUpdateRecord(key, record, value, dataOldValue);
                invalidateEntry(key, source);

                if (isEventsEnabled()) {
                    publishEvent(createCacheUpdatedEvent(eventDataKey, eventDataValue, eventDataOldValue,
                            record.getCreationTime(), record.getExpirationTime(),
                            record.getLastAccessTime(), record.getAccessHit(),
                            origin, completionId));
                }
            }
        } catch (Throwable error) {
            onUpdateRecordError(key, record, value, dataValue, dataOldValue, error);
            throw ExceptionUtil.rethrow(error);
        }
    }

    protected void updateRecordValue(R record, Object recordValue) {
        record.setValue(recordValue);
    }

    @SuppressWarnings("checkstyle:parameternumber")
    protected boolean updateRecordWithExpiry(Data key, Object value, R record, long expiryTime, long now,
                                             boolean disableWriteThrough, int completionId,
                                             String source, String origin) {
        updateRecord(key, record, value, expiryTime, now, disableWriteThrough, completionId, source, origin);
        return processExpiredEntry(key, record, expiryTime, now, source) != null;
    }

    protected boolean updateRecordWithExpiry(Data key, Object value, R record, long expiryTime,
                                             long now, boolean disableWriteThrough, int completionId) {
        return updateRecordWithExpiry(key, value, record, expiryTime, now, disableWriteThrough,
                completionId, SOURCE_NOT_AVAILABLE);
    }

    protected boolean updateRecordWithExpiry(Data key, Object value, R record, long expiryTime,
                                             long now, boolean disableWriteThrough, int completionId, String source) {
        return updateRecordWithExpiry(key, value, record, expiryTime, now,
                disableWriteThrough, completionId, source, null);
    }

    protected boolean updateRecordWithExpiry(Data key, Object value, R record, ExpiryPolicy expiryPolicy,
                                             long now, boolean disableWriteThrough, int completionId) {
        return updateRecordWithExpiry(key, value, record, expiryPolicy, now,
                disableWriteThrough, completionId, SOURCE_NOT_AVAILABLE);
    }

    protected boolean updateRecordWithExpiry(Data key, Object value, R record, ExpiryPolicy expiryPolicy,
                                             long now, boolean disableWriteThrough, int completionId, String source) {
        return updateRecordWithExpiry(key, value, record, expiryPolicy, now,
                disableWriteThrough, completionId, source, null);
    }

    @SuppressWarnings("checkstyle:parameternumber")
    protected boolean updateRecordWithExpiry(Data key, Object value, R record, ExpiryPolicy expiryPolicy, long now,
                                             boolean disableWriteThrough, int completionId, String source, String origin) {
        expiryPolicy = getExpiryPolicy(expiryPolicy);
        long expiryTime = CacheRecord.TIME_NOT_AVAILABLE;
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

    protected void onDeleteRecord(Data key, R record, Data dataValue, boolean deleted) {
    }

    protected void onDeleteRecordError(Data key, R record, Data dataValue, boolean deleted, Throwable error) {
    }

    protected boolean deleteRecord(Data key, int completionId) {
        return deleteRecord(key, completionId, SOURCE_NOT_AVAILABLE);
    }

    protected boolean deleteRecord(Data key, int completionId, String source) {
        return deleteRecord(key, completionId, source, null);
    }

    protected boolean deleteRecord(Data key, int completionId, String source, String origin) {
        R record = doRemoveRecord(key, source);
        Data dataValue = null;
        try {
            switch (cacheConfig.getInMemoryFormat()) {
                case BINARY:
                case OBJECT:
                case NATIVE:
                    dataValue = toData(record);
                    break;
                default:
                    throw new IllegalArgumentException("Invalid storage format: " + cacheConfig.getInMemoryFormat());
            }
            Data eventDataKey = toEventData(key);
            Data eventDataValue = toEventData(dataValue);
            onDeleteRecord(key, record, dataValue, record != null);
            if (isEventsEnabled()) {
                publishEvent(createCacheRemovedEvent(eventDataKey, eventDataValue,
                        CacheRecord.TIME_NOT_AVAILABLE, origin, completionId));
            }
            return record != null;
        } catch (Throwable error) {
            onDeleteRecordError(key, record, dataValue, record != null, error);
            throw ExceptionUtil.rethrow(error);
        }
    }

    public R readThroughRecord(Data key, long now) {
        Object value = readThroughCache(key);
        if (value == null) {
            return null;
        }
        Duration expiryDuration;
        try {
            expiryDuration = defaultExpiryPolicy.getExpiryForCreation();
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
        if (isReadThrough() && cacheLoader != null) {
            try {
                Object o = dataToValue(key);
                return cacheLoader.load(o);
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
        if (isWriteThrough() && cacheWriter != null) {
            try {
                Object objKey = dataToValue(key);
                Object objValue = toValue(value);
                cacheWriter.write(new CacheEntry<Object, Object>(objKey, objValue));
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
        if (isWriteThrough() && cacheWriter != null) {
            try {
                Object objKey = dataToValue(key);
                cacheWriter.delete(objKey);
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
        if (isWriteThrough() && cacheWriter != null && keys != null && !keys.isEmpty()) {
            Map<Object, Data> keysToDelete = createHashMap(keys.size());
            for (Data key : keys) {
                Object localKeyObj = dataToValue(key);
                keysToDelete.put(localKeyObj, key);
            }
            Set<Object> keysObject = keysToDelete.keySet();
            try {
                cacheWriter.deleteAll(keysObject);
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

    protected Map<Data, Object> loadAllCacheEntry(Set<Data> keys) {
        if (cacheLoader != null) {
            Map<Object, Data> keysToLoad = createHashMap(keys.size());
            for (Data key : keys) {
                Object localKeyObj = dataToValue(key);
                keysToLoad.put(localKeyObj, key);
            }
            Map<Object, Object> loaded;
            try {
                loaded = cacheLoader.loadAll(keysToLoad.keySet());
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

    @Override
    public CacheRecord getRecord(Data key) {
        return records.get(key);
    }

    @Override
    public void putRecord(Data key, CacheRecord record, boolean updateJournal) {
        evictIfRequired();
        doPutRecord(key, (R) record, SOURCE_NOT_AVAILABLE, updateJournal);
    }

    protected R doPutRecord(Data key, R record, String source, boolean updateJournal) {
        R oldRecord = records.put(key, record);
        if (updateJournal) {
            if (oldRecord != null) {
                cacheService.eventJournal.writeUpdateEvent(
                        eventJournalConfig, objectNamespace, partitionId, key, oldRecord.getValue(), record.getValue());
            } else {
                cacheService.eventJournal.writeCreatedEvent(
                        eventJournalConfig, objectNamespace, partitionId, key, record.getValue());
            }
        }
        invalidateEntry(key, source);
        return oldRecord;
    }

    @Override
    public CacheRecord removeRecord(Data key) {
        return doRemoveRecord(key, SOURCE_NOT_AVAILABLE);
    }

    protected R doRemoveRecord(Data key, String source) {
        R removedRecord = records.remove(key);
        if (removedRecord != null) {
            cacheService.eventJournal.writeRemoveEvent(eventJournalConfig, objectNamespace, partitionId,
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
        expiryPolicy = getExpiryPolicy(expiryPolicy);
        long start = isStatisticsEnabled() ? System.nanoTime() : 0;
        long now = Clock.currentTimeMillis();
        Object value = null;
        R record = records.get(key);
        boolean isExpired = processExpiredEntry(key, record, now);
        try {
            if (recordNotExistOrExpired(record, isExpired)) {
                if (isStatisticsEnabled()) {
                    statistics.increaseCacheMisses(1);
                }
                value = readThroughCache(key);
                if (value == null) {
                    if (isStatisticsEnabled()) {
                        statistics.addGetTimeNanos(System.nanoTime() - start);
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
                statistics.addGetTimeNanos(System.nanoTime() - start);
            }
            onGet(key, expiryPolicy, value, record);
            return value;
        } catch (Throwable error) {
            onGetError(key, expiryPolicy, value, record, error);
            throw ExceptionUtil.rethrow(error);
        }
    }

    @Override
    public boolean contains(Data key) {
        long now = Clock.currentTimeMillis();
        R record = records.get(key);
        boolean isExpired = processExpiredEntry(key, record, now);
        return record != null && !isExpired;
    }

    @SuppressWarnings("checkstyle:parameternumber")
    protected void onPut(Data key, Object value, ExpiryPolicy expiryPolicy, String source,
                         boolean getValue, boolean disableWriteThrough, R record, Object oldValue,
                         boolean isExpired, boolean isNewPut, boolean isSaveSucceed) {
    }

    @SuppressWarnings("checkstyle:parameternumber")
    protected void onPutError(Data key, Object value, ExpiryPolicy expiryPolicy, String source,
                              boolean getValue, boolean disableWriteThrough, R record,
                              Object oldValue, boolean wouldBeNewPut, Throwable error) {
    }

    protected Object put(Data key, Object value, ExpiryPolicy expiryPolicy, String source,
                         boolean getValue, boolean disableWriteThrough, int completionId) {
        expiryPolicy = getExpiryPolicy(expiryPolicy);
        long now = Clock.currentTimeMillis();
        long start = isStatisticsEnabled() ? System.nanoTime() : 0;
        boolean isOnNewPut = false;
        boolean isSaveSucceed;
        Object oldValue = null;
        R record = records.get(key);
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
            updateGetAndPutStat(isSaveSucceed, getValue, oldValue == null, start);
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

    protected Object put(Data key, Object value, ExpiryPolicy expiryPolicy, String source,
                         boolean getValue, int completionId) {
        return put(key, value, expiryPolicy, source, getValue, false, completionId);
    }

    @Override
    public R put(Data key, Object value, ExpiryPolicy expiryPolicy, String source, int completionId) {
        return (R) put(key, value, expiryPolicy, source, false, false, completionId);
    }

    @Override
    public Object getAndPut(Data key, Object value, ExpiryPolicy expiryPolicy, String source, int completionId) {
        return put(key, value, expiryPolicy, source, true, false, completionId);
    }

    protected void onPutIfAbsent(Data key, Object value, ExpiryPolicy expiryPolicy, String source,
                                 boolean disableWriteThrough, R record, boolean isExpired, boolean isSaveSucceed) {
    }

    protected void onPutIfAbsentError(Data key, Object value, ExpiryPolicy expiryPolicy,
                                      String source, boolean disableWriteThrough, R record, Throwable error) {
    }

    protected boolean putIfAbsent(Data key, Object value, ExpiryPolicy expiryPolicy, String source,
                                  boolean disableWriteThrough, int completionId) {
        expiryPolicy = getExpiryPolicy(expiryPolicy);
        long now = Clock.currentTimeMillis();
        long start = isStatisticsEnabled() ? System.nanoTime() : 0;
        boolean saved = false;
        R record = records.get(key);
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
                    statistics.addPutTimeNanos(System.nanoTime() - start);
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
                               String source, int completionId) {
        return putIfAbsent(key, value, expiryPolicy, source, false, completionId);
    }

    @SuppressWarnings("checkstyle:parameternumber")
    protected void onReplace(Data key, Object oldValue, Object newValue, ExpiryPolicy expiryPolicy,
                             String source, boolean getValue, R record, boolean isExpired, boolean replaced) {
    }

    @SuppressWarnings("checkstyle:parameternumber")
    protected void onReplaceError(Data key, Object oldValue, Object newValue, ExpiryPolicy expiryPolicy, String source,
                                  boolean getValue, R record, boolean isExpired, boolean replaced, Throwable error) {
    }

    @Override
    public boolean replace(Data key, Object value, ExpiryPolicy expiryPolicy, String source, int completionId) {
        expiryPolicy = getExpiryPolicy(expiryPolicy);
        long now = Clock.currentTimeMillis();
        long start = isStatisticsEnabled() ? System.nanoTime() : 0;
        boolean replaced = false;
        R record = records.get(key);
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
                    statistics.addPutTimeNanos(System.nanoTime() - start);
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
                           String source, int completionId) {
        expiryPolicy = getExpiryPolicy(expiryPolicy);
        long now = Clock.currentTimeMillis();
        long start = isStatisticsEnabled() ? System.nanoTime() : 0;
        boolean isHit = false;
        boolean replaced = false;
        R record = records.get(key);
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
            updateReplaceStat(replaced, isHit, start);
            return replaced;
        } catch (Throwable error) {
            onReplaceError(key, oldValue, newValue, expiryPolicy, source, false,
                    record, isExpired, replaced, error);
            throw ExceptionUtil.rethrow(error);
        }
    }

    @Override
    public Object getAndReplace(Data key, Object value, ExpiryPolicy expiryPolicy, String source, int completionId) {
        expiryPolicy = getExpiryPolicy(expiryPolicy);
        long now = Clock.currentTimeMillis();
        long start = isStatisticsEnabled() ? System.nanoTime() : 0;
        boolean replaced = false;
        R record = records.get(key);
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
                statistics.addGetTimeNanos(System.nanoTime() - start);
                if (obj != null) {
                    statistics.increaseCacheHits(1);
                    statistics.increaseCachePuts(1);
                    statistics.addPutTimeNanos(System.nanoTime() - start);
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

    protected void onRemove(Data key, Object value, String source, boolean getValue, R record, boolean removed) {
    }


    protected void onRemoveError(Data key, Object value, String source, boolean getValue,
                                 R record, boolean removed, Throwable error) {
    }

    @Override
    public boolean remove(Data key, String source, String origin, int completionId) {
        long now = Clock.currentTimeMillis();
        long start = isStatisticsEnabled() ? System.nanoTime() : 0;

        deleteCacheEntry(key);

        R record = records.get(key);
        boolean removed = false;
        try {
            if (recordNotExistOrExpired(record, now)) {
                if (isEventsEnabled()) {
                    publishEvent(createCacheCompleteEvent(toEventData(key), CacheRecord.TIME_NOT_AVAILABLE,
                            origin, completionId));
                }
            } else {
                removed = deleteRecord(key, completionId, source, origin);
            }
            onRemove(key, null, source, false, record, removed);
            if (removed && isStatisticsEnabled()) {
                statistics.increaseCacheRemovals(1);
                statistics.addRemoveTimeNanos(System.nanoTime() - start);
            }
            return removed;
        } catch (Throwable error) {
            onRemoveError(key, null, source, false, record, removed, error);
            throw ExceptionUtil.rethrow(error);
        }
    }

    @Override
    public boolean remove(Data key, Object value, String source, String origin, int completionId) {
        long now = Clock.currentTimeMillis();
        long start = System.nanoTime();
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
                    long expiryTime = onRecordAccess(key, record, defaultExpiryPolicy, now);
                    processExpiredEntry(key, record, expiryTime, now, source, origin);
                }
            }
            if (!removed) {
                if (isEventsEnabled()) {
                    publishEvent(createCacheCompleteEvent(toEventData(key), CacheRecord.TIME_NOT_AVAILABLE,
                            origin, completionId));
                }
            }
            onRemove(key, value, source, false, record, removed);
            updateRemoveStatistics(removed, hitCount, start);
            return removed;
        } catch (Throwable error) {
            onRemoveError(key, null, source, false, record, removed, error);
            throw ExceptionUtil.rethrow(error);
        }
    }

    private void updateRemoveStatistics(boolean result, int hitCount, long start) {
        if (result && isStatisticsEnabled()) {
            statistics.increaseCacheRemovals(1);
            statistics.addRemoveTimeNanos(System.nanoTime() - start);
            if (hitCount == 1) {
                statistics.increaseCacheHits(hitCount);
            } else {
                statistics.increaseCacheMisses(1);
            }
        }
    }

    @Override
    public Object getAndRemove(Data key, String source, int completionId) {
        return getAndRemove(key, source, completionId, null);
    }

    public Object getAndRemove(Data key, String source, int completionId, String origin) {
        long now = Clock.currentTimeMillis();
        long start = isStatisticsEnabled() ? System.nanoTime() : 0;

        deleteCacheEntry(key);

        R record = records.get(key);
        Object obj;
        boolean removed = false;
        try {
            if (recordNotExistOrExpired(record, now)) {
                obj = null;
                if (isEventsEnabled()) {
                    publishEvent(createCacheCompleteEvent(toEventData(key), CacheRecord.TIME_NOT_AVAILABLE,
                            origin, completionId));
                }
            } else {
                obj = toValue(record);
                removed = deleteRecord(key, completionId, source, origin);
            }
            onRemove(key, null, source, false, record, removed);
            if (isStatisticsEnabled()) {
                statistics.addGetTimeNanos(System.nanoTime() - start);
                if (obj != null) {
                    statistics.increaseCacheHits(1);
                    statistics.increaseCacheRemovals(1);
                    statistics.addRemoveTimeNanos(System.nanoTime() - start);
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
        expiryPolicy = getExpiryPolicy(expiryPolicy);
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
    public CacheRecord merge(CacheMergeTypes mergingEntry, SplitBrainMergePolicy<Data, CacheMergeTypes> mergePolicy) {
        final long now = Clock.currentTimeMillis();
        final long start = isStatisticsEnabled() ? System.nanoTime() : 0;

        injectDependencies(mergingEntry);
        injectDependencies(mergePolicy);

        boolean merged = false;
        Data key = mergingEntry.getKey();
        long expiryTime = mergingEntry.getExpirationTime();
        R record = records.get(key);
        boolean isExpired = processExpiredEntry(key, record, now);

        if (record == null || isExpired) {
            Data newValue = mergePolicy.merge(mergingEntry, null);
            if (newValue != null) {
                record = createRecordWithExpiry(key, newValue, expiryTime, now, true, IGNORE_COMPLETION);
                merged = record != null;
            }
        } else {
            SerializationService serializationService = nodeEngine.getSerializationService();
            Data oldValue = serializationService.toData(record.getValue());
            CacheMergeTypes existingEntry = createMergingEntry(serializationService, key, oldValue, record);
            Data newValue = mergePolicy.merge(mergingEntry, existingEntry);
            if (newValue != null && newValue != oldValue) {
                merged = updateRecordWithExpiry(key, newValue, record, expiryTime, now, true, IGNORE_COMPLETION);
            }
        }

        if (merged && isStatisticsEnabled()) {
            statistics.increaseCachePuts(1);
            statistics.addPutTimeNanos(System.nanoTime() - start);
        }

        return merged ? record : null;
    }

    @Override
    public CacheRecord merge(CacheEntryView<Data, Data> cacheEntryView, CacheMergePolicy mergePolicy,
                             String caller, String origin, int completionId) {
        final long now = Clock.currentTimeMillis();
        final long start = isStatisticsEnabled() ? System.nanoTime() : 0;

        boolean merged = false;
        Data key = cacheEntryView.getKey();
        Data value = cacheEntryView.getValue();
        long expiryTime = cacheEntryView.getExpirationTime();
        R record = records.get(key);
        boolean isExpired = processExpiredEntry(key, record, now);

        if (record == null || isExpired) {
            Object newValue = mergePolicy.merge(name, createCacheEntryView(
                    key,
                    value,
                    cacheEntryView.getCreationTime(),
                    cacheEntryView.getExpirationTime(),
                    cacheEntryView.getLastAccessTime(),
                    cacheEntryView.getAccessHit(),
                    mergePolicy),
                    null);
            if (newValue != null) {
                record = createRecordWithExpiry(key, newValue, expiryTime, now, true, IGNORE_COMPLETION);
                merged = record != null;
            }
        } else {
            Object existingValue = record.getValue();
            Object newValue = mergePolicy.merge(name,
                    createCacheEntryView(
                            key,
                            value,
                            cacheEntryView.getCreationTime(),
                            cacheEntryView.getExpirationTime(),
                            cacheEntryView.getLastAccessTime(),
                            cacheEntryView.getAccessHit(),
                            mergePolicy),
                    createCacheEntryView(
                            key,
                            existingValue,
                            cacheEntryView.getCreationTime(),
                            record.getExpirationTime(),
                            record.getLastAccessTime(),
                            record.getAccessHit(),
                            mergePolicy));
            if (existingValue != newValue) {
                merged = updateRecordWithExpiry(key, newValue, record, expiryTime, now, true, IGNORE_COMPLETION);
            }
        }

        if (merged && isStatisticsEnabled()) {
            statistics.increaseCachePuts(1);
            statistics.addPutTimeNanos(System.nanoTime() - start);
        }

        return merged ? record : null;
    }

    private CacheEntryView createCacheEntryView(Object key, Object value, long creationTime, long expirationTime,
                                                long lastAccessTime, long accessHit, CacheMergePolicy mergePolicy) {
        // null serialization service means that use as storage type without conversion,
        // non-null serialization service means that conversion is required
        SerializationService ss
                = mergePolicy instanceof StorageTypeAwareCacheMergePolicy ? null : nodeEngine.getSerializationService();
        return new LazyCacheEntryView(key, value, creationTime, expirationTime, lastAccessTime, accessHit, ss);
    }

    @Override
    public CacheKeyIterationResult fetchKeys(int tableIndex, int size) {
        return records.fetchKeys(tableIndex, size);
    }

    @Override
    public CacheEntryIterationResult fetchEntries(int tableIndex, int size) {
        return records.fetchEntries(tableIndex, size);
    }

    @Override
    public Object invoke(Data key, EntryProcessor entryProcessor, Object[] arguments, int completionId) {
        long now = Clock.currentTimeMillis();
        long start = isStatisticsEnabled() ? System.nanoTime() : 0;
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
            statistics.addGetTimeNanos(System.nanoTime() - start);
        }
        CacheEntryProcessorEntry entry = createCacheEntryProcessorEntry(key, record, now, completionId);
        injectDependencies(entryProcessor);
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
        cacheService.eventJournal.destroy(objectNamespace, partitionId);
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
        InternalEventService eventService = (InternalEventService) cacheService.getNodeEngine().getEventService();
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
