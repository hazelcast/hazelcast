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

package com.hazelcast.internal.nearcache.impl.store;

import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.internal.eviction.EvictionChecker;
import com.hazelcast.internal.eviction.EvictionListener;
import com.hazelcast.internal.eviction.impl.evaluator.EvictionPolicyEvaluator;
import com.hazelcast.internal.eviction.impl.strategy.sampling.SamplingEvictionStrategy;
import com.hazelcast.internal.monitor.impl.NearCacheStatsImpl;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.internal.nearcache.NearCacheRecord;
import com.hazelcast.internal.nearcache.NearCacheRecordStore;
import com.hazelcast.internal.nearcache.impl.SampleableNearCacheRecordMap;
import com.hazelcast.internal.nearcache.impl.invalidation.MetaDataContainer;
import com.hazelcast.internal.nearcache.impl.invalidation.StaleReadDetector;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.nearcache.NearCacheStats;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static com.hazelcast.internal.eviction.EvictionPolicyEvaluatorProvider.getEvictionPolicyEvaluator;
import static com.hazelcast.internal.nearcache.NearCache.CACHED_AS_NULL;
import static com.hazelcast.internal.nearcache.NearCache.UpdateSemantic.READ_UPDATE;
import static com.hazelcast.internal.nearcache.NearCache.UpdateSemantic.WRITE_UPDATE;
import static com.hazelcast.internal.nearcache.NearCacheRecord.NOT_RESERVED;
import static com.hazelcast.internal.nearcache.NearCacheRecord.READ_PERMITTED;
import static com.hazelcast.internal.nearcache.impl.invalidation.StaleReadDetector.ALWAYS_FRESH;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;

/**
 * Abstract implementation of {@link NearCacheRecordStore} and {@link EvictionListener}.
 *
 * @param <K>    the type of the key stored in Near Cache
 * @param <V>    the type of the value stored in Near Cache
 * @param <KS>   the type of the key of the underlying {@link com.hazelcast.internal.nearcache.impl.NearCacheRecordMap}
 * @param <R>    the type of the value of the underlying {@link com.hazelcast.internal.nearcache.impl.NearCacheRecordMap}
 * @param <NCRM> the type of the underlying {@link com.hazelcast.internal.nearcache.impl.NearCacheRecordMap}
 */
@SuppressWarnings("checkstyle:methodcount")
public abstract class AbstractNearCacheRecordStore<K, V, KS, R extends NearCacheRecord,
        NCRM extends SampleableNearCacheRecordMap<KS, R>>
        implements NearCacheRecordStore<K, V>, EvictionListener<KS, R> {

    private static final long MILLI_SECONDS_IN_A_SECOND = 1000;
    private static final AtomicLongFieldUpdater<AbstractNearCacheRecordStore> RESERVATION_ID
            = newUpdater(AbstractNearCacheRecordStore.class, "reservationId");

    protected final long timeToLiveMillis;
    protected final long maxIdleMillis;
    protected final boolean evictionDisabled;
    protected final ClassLoader classLoader;
    protected final InMemoryFormat inMemoryFormat;
    protected final NearCacheConfig nearCacheConfig;
    protected final NearCacheStatsImpl nearCacheStats;
    protected final SerializationService serializationService;

    protected NCRM records;
    protected EvictionChecker evictionChecker;
    protected SamplingEvictionStrategy<KS, R, NCRM> evictionStrategy;
    protected EvictionPolicyEvaluator<KS, R> evictionPolicyEvaluator;

    protected volatile long reservationId;
    protected volatile StaleReadDetector staleReadDetector = ALWAYS_FRESH;

    public AbstractNearCacheRecordStore(NearCacheConfig nearCacheConfig,
                                        SerializationService serializationService,
                                        ClassLoader classLoader) {
        this(nearCacheConfig, new NearCacheStatsImpl(), serializationService, classLoader);
    }

    // extended in EE
    protected AbstractNearCacheRecordStore(NearCacheConfig nearCacheConfig, NearCacheStatsImpl nearCacheStats,
                                           SerializationService serializationService, ClassLoader classLoader) {
        this.nearCacheConfig = nearCacheConfig;
        this.inMemoryFormat = nearCacheConfig.getInMemoryFormat();
        this.timeToLiveMillis = nearCacheConfig.getTimeToLiveSeconds() * MILLI_SECONDS_IN_A_SECOND;
        this.maxIdleMillis = nearCacheConfig.getMaxIdleSeconds() * MILLI_SECONDS_IN_A_SECOND;
        this.serializationService = serializationService;
        this.classLoader = classLoader;
        this.nearCacheStats = nearCacheStats;
        this.evictionDisabled = nearCacheConfig.getEvictionConfig().getEvictionPolicy() == EvictionPolicy.NONE;
    }

    @Override
    public void initialize() {
        this.records = createNearCacheRecordMap(nearCacheConfig);
        EvictionConfig evictionConfig = nearCacheConfig.getEvictionConfig();
        this.evictionChecker = createNearCacheEvictionChecker(evictionConfig, nearCacheConfig);
        if (!evictionDisabled) {
            this.evictionStrategy = SamplingEvictionStrategy.INSTANCE;
            this.evictionPolicyEvaluator = getEvictionPolicyEvaluator(evictionConfig, classLoader);
        }
    }

    @Override
    public void setStaleReadDetector(StaleReadDetector staleReadDetector) {
        this.staleReadDetector = staleReadDetector;
    }

    @Override
    public abstract R getRecord(K key);

    protected abstract EvictionChecker createNearCacheEvictionChecker(EvictionConfig evictionConfig,
                                                                      NearCacheConfig nearCacheConfig);

    protected abstract NCRM createNearCacheRecordMap(NearCacheConfig nearCacheConfig);

    protected abstract long getKeyStorageMemoryCost(K key);

    protected abstract long getRecordStorageMemoryCost(R record);

    protected abstract R createRecord(V value);

    protected abstract void updateRecordValue(R record, V value);

    protected abstract R reserveForReadUpdate(K key, Data keyData, long reservationId);

    protected abstract R reserveForWriteUpdate(K key, Data keyData, long reservationId);

    protected abstract R putRecord(K key, R record);

    protected abstract boolean containsRecordKey(K key);

    protected void checkAvailable() {
        if (!isAvailable()) {
            throw new IllegalStateException(nearCacheConfig.getName() + " named Near Cache record store is not available");
        }
    }

    private boolean isAvailable() {
        return records != null;
    }

    protected Data toData(Object obj) {
        return serializationService.toData(obj);
    }

    protected V toValue(Object obj) {
        return serializationService.toObject(obj);
    }

    protected long getTotalStorageMemoryCost(K key, R record) {
        return getKeyStorageMemoryCost(key) + getRecordStorageMemoryCost(record);
    }

    protected boolean isRecordExpired(R record) {
        if (!canUpdateStats(record)) {
            // A record can only be checked for expiry if its record state is
            // READ_PERMITTED. We can't check reserved records for expiry.
            return false;
        }

        long now = Clock.currentTimeMillis();
        if (record.isExpiredAt(now)) {
            return true;
        } else {
            return record.isIdleAt(maxIdleMillis, now);
        }
    }

    @SuppressWarnings("unused")
    protected void onGet(K key, V value, R record) {
    }

    @SuppressWarnings("unused")
    protected void onGetError(K key, V value, R record, Throwable error) {
    }

    @SuppressWarnings("unused")
    protected void onPut(K key, V value, R record, R oldRecord) {
    }

    @SuppressWarnings("unused")
    protected void onPutError(K key, V value, R record, R oldRecord, Throwable error) {
    }

    @SuppressWarnings("unused")
    protected void onRemove(K key, R record, boolean removed) {
    }

    @SuppressWarnings("unused")
    protected void onRemoveError(K key, R record, boolean removed, Throwable error) {
    }

    @SuppressWarnings("unused")
    protected void onExpire(K key, R record) {
        nearCacheStats.incrementExpirations();
    }

    @Override
    public void onEvict(KS key, R record, boolean wasExpired) {
        if (wasExpired) {
            nearCacheStats.incrementExpirations();
        } else {
            nearCacheStats.incrementEvictions();
        }
        nearCacheStats.decrementOwnedEntryCount();
    }

    @Override
    public V get(K key) {
        checkAvailable();

        R record = null;
        V value = null;
        try {
            record = getRecord(key);

            if (record == null) {
                nearCacheStats.incrementMisses();
                return null;
            }

            value = (V) record.getValue();
            long recordState = record.getReservationId();

            if (recordState != READ_PERMITTED
                    && !record.isCachedAsNull()
                    && value == null) {
                nearCacheStats.incrementMisses();
                return null;
            }

            if (staleReadDetector.isStaleRead(key, record)) {
                invalidate(key);
                nearCacheStats.incrementMisses();
                return null;
            }

            if (isRecordExpired(record)) {
                invalidate(key);
                onExpire(key, record);
                return null;
            }

            // TODO what does onGet do?
            onGet(key, value, record);
            onRecordAccess(record);
            nearCacheStats.incrementHits();

            return recordToValue(record);
        } catch (Throwable error) {
            onGetError(key, value, record, error);
            throw rethrow(error);
        }
    }

    protected V recordToValue(R record) {
        return record.getValue() == null
                ? (V) CACHED_AS_NULL : toValue(record.getValue());
    }

    // only implemented for testing purposes
    @Override
    public void put(K key, Data keyData, V value, Data valueData) {
        long reservationId = tryReserveForUpdate(key, keyData, READ_UPDATE);
        if (reservationId != NOT_RESERVED) {
            tryPublishReserved(key, value, reservationId, false);
        }
    }

    // only used for testing purposes
    public StaleReadDetector getStaleReadDetector() {
        return staleReadDetector;
    }

    protected boolean canUpdateStats(R record) {
        return record != null && record.getReservationId() == READ_PERMITTED;
    }

    @Override
    public void clear() {
        checkAvailable();

        int size = records.size();
        records.clear();
        nearCacheStats.setOwnedEntryCount(0);
        nearCacheStats.setOwnedEntryMemoryCost(0L);
        nearCacheStats.incrementInvalidations(size);
        nearCacheStats.incrementInvalidationRequests();
    }

    @Override
    public void destroy() {
        clear();
    }

    @Override
    public NearCacheStats getNearCacheStats() {
        checkAvailable();

        return nearCacheStats;
    }

    @Override
    public int size() {
        checkAvailable();

        return records.size();
    }

    @Override
    public boolean doEviction(boolean withoutMaxSizeCheck) {
        checkAvailable();

        if (evictionDisabled) {
            return false;
        }

        EvictionChecker evictionChecker = withoutMaxSizeCheck ? null : this.evictionChecker;
        evictionStrategy.evict(records, evictionPolicyEvaluator, evictionChecker, this);
        return true;
    }

    @Override
    public long tryReserveForUpdate(K key, Data keyData, NearCache.UpdateSemantic updateSemantic) {
        checkAvailable();
        // if there is no eviction configured we return if the Near Cache is full and it's a new key
        // (we have to check the key, otherwise we might lose updates on existing keys)
        if (evictionDisabled && evictionChecker.isEvictionRequired() && !containsRecordKey(key)) {
            return NOT_RESERVED;
        }

        long reservationId = nextReservationId();

        R reservedRecord = updateSemantic == WRITE_UPDATE
                ? reserveForWriteUpdate(key, keyData, reservationId)
                : reserveForReadUpdate(key, keyData, reservationId);

        if (reservedRecord == null || reservedRecord.getReservationId() != reservationId) {
            return NOT_RESERVED;
        }

        return reservationId;
    }

    protected R publishReservedRecord(K key, V value, R reservedRecord, long reservationId) {
        if (reservedRecord.getReservationId() != reservationId) {
            return reservedRecord;
        }

        boolean update = reservedRecord.getValue() != null || reservedRecord.isCachedAsNull();
        if (update) {
            nearCacheStats.incrementOwnedEntryMemoryCost(-getTotalStorageMemoryCost(key, reservedRecord));
        }

        updateRecordValue(reservedRecord, value);
        if (value == null) {
            // TODO Add ICache/IMap config to allow cache as null
            reservedRecord.setCachedAsNull(true);
        }
        reservedRecord.setReservationId(READ_PERMITTED);

        nearCacheStats.incrementOwnedEntryMemoryCost(getTotalStorageMemoryCost(key, reservedRecord));
        if (!update) {
            nearCacheStats.incrementOwnedEntryCount();
        }

        return reservedRecord;
    }

    private void onRecordAccess(R record) {
        record.setLastAccessTime(Clock.currentTimeMillis());
        record.incrementHits();
    }

    protected void initInvalidationMetaData(R record, K key, Data keyData) {
        if (staleReadDetector == ALWAYS_FRESH) {
            // means invalidation event creation is disabled for this Near Cache
            return;
        }

        int partitionId = staleReadDetector.getPartitionId(keyData == null ? toData(key) : keyData);
        MetaDataContainer metaDataContainer = staleReadDetector.getMetaDataContainer(partitionId);
        record.setPartitionId(partitionId);
        record.setInvalidationSequence(metaDataContainer.getSequence());
        record.setUuid(metaDataContainer.getUuid());
    }

    private long nextReservationId() {
        return RESERVATION_ID.incrementAndGet(this);
    }

    protected R newReservationRecord(K key, Data keyData, long reservationId) {
        R record = null;
        try {
            record = createRecord(null);
            record.setReservationId(reservationId);
            initInvalidationMetaData(record, key, keyData);
        } catch (Throwable throwable) {
            onPutError(key, null, record, null, throwable);
            throw rethrow(throwable);
        }
        return record;
    }

    // overridden in EE
    protected R reserveForWriteUpdate(K key, Data keyData, R existingRecord, long reservationId) {
        // 1. When no existingRecord, create a new reservation record.
        if (existingRecord == null) {
            return newReservationRecord(key, keyData, reservationId);
        }

        // 2. When there is a readable existingRecord, change its reservation id
        if (existingRecord.getReservationId() == READ_PERMITTED) {
            existingRecord.setReservationId(reservationId);
            return existingRecord;
        }

        // 3. If this record is a previously reserved one, delete it.
        // Reasoning: CACHE_ON_UPDATE mode has different characteristics
        // than INVALIDATE mode when updating local near-cache. During
        // update, if CACHE_ON_UPDATE finds a previously reserved
        // record, that record is deleted. This is different from
        // INVALIDATE mode which doesn't delete previously reserved
        // record and keeps it as is. The reason for this deletion
        // is: concurrent reservation attempts. If CACHE_ON_UPDATE
        // doesn't delete previously reserved record, indefinite
        // read of stale value situation can be seen. Since we
        // don't apply invalidations which are sent from server
        // to near-cache if the source UUID of the invalidation
        // is same with the end's UUID which has near-cache on
        // it (client or server UUID which has near cache on it).
        return null;
    }
}
