/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.nearcache.NearCacheRecord;
import com.hazelcast.internal.nearcache.NearCacheRecordStore;
import com.hazelcast.internal.nearcache.impl.SampleableNearCacheRecordMap;
import com.hazelcast.internal.nearcache.impl.invalidation.MetaDataContainer;
import com.hazelcast.internal.nearcache.impl.invalidation.StaleReadDetector;
import com.hazelcast.nearcache.NearCacheStats;
import com.hazelcast.internal.monitor.impl.NearCacheStatsImpl;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.Clock;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.Function;

import static com.hazelcast.internal.eviction.EvictionPolicyEvaluatorProvider.getEvictionPolicyEvaluator;
import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.MEM;
import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.MEM_AVAILABLE;
import static com.hazelcast.internal.nearcache.NearCache.CACHED_AS_NULL;
import static com.hazelcast.internal.nearcache.NearCacheRecord.NOT_RESERVED;
import static com.hazelcast.internal.nearcache.NearCacheRecord.READ_PERMITTED;
import static com.hazelcast.internal.nearcache.NearCacheRecord.RESERVED;
import static com.hazelcast.internal.nearcache.NearCacheRecord.UPDATE_STARTED;
import static com.hazelcast.internal.nearcache.impl.invalidation.StaleReadDetector.ALWAYS_FRESH;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static java.lang.String.format;
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

    protected static final AtomicLongFieldUpdater<AbstractNearCacheRecordStore> RESERVATION_ID
            = newUpdater(AbstractNearCacheRecordStore.class, "reservationId");

    /**
     * If Unsafe is available, Object array index scale (every index represents a reference)
     * can be assumed as reference size.
     * <p>
     * Otherwise, we assume reference size as integer size that means
     * we assume 32 bit JVM or compressed-references enabled 64 bit JVM
     * by ignoring compressed-references disable mode on 64 bit JVM.
     */
    protected static final long REFERENCE_SIZE = MEM_AVAILABLE
            ? MEM.arrayIndexScale(Object[].class) : (Integer.SIZE / Byte.SIZE);
    protected static final long MILLI_SECONDS_IN_A_SECOND = 1000;

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

    protected abstract R getOrCreateToReserve(K key, Data keyData);

    protected abstract V updateAndGetReserved(K key, V value, long reservationId, boolean deserialize);

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
        long now = Clock.currentTimeMillis();
        if (record.isExpiredAt(now)) {
            return true;
        } else {
            return record.isIdleAt(maxIdleMillis, now);
        }
    }

    protected V recordToValue(R record) {
        if (record.getValue() == null) {
            return (V) CACHED_AS_NULL;
        }
        return toValue(record.getValue());
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
            if (record != null) {
                if (record.getRecordState() != READ_PERMITTED) {
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

                onRecordAccess(record);
                nearCacheStats.incrementHits();
                value = recordToValue(record);
                onGet(key, value, record);
                return value;
            } else {
                nearCacheStats.incrementMisses();
                return null;
            }
        } catch (Throwable error) {
            onGetError(key, value, record, error);
            throw rethrow(error);
        }
    }

    @Override
    public void put(K key, Data keyData, V value, Data valueData) {
        checkAvailable();
        // if there is no eviction configured we return if the Near Cache is full and it's a new key
        // (we have to check the key, otherwise we might lose updates on existing keys)
        if (evictionDisabled && evictionChecker.isEvictionRequired() && !containsRecordKey(key)) {
            return;
        }

        R record = null;
        R oldRecord = null;
        try {
            record = createRecord((V) selectInMemoryFormatFriendlyValue(inMemoryFormat, value, valueData));
            onRecordCreate(key, keyData, record);
            oldRecord = putRecord(key, record);
            if (oldRecord == null) {
                nearCacheStats.incrementOwnedEntryCount();
            }
            onPut(key, value, record, oldRecord);
        } catch (Throwable error) {
            onPutError(key, value, record, oldRecord, error);
            throw rethrow(error);
        }
    }

    private static Object selectInMemoryFormatFriendlyValue(InMemoryFormat inMemoryFormat,
                                                            Object value1, Object value2) {
        switch (inMemoryFormat) {
            case OBJECT:
                return prioritizeObjectValue(value1, value2);
            case BINARY:
            case NATIVE:
                return prioritizeDataValue(value1, value2);
            default:
                throw new IllegalArgumentException(format("Unrecognized in memory format "
                        + "was found: '%s'", inMemoryFormat));
        }
    }

    private static Object prioritizeObjectValue(Object value1, Object value2) {
        boolean value1NotNull = value1 != null;
        if (value1NotNull && !(value1 instanceof Data)) {
            return value1;
        }

        boolean value2NotNull = value2 != null;
        if (value2NotNull && !(value2 instanceof Data)) {
            return value2;
        }

        if (value1NotNull) {
            return value1;
        }

        if (value2NotNull) {
            return value2;
        }

        return null;
    }

    private static Object prioritizeDataValue(Object value1, Object value2) {
        if (value1 instanceof Data) {
            return value1;
        }

        if (value2 instanceof Data) {
            return value2;
        }

        if (value1 != null) {
            return value1;
        }

        if (value2 != null) {
            return value2;
        }

        return null;
    }


    protected boolean canUpdateStats(R record) {
        return record != null && record.getRecordState() == READ_PERMITTED;
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
    public void doEviction(boolean withoutMaxSizeCheck) {
        checkAvailable();

        if (!evictionDisabled) {
            EvictionChecker evictionChecker = withoutMaxSizeCheck ? null : this.evictionChecker;
            evictionStrategy.evict(records, evictionPolicyEvaluator, evictionChecker, this);
        }
    }

    @Override
    public long tryReserveForUpdate(K key, Data keyData) {
        checkAvailable();
        // if there is no eviction configured we return if the Near Cache is full and it's a new key
        // (we have to check the key, otherwise we might lose updates on existing keys)
        if (evictionDisabled && evictionChecker.isEvictionRequired() && !containsRecordKey(key)) {
            return NOT_RESERVED;
        }

        R reservedRecord = getOrCreateToReserve(key, keyData);
        long reservationId = nextReservationId();
        if (reservedRecord.casRecordState(RESERVED, reservationId)) {
            return reservationId;
        } else {
            return NOT_RESERVED;
        }
    }

    @Override
    public V tryPublishReserved(K key, V value, long reservationId, boolean deserialize) {
        checkAvailable();
        return updateAndGetReserved(key, value, reservationId, deserialize);
    }

    // only used for testing purposes
    public StaleReadDetector getStaleReadDetector() {
        return staleReadDetector;
    }

    protected void onRecordCreate(K key, Data keyData, R record) {
        record.setCreationTime(Clock.currentTimeMillis());
        initInvalidationMetaData(record, key, keyData);
    }

    protected R updateReservedRecordInternal(K key, V value, R reservedRecord, long reservationId) {
        if (!reservedRecord.casRecordState(reservationId, UPDATE_STARTED)) {
            return reservedRecord;
        }

        updateRecordValue(reservedRecord, value);
        reservedRecord.casRecordState(UPDATE_STARTED, READ_PERMITTED);

        nearCacheStats.incrementOwnedEntryMemoryCost(getTotalStorageMemoryCost(key, reservedRecord));
        nearCacheStats.incrementOwnedEntryCount();
        return reservedRecord;
    }

    private void onRecordAccess(R record) {
        record.setAccessTime(Clock.currentTimeMillis());
        record.incrementHits();
    }

    private void initInvalidationMetaData(R record, K key, Data keyData) {
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

    @SuppressWarnings("WeakerAccess")
    protected class ReserveForUpdateFunction implements Function<K, R> {

        private final Data keyData;

        public ReserveForUpdateFunction(Data keyData) {
            this.keyData = keyData;
        }

        @Override
        public R apply(K key) {
            R record = null;
            try {
                record = createRecord(null);
                onRecordCreate(key, keyData, record);
                record.casRecordState(READ_PERMITTED, RESERVED);
            } catch (Throwable throwable) {
                onPutError(key, null, record, null, throwable);
                throw rethrow(throwable);
            }
            return record;
        }
    }
}
