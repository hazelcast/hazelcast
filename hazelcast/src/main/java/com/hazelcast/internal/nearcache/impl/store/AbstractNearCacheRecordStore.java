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

package com.hazelcast.internal.nearcache.impl.store;

import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.IFunction;
import com.hazelcast.internal.eviction.EvictionChecker;
import com.hazelcast.internal.eviction.EvictionListener;
import com.hazelcast.internal.eviction.EvictionPolicyEvaluator;
import com.hazelcast.internal.eviction.EvictionPolicyType;
import com.hazelcast.internal.eviction.EvictionStrategy;
import com.hazelcast.internal.eviction.MaxSizeChecker;
import com.hazelcast.internal.nearcache.NearCacheRecord;
import com.hazelcast.internal.nearcache.NearCacheRecordStore;
import com.hazelcast.internal.nearcache.impl.NearCacheRecordMap;
import com.hazelcast.internal.nearcache.impl.invalidation.MetaDataContainer;
import com.hazelcast.internal.nearcache.impl.invalidation.StaleReadDetector;
import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.monitor.impl.NearCacheStatsImpl;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.Clock;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static com.hazelcast.internal.eviction.EvictionPolicyEvaluatorProvider.getEvictionPolicyEvaluator;
import static com.hazelcast.internal.eviction.EvictionStrategyProvider.getEvictionStrategy;
import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.MEM;
import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.MEM_AVAILABLE;
import static com.hazelcast.internal.nearcache.NearCacheRecord.NOT_RESERVED;
import static com.hazelcast.internal.nearcache.NearCacheRecord.READ_PERMITTED;
import static com.hazelcast.internal.nearcache.NearCacheRecord.RESERVED;
import static com.hazelcast.internal.nearcache.NearCacheRecord.UPDATE_STARTED;
import static com.hazelcast.internal.nearcache.impl.invalidation.StaleReadDetector.ALWAYS_FRESH;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;

@SuppressWarnings("checkstyle:methodcount")
public abstract class AbstractNearCacheRecordStore<K, V, KS, R extends NearCacheRecord, NCRM extends NearCacheRecordMap<KS, R>>
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
    protected static final int REFERENCE_SIZE = MEM_AVAILABLE ? MEM.arrayIndexScale(Object[].class) : (Integer.SIZE / Byte.SIZE);
    protected static final int MILLI_SECONDS_IN_A_SECOND = 1000;

    protected final long timeToLiveMillis;
    protected final long maxIdleMillis;
    protected final NearCacheConfig nearCacheConfig;
    protected final SerializationService serializationService;
    protected final ClassLoader classLoader;
    protected final NearCacheStatsImpl nearCacheStats;
    protected final IFunction<K, R> reserveForUpdate = new IFunction<K, R>() {
        @Override
        public R apply(K key) {
            R record = null;
            try {
                record = valueToRecord(null);
                onRecordCreate(key, record);
                record.casRecordState(READ_PERMITTED, RESERVED);
            } catch (Throwable throwable) {
                onPutError(key, null, record, null, throwable);
                throw rethrow(throwable);
            }
            return record;
        }
    };
    protected MaxSizeChecker maxSizeChecker;

    protected EvictionPolicyEvaluator<KS, R> evictionPolicyEvaluator;
    protected EvictionChecker evictionChecker;
    protected EvictionStrategy<KS, R, NCRM> evictionStrategy;
    protected EvictionPolicyType evictionPolicyType;
    protected NCRM records;

    protected volatile StaleReadDetector staleReadDetector = ALWAYS_FRESH;
    protected volatile long reservationId;

    public AbstractNearCacheRecordStore(NearCacheConfig nearCacheConfig, SerializationService serializationService,
                                        ClassLoader classLoader) {
        this(nearCacheConfig, new NearCacheStatsImpl(), serializationService, classLoader);
    }

    // extended in ee.
    protected AbstractNearCacheRecordStore(NearCacheConfig nearCacheConfig, NearCacheStatsImpl nearCacheStats,
                                           SerializationService serializationService, ClassLoader classLoader) {
        this.nearCacheConfig = nearCacheConfig;
        this.timeToLiveMillis = nearCacheConfig.getTimeToLiveSeconds() * MILLI_SECONDS_IN_A_SECOND;
        this.maxIdleMillis = nearCacheConfig.getMaxIdleSeconds() * MILLI_SECONDS_IN_A_SECOND;
        this.serializationService = serializationService;
        this.classLoader = classLoader;
        this.nearCacheStats = nearCacheStats;
        this.evictionPolicyType = nearCacheConfig.getEvictionConfig().getEvictionPolicyType();
    }

    @Override
    public void initialize() {
        EvictionConfig evictionConfig = nearCacheConfig.getEvictionConfig();
        this.records = createNearCacheRecordMap(nearCacheConfig);
        this.maxSizeChecker = createNearCacheMaxSizeChecker(evictionConfig, nearCacheConfig);
        this.evictionPolicyEvaluator = createEvictionPolicyEvaluator(evictionConfig);
        this.evictionChecker = createEvictionChecker(nearCacheConfig);
        this.evictionStrategy = createEvictionStrategy(evictionConfig);
        this.evictionPolicyType = evictionConfig.getEvictionPolicyType();
    }

    @Override
    public void setStaleReadDetector(StaleReadDetector staleReadDetector) {
        this.staleReadDetector = staleReadDetector;
    }

    @Override
    public StaleReadDetector getStaleReadDetector() {
        return staleReadDetector;
    }

    protected abstract MaxSizeChecker createNearCacheMaxSizeChecker(EvictionConfig evictionConfig,
                                                                    NearCacheConfig nearCacheConfig);

    protected abstract NCRM createNearCacheRecordMap(NearCacheConfig nearCacheConfig);

    protected abstract long getKeyStorageMemoryCost(K key);

    protected abstract long getRecordStorageMemoryCost(R record);

    protected abstract R valueToRecord(V value);

    protected abstract void updateRecordValue(R record, V value);

    protected abstract V recordToValue(R record);

    // public for tests.
    public abstract R getRecord(K key);

    protected abstract R getOrCreateToReserve(K key);

    protected abstract V updateAndGetReserved(K key, final V value, final long reservationId, boolean deserialize);

    protected abstract R putRecord(K key, R record);

    protected abstract R removeRecord(K key);

    protected abstract boolean containsRecordKey(K key);

    protected void checkAvailable() {
        if (!isAvailable()) {
            throw new IllegalStateException(nearCacheConfig.getName() + " named Near Cache record store is not available");
        }
    }

    protected EvictionPolicyEvaluator<KS, R> createEvictionPolicyEvaluator(EvictionConfig evictionConfig) {
        final EvictionPolicyType evictionPolicyType = evictionConfig.getEvictionPolicyType();
        if (evictionPolicyType == null) {
            throw new IllegalArgumentException("Eviction policy cannot be null");
        }
        return getEvictionPolicyEvaluator(evictionConfig, classLoader);
    }

    protected EvictionStrategy<KS, R, NCRM> createEvictionStrategy(EvictionConfig evictionConfig) {
        return getEvictionStrategy(evictionConfig);
    }

    protected EvictionChecker createEvictionChecker(NearCacheConfig nearCacheConfig) {
        return new MaxSizeEvictionChecker();
    }

    protected boolean isAvailable() {
        return records != null;
    }

    protected Data valueToData(V value) {
        if (value instanceof Data) {
            return (Data) value;
        } else if (value != null) {
            return serializationService.toData(value);
        } else {
            return null;
        }
    }

    protected V dataToValue(Data data) {
        if (data != null) {
            return serializationService.toObject(data);
        } else {
            return null;
        }
    }

    protected Data toData(Object obj) {
        if (obj == null) {
            return null;
        } else if (obj instanceof Data) {
            return (Data) obj;
        } else {
            return valueToData((V) obj);
        }
    }

    protected V toValue(Object obj) {
        if (obj == null) {
            return null;
        } else if (obj instanceof Data) {
            return dataToValue((Data) obj);
        } else {
            return (V) obj;
        }
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

    protected void onRecordCreate(K key, R record) {
        record.setCreationTime(Clock.currentTimeMillis());
        MetaDataContainer metaDataContainer = staleReadDetector.getMetaDataContainer(key);
        if (metaDataContainer != null) {
            record.setUuid(metaDataContainer.getUuid());
            record.setInvalidationSequence(metaDataContainer.getSequence());
        }
    }

    protected void onRecordAccess(R record) {
        record.setAccessTime(Clock.currentTimeMillis());
        record.incrementAccessHit();
    }

    protected void onGet(K key, V value, R record) {
    }

    protected void onGetError(K key, V value, R record, Throwable error) {
    }

    protected void onPut(K key, V value, R record, R oldRecord) {
    }

    protected void onPutError(K key, V value, R record, R oldRecord, Throwable error) {
    }

    protected void onRemove(K key, R record, boolean removed) {
    }

    protected void onRemoveError(K key, R record, boolean removed, Throwable error) {
    }

    protected void onExpire(K key, R record) {
        nearCacheStats.incrementExpirations();
    }

    protected boolean isEvictionEnabled() {
        return evictionStrategy != null
                && evictionPolicyEvaluator != null
                && evictionPolicyType != null
                && !evictionPolicyType.equals(EvictionPolicyType.NONE);
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
                    remove(key);
                    return null;
                }

                if (isRecordExpired(record)) {
                    remove(key);
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
    public void put(K key, V value) {
        checkAvailable();
        // if there is no eviction configured we return if the Near Cache is full and it's a new key
        // (we have to check the key, otherwise we might lose updates on existing keys)
        if (!isEvictionEnabled() && evictionChecker.isEvictionRequired() && !containsRecordKey(key)) {
            return;
        }

        R record = null;
        R oldRecord = null;
        try {
            record = valueToRecord(value);
            onRecordCreate(key, record);
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

    @Override
    public boolean remove(K key) {
        checkAvailable();

        R record = null;
        boolean removed = false;
        try {
            record = removeRecord(key);
            if (record != null) {
                removed = true;
                nearCacheStats.decrementOwnedEntryCount();
            }
            onRemove(key, record, removed);
            return record != null;
        } catch (Throwable error) {
            onRemoveError(key, record, removed, error);
            throw rethrow(error);
        }
    }

    protected void clearRecords() {
        records.clear();
    }

    @Override
    public void clear() {
        checkAvailable();

        clearRecords();
        nearCacheStats.setOwnedEntryCount(0);
        nearCacheStats.setOwnedEntryMemoryCost(0L);
    }

    protected void destroyStore() {
        clearRecords();
    }

    @Override
    public void destroy() {
        checkAvailable();

        destroyStore();
        nearCacheStats.setOwnedEntryCount(0);
        nearCacheStats.setOwnedEntryMemoryCost(0L);
    }

    @Override
    public int size() {
        checkAvailable();

        return records.size();
    }

    @Override
    public NearCacheStats getNearCacheStats() {
        checkAvailable();

        return nearCacheStats;
    }

    @Override
    public void doEvictionIfRequired() {
        checkAvailable();

        if (isEvictionEnabled()) {
            evictionStrategy.evict(records, evictionPolicyEvaluator, evictionChecker, this);
        }
    }

    @Override
    public void doEviction() {
        checkAvailable();

        if (isEvictionEnabled()) {
            evictionStrategy.evict(records, evictionPolicyEvaluator, null, this);
        }
    }

    @Override
    public long tryReserveForUpdate(K key) {
        checkAvailable();
        // if there is no eviction configured we return if the Near Cache is full and it's a new key
        // (we have to check the key, otherwise we might lose updates on existing keys)
        if (!isEvictionEnabled() && evictionChecker.isEvictionRequired() && !containsRecordKey(key)) {
            return NOT_RESERVED;
        }

        R reservedRecord = getOrCreateToReserve(key);
        long reservationId = nextReservationId();
        if (reservedRecord.casRecordState(RESERVED, reservationId)) {
            nearCacheStats.incrementOwnedEntryMemoryCost(getTotalStorageMemoryCost(key, reservedRecord));
            nearCacheStats.incrementOwnedEntryCount();
            return reservationId;
        } else {
            return NOT_RESERVED;
        }
    }

    @Override
    public V tryPublishReserved(K key, final V value, final long reservationId, boolean deserialize) {
        return updateAndGetReserved(key, value, reservationId, deserialize);
    }

    protected R updateReservedRecordInternal(K key, V value, R reservedRecord, long reservationId) {
        if (!reservedRecord.casRecordState(reservationId, UPDATE_STARTED)) {
            return reservedRecord;
        }

        nearCacheStats.decrementOwnedEntryMemoryCost(getTotalStorageMemoryCost(key, reservedRecord));

        updateRecordValue(reservedRecord, value);
        reservedRecord.casRecordState(UPDATE_STARTED, READ_PERMITTED);

        nearCacheStats.incrementOwnedEntryMemoryCost(getTotalStorageMemoryCost(key, reservedRecord));
        return reservedRecord;
    }

    protected long nextReservationId() {
        return RESERVATION_ID.incrementAndGet(this);
    }

    private class MaxSizeEvictionChecker implements EvictionChecker {

        @Override
        public boolean isEvictionRequired() {
            return maxSizeChecker != null && maxSizeChecker.isReachedToMaxSize();
        }
    }
}
