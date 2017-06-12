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
import com.hazelcast.core.IBiFunction;
import com.hazelcast.internal.eviction.EvictionChecker;
import com.hazelcast.internal.eviction.EvictionListener;
import com.hazelcast.internal.eviction.impl.evaluator.EvictionPolicyEvaluator;
import com.hazelcast.internal.eviction.impl.strategy.sampling.SamplingEvictionStrategy;
import com.hazelcast.internal.nearcache.NearCacheRecord;
import com.hazelcast.internal.nearcache.NearCacheRecordStore;
import com.hazelcast.internal.nearcache.impl.SampleableNearCacheRecordMap;
import com.hazelcast.internal.nearcache.impl.invalidation.MetaDataContainer;
import com.hazelcast.internal.nearcache.impl.invalidation.StaleReadDetector;
import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.monitor.impl.NearCacheStatsImpl;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializableByConvention;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.Clock;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static com.hazelcast.config.EvictionPolicy.NONE;
import static com.hazelcast.internal.eviction.EvictionPolicyEvaluatorProvider.getEvictionPolicyEvaluator;
import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.MEM;
import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.MEM_AVAILABLE;
import static com.hazelcast.internal.nearcache.NearCacheRecord.NOT_RESERVED;
import static com.hazelcast.internal.nearcache.NearCacheRecord.TIME_NOT_SET;
import static com.hazelcast.internal.nearcache.impl.invalidation.StaleReadDetector.ALWAYS_FRESH;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static java.util.concurrent.TimeUnit.SECONDS;
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

    /**
     * If Unsafe is available, Object array index scale (every index represents a reference)
     * can be assumed as reference size.
     * <p>
     * Otherwise, we assume reference size as integer size that means
     * we assume 32 bit JVM or compressed-references enabled 64 bit JVM
     * by ignoring compressed-references disable mode on 64 bit JVM.
     */
    protected static final int REFERENCE_SIZE = MEM_AVAILABLE ? MEM.arrayIndexScale(Object[].class) : (Integer.SIZE / Byte.SIZE);
    protected static final AtomicLongFieldUpdater<AbstractNearCacheRecordStore> NEXT_RESERVATION_ID
            = newUpdater(AbstractNearCacheRecordStore.class, "nextReservationId");

    protected final boolean evictionDisabled;
    protected final long timeToLiveMillis;
    protected final long maxIdleMillis;
    protected final ClassLoader classLoader;
    protected final NearCacheConfig nearCacheConfig;
    protected final SerializationService serializationService;
    protected final NearCacheStatsImpl nearCacheStats;
    protected final boolean expirationEnabled;

    protected EvictionChecker evictionChecker;
    protected SamplingEvictionStrategy<KS, R, NCRM> evictionStrategy;
    protected EvictionPolicyEvaluator<KS, R> evictionPolicyEvaluator;
    protected NCRM records;

    protected volatile StaleReadDetector staleReadDetector = ALWAYS_FRESH;
    protected volatile long nextReservationId;

    public AbstractNearCacheRecordStore(NearCacheConfig nearCacheConfig, SerializationService serializationService,
                                        ClassLoader classLoader) {
        this(nearCacheConfig, new NearCacheStatsImpl(), serializationService, classLoader);
    }

    // extended in EE
    protected AbstractNearCacheRecordStore(NearCacheConfig nearCacheConfig, NearCacheStatsImpl nearCacheStats,
                                           SerializationService serializationService, ClassLoader classLoader) {
        this.nearCacheConfig = nearCacheConfig;
        this.timeToLiveMillis = SECONDS.toMillis(nearCacheConfig.getTimeToLiveSeconds());
        this.maxIdleMillis = SECONDS.toMillis(nearCacheConfig.getMaxIdleSeconds());
        this.serializationService = serializationService;
        this.classLoader = classLoader;
        this.nearCacheStats = nearCacheStats;
        this.evictionDisabled = nearCacheConfig.getEvictionConfig().getEvictionPolicy() == NONE;
        this.expirationEnabled = timeToLiveMillis != 0 || maxIdleMillis != 0;
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
    public StaleReadDetector getStaleReadDetector() {
        return staleReadDetector;
    }

    protected abstract EvictionChecker createNearCacheEvictionChecker(EvictionConfig evictionConfig,
                                                                      NearCacheConfig nearCacheConfig);

    protected abstract NCRM createNearCacheRecordMap(NearCacheConfig nearCacheConfig);

    protected abstract long getKeyStorageMemoryCost(K key);

    protected abstract long getRecordStorageMemoryCost(R record);

    protected abstract R valueToRecord(V value);

    protected abstract V recordToValue(R record);

    /**
     * Converts given object to near cache {@link com.hazelcast.config.InMemoryFormat}
     *
     * @return converted object
     */
    protected abstract Object toStorageType(V value);

    // public for tests
    public abstract R getRecord(K key);

    protected abstract R tryReserveForUpdateInternal(K key, long reservationId);

    protected abstract R tryPublishReservedInternal(K key, V value, long reservationId);

    protected abstract R putRecord(K key, R record);

    protected abstract R removeRecord(K key);

    protected abstract boolean containsRecordKey(K key);

    protected void checkAvailable() {
        if (!isAvailable()) {
            throw new IllegalStateException(nearCacheConfig.getName() + " named Near Cache record store is not available");
        }
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

    protected void onRecordAccess(R record) {
        record.setLastAccessTime(Clock.currentTimeMillis());
        record.incrementAccessHit();
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
        R record = null;
        V value = null;
        try {
            record = getRecord(key);
            if (record != null) {
                if (isTemporaryRecord(record)) {
                    // handles pure reservation record which is newly
                    // created and not like existing records
                    return null;
                }

                if (staleReadDetector.isStaleRead(key, record)) {
                    remove(key);
                    return null;
                }

                if (expirationEnabled && isRecordExpired(record)) {
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

    /**
     * @return {@code true} when the supplied record is temporarily created for reservation,
     * otherwise returns {@code false} if the record is an already existing record in near cache, for example
     * when {@link com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy#CACHE_ON_UPDATE} is used this method can return
     * {@code false} because the record we want to update can already be in near cache.
     */
    protected static boolean isTemporaryRecord(NearCacheRecord record) {
        return record.getCreationTime() == TIME_NOT_SET;
    }

    @Override
    public void put(K key, V value) {
        checkAvailable();
        // if there is no eviction configured we return if the Near Cache is full and it's a new key
        // (we have to check the key, otherwise we might lose updates on existing keys)
        if (evictionDisabled && evictionChecker.isEvictionRequired() && !containsRecordKey(key)) {
            return;
        }

        R record = null;
        R oldRecord = null;
        try {
            record = valueToRecord(value);
            onRecordCreate(key, record);
            record.setCreationTime(Clock.currentTimeMillis());
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
            if (record != null && !isTemporaryRecord(record)) {
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

    @Override
    public void clear() {
        checkAvailable();

        records.clear();
        nearCacheStats.setOwnedEntryCount(0);
        nearCacheStats.setOwnedEntryMemoryCost(0L);
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
    public void doEvictionIfRequired() {
        checkAvailable();

        if (!evictionDisabled) {
            evictionStrategy.evict(records, evictionPolicyEvaluator, evictionChecker, this);
        }
    }

    @Override
    public void doEviction() {
        checkAvailable();

        if (!evictionDisabled) {
            evictionStrategy.evict(records, evictionPolicyEvaluator, null, this);
        }
    }

    @Override
    public long tryReserveForUpdate(K key) {
        // if there is no eviction configured we return if the Near Cache is full and it's a new key
        // (we have to check the key, otherwise we might lose updates on existing keys)
        if (evictionDisabled && evictionChecker.isEvictionRequired() && !containsRecordKey(key)) {
            return NOT_RESERVED;
        }

        long reservationId = nextReservationId();
        R reservedRecord = tryReserveForUpdateInternal(key, reservationId);
        return reservedRecord.getReservationId() == reservationId ? reservationId : NOT_RESERVED;
    }

    @Override
    public V tryPublishReserved(K key, final V value, final long reservationId, boolean deserialize) {
        R existingRecord = tryPublishReservedInternal(key, value, reservationId);

        if (existingRecord == null || !deserialize) {
            return null;
        }

        return toValue(existingRecord.getValue());
    }

    protected long nextReservationId() {
        return NEXT_RESERVATION_ID.incrementAndGet(this);
    }

    private void onRecordCreate(K key, R record) {
        if (staleReadDetector == ALWAYS_FRESH) {
            // this means invalidations are off and no need to track metadata.
            return;
        }

        int partitionId = staleReadDetector.getPartitionId(key);
        MetaDataContainer metaDataContainer = staleReadDetector.getMetaDataContainer(partitionId);
        if (metaDataContainer != null) {
            record.setPartitionId(partitionId);
            record.setInvalidationSequence(metaDataContainer.getSequence());
            record.setUuid(metaDataContainer.getUuid());
        }
    }

    @SerializableByConvention
    protected class ReserveWithId implements IBiFunction<K, R, R> {

        private long reservationId;

        public ReserveWithId() {
        }

        public void init(long reservationId) {
            this.reservationId = reservationId;
        }

        @Override
        public R apply(K key, R oldRecord) {
            if (oldRecord == null) {
                // There is no existing record and we can create a temporary reservation record.
                R newRecord = null;
                try {
                    newRecord = valueToRecord(null);
                    newRecord.setCreationTime(TIME_NOT_SET);
                    newRecord.reserveWithId(reservationId);
                    onRecordCreate(key, newRecord);
                    return newRecord;
                } catch (Throwable throwable) {
                    onPutError(key, null, newRecord, null, throwable);
                    throw rethrow(throwable);
                }
            } else {
                // We are trying to reserve an already existing record.
                // This can happen in 2 cases:
                //  - there may be a previously reserved record.
                //  - CACHE_ON_UPDATE policy is configured and we are trying to set reservation id to existing record
                oldRecord.reserveWithId(reservationId);
                return oldRecord;
            }
        }
    }

    @SerializableByConvention
    protected class UpdateWithId implements IBiFunction<K, R, R> {

        private Object newValue;
        private long reservationId;

        public UpdateWithId() {
        }

        public void init(Object newValue, long reservationId) {
            this.newValue = newValue;
            this.reservationId = reservationId;
        }

        @Override
        public R apply(K key, R record) {
            if (reservationId != record.getReservationId()) {
                // another reservation took place and existing reservationId became obsolete
                return record;
            }

            boolean temporaryRecord = isTemporaryRecord(record);

            long netMemoryCost = 0;
            if (!temporaryRecord) {
                netMemoryCost -= getTotalStorageMemoryCost(key, record);
            }

            record.setValue(toStorageType((V) newValue));

            netMemoryCost += getTotalStorageMemoryCost(key, record);
            nearCacheStats.incrementOwnedEntryMemoryCost(netMemoryCost);

            if (temporaryRecord) {
                nearCacheStats.incrementOwnedEntryCount();
                record.setCreationTime(Clock.currentTimeMillis());
            }

            record.doReservable();

            return record;
        }
    }
}
