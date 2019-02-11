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
import com.hazelcast.core.IBiFunction;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.internal.eviction.EvictionChecker;
import com.hazelcast.internal.eviction.EvictionListener;
import com.hazelcast.internal.eviction.impl.evaluator.EvictionPolicyEvaluator;
import com.hazelcast.internal.eviction.impl.strategy.sampling.SamplingEvictionStrategy;
import com.hazelcast.internal.nearcache.FutureSupplier;
import com.hazelcast.internal.nearcache.NearCacheRecord;
import com.hazelcast.internal.nearcache.NearCacheRecordStore;
import com.hazelcast.internal.nearcache.impl.SampleableNearCacheRecordMap;
import com.hazelcast.internal.nearcache.impl.invalidation.MetaDataContainer;
import com.hazelcast.internal.nearcache.impl.invalidation.MinimalPartitionService;
import com.hazelcast.internal.nearcache.impl.invalidation.StaleReadDetector;
import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.monitor.impl.NearCacheStatsImpl;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.Clock;
import com.hazelcast.util.function.Supplier;

import javax.annotation.Nullable;
import java.util.concurrent.Executor;

import static com.hazelcast.internal.eviction.EvictionPolicyEvaluatorProvider.getEvictionPolicyEvaluator;
import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.MEM;
import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.MEM_AVAILABLE;
import static com.hazelcast.internal.nearcache.impl.invalidation.StaleReadDetector.ALWAYS_FRESH;
import static java.lang.String.format;

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
    protected static final long REFERENCE_SIZE = MEM_AVAILABLE ? MEM.arrayIndexScale(Object[].class) : (Integer.SIZE / Byte.SIZE);
    protected static final long MILLI_SECONDS_IN_A_SECOND = 1000;

    protected final long maxIdleMillis;
    protected final long timeToLiveMillis;
    protected final boolean serializeKeys;
    protected final boolean evictionDisabled;
    protected final boolean cacheLocalEntries;
    protected final boolean cacheNullValues;
    protected final Executor executor;
    protected final SerializationService ss;
    protected final ClassLoader classLoader;
    protected final InMemoryFormat valueInMemoryFormat;
    protected final NearCacheConfig nearCacheConfig;
    protected final NearCacheStatsImpl nearCacheStats;
    protected final MinimalPartitionService partitionService;
    protected final PartitioningStrategy partitioningStrategy;
    protected final Context context = new Context();

    protected NCRM records;
    protected EvictionChecker evictionChecker;
    protected SamplingEvictionStrategy<KS, R, NCRM> evictionStrategy;
    protected EvictionPolicyEvaluator<KS, R> evictionPolicyEvaluator;

    protected volatile StaleReadDetector staleReadDetector = ALWAYS_FRESH;

    public AbstractNearCacheRecordStore(NearCacheConfig nearCacheConfig,
                                        SerializationService ss,
                                        MinimalPartitionService partitionService,
                                        PartitioningStrategy partitioningStrategy,
                                        Executor executor,
                                        ClassLoader classLoader) {
        this(nearCacheConfig, new NearCacheStatsImpl(),
                ss, partitionService, partitioningStrategy, executor, classLoader);
    }

    // extended in EE
    protected AbstractNearCacheRecordStore(NearCacheConfig nearCacheConfig, NearCacheStatsImpl nearCacheStats,
                                           SerializationService ss,
                                           MinimalPartitionService partitionService,
                                           PartitioningStrategy partitioningStrategy,
                                           Executor executor,
                                           ClassLoader classLoader) {
        this.nearCacheConfig = nearCacheConfig;
        this.valueInMemoryFormat = nearCacheConfig.getInMemoryFormat();
        this.timeToLiveMillis = nearCacheConfig.getTimeToLiveSeconds() * MILLI_SECONDS_IN_A_SECOND;
        this.serializeKeys = nearCacheConfig.isSerializeKeys();
        this.cacheLocalEntries = nearCacheConfig.isCacheLocalEntries();
        this.cacheNullValues = true;
        this.maxIdleMillis = nearCacheConfig.getMaxIdleSeconds() * MILLI_SECONDS_IN_A_SECOND;
        this.ss = ss;
        this.classLoader = classLoader;
        this.nearCacheStats = nearCacheStats;
        this.evictionDisabled = nearCacheConfig.getEvictionConfig().getEvictionPolicy() == EvictionPolicy.NONE;
        this.partitionService = partitionService;
        this.partitioningStrategy = partitioningStrategy;
        this.executor = executor;
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

    protected abstract V toRecordValue(Object value);

    protected abstract R putRecord(K key, R record);

    protected void checkAvailable() {
        if (!isAvailable()) {
            throw new IllegalStateException(nearCacheConfig.getName()
                    + " named Near Cache record store is not available");
        }
    }

    private boolean isAvailable() {
        return records != null;
    }

    protected Data toData(Object obj) {
        return ss.toData(obj);
    }

    protected V toValue(Object obj) {
        return ss.toObject(obj);
    }

    protected long getTotalStorageMemoryCost(K key, R record) {
        return getKeyStorageMemoryCost(key) + getRecordStorageMemoryCost(record);
    }

    protected boolean isExpired(R record) {
        long now = Clock.currentTimeMillis();
        if (record.isExpiredAt(now)) {
            nearCacheStats.incrementExpirations();
            return true;
        } else {
            return record.isIdleAt(maxIdleMillis, now);
        }
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
        return record != null && !(record.getState() instanceof FutureSupplier);
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

    // only used for testing purposes
    public StaleReadDetector getStaleReadDetector() {
        return staleReadDetector;
    }

    public final class Context implements NearCacheContext<K, R> {

        public void updateRecordState(R record, Object ncKey,
                                      @Nullable Object dataValue,
                                      @Nullable Object objectValue, FutureSupplier supplier) {
            assert record != null;
            assert ncKey != null;

            Object newState = selectInMemoryFormatFriendlyValue(valueInMemoryFormat, dataValue, objectValue);
            record.casState(supplier, toRecordValue(newState));
            nearCacheStats.incrementOwnedEntryMemoryCost(getTotalStorageMemoryCost((K) ncKey, record));
            nearCacheStats.incrementOwnedEntryCount();
        }

        public Object toNearCacheKey(Data dataKey, Object objectKey) {
            assert !(objectKey instanceof Data);
            return serializeKeys ? dataKey : objectKey;
        }

        // TODO return cacheLocalEntries || clusterService.getLocalMember().isLiteMember() || !isOwn(keyData);
        public boolean cachingAllowedFor(Data keyData, Object response) {
            return cacheLocalEntries
                    || !isOwn(keyData)
                    || (cacheNullValues && response == null);
        }

        private boolean isOwn(Data key) {
            int partitionId = partitionService.getPartitionId(key);
            return partitionService.isPartitionOwner(partitionId);
        }

        public void removeRecord(K ncKey) {
            records.remove(ncKey);
        }

        public boolean serializeKeys() {
            return serializeKeys;
        }
    }

    protected void initInvalidationMetaData(R record, int partitionId) {
        if (staleReadDetector == ALWAYS_FRESH) {
            // means invalidation event creation is disabled for this Near Cache
            return;
        }

        MetaDataContainer metaDataContainer = staleReadDetector.getMetaDataContainer(partitionId);
        record.setPartitionId(partitionId);
        record.setInvalidationSequence(metaDataContainer.getSequence());
        record.setUuid(metaDataContainer.getUuid());
    }

    protected void updateInvalidationStats(K key, R record) {
        if (canUpdateStats(record)) {
            nearCacheStats.decrementOwnedEntryCount();
            nearCacheStats.decrementOwnedEntryMemoryCost(getTotalStorageMemoryCost(key, record));
            nearCacheStats.incrementInvalidations();
        }
    }

    protected class GetOrCreateWithSupplier implements IBiFunction<K, R, R> {

        private final Supplier<ICompletableFuture> supplier;

        public GetOrCreateWithSupplier(Supplier<ICompletableFuture> supplier) {
            this.supplier = supplier;
            ((FutureSupplier) supplier).setContext(context);
        }

        @Override
        public R apply(K ncKey, R record) {
            if (record != null) {
                record = removeIfStaleOrExpired(ncKey, record);
            }

            if (record == null) {
                record = createRecordWithSupplier(ncKey, supplier);
            }

            return record;
        }
    }

    protected R removeIfStaleOrExpired(K ncKey, R record) {
        if (staleReadDetector.isStaleRead(record) || isExpired(record)) {
            nearCacheStats.incrementMisses();
            nearCacheStats.incrementInvalidationRequests();
            updateInvalidationStats(ncKey, record);

            records.remove(ncKey);
            return null;
        }

        return record;
    }

    private R createRecordWithSupplier(K ncKey, Supplier<ICompletableFuture> supplier) {
        R record = createRecord(null);
        record.setState(supplier);
        ((FutureSupplier) supplier).supplyValueFor(ncKey, record);
        Data dataKey = ((FutureSupplier) supplier).getDataKey(ncKey);
        initInvalidationMetaData(record, partitionService.getPartitionId(dataKey));
        return record;
    }

    private boolean cachingAllowedFor(Data keyData) {
        return cacheLocalEntries || !isOwn(keyData);
    }

    private boolean isOwn(Data key) {
        int partitionId = partitionService.getPartitionId(key);
        return partitionService.isPartitionOwner(partitionId);
    }
}
