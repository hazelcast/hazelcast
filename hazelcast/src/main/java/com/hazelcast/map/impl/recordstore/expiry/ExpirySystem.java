/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.recordstore.expiry;

import com.hazelcast.config.MapConfig;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.eviction.ClearExpiredRecordsTask;
import com.hazelcast.internal.eviction.ExpiredKey;
import com.hazelcast.internal.nearcache.impl.invalidation.InvalidationQueue;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.internal.util.MapUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.util.ToHeapDataConverter.toHeapData;
import static com.hazelcast.map.impl.ExpirationTimeSetter.isTtlOrMaxIdleConfigured;
import static com.hazelcast.map.impl.ExpirationTimeSetter.nextExpirationTime;
import static com.hazelcast.map.impl.ExpirationTimeSetter.pickMaxIdleMillis;
import static com.hazelcast.map.impl.ExpirationTimeSetter.pickTTLMillis;
import static com.hazelcast.map.impl.record.Record.UNSET;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * This class has all logic to remove expired entries. Expiry reason
 * can be ttl or idleness. An instance of this class is created for
 * each {@link RecordStore} and it is always accessed by same single thread.
 */
public class ExpirySystem {
    private static final long DEFAULT_EXPIRED_KEY_SCAN_TIMEOUT_NANOS
            = TimeUnit.MILLISECONDS.toNanos(1);
    private static final String PROP_EXPIRED_KEY_SCAN_TIMEOUT_NANOS
            = "hazelcast.internal.map.expired.key.scan.timeout.nanos";
    private static final HazelcastProperty EXPIRED_KEY_SCAN_TIMEOUT_NANOS
            = new HazelcastProperty(PROP_EXPIRED_KEY_SCAN_TIMEOUT_NANOS,
            DEFAULT_EXPIRED_KEY_SCAN_TIMEOUT_NANOS, NANOSECONDS);
    private static final int ONE_HUNDRED_PERCENT = 100;
    private static final int MIN_TOTAL_NUMBER_OF_KEYS_TO_SCAN = 100;
    private static final int MAX_SAMPLE_AT_A_TIME = 16;
    private static final ThreadLocal<List> BATCH_OF_EXPIRED
            = ThreadLocal.withInitial(() -> new ArrayList<>(MAX_SAMPLE_AT_A_TIME << 1));

    private final long expiryDelayMillis;
    private final long expiredKeyScanTimeoutNanos;
    private final boolean canPrimaryDriveExpiration;
    private final ILogger logger;
    private final RecordStore recordStore;
    private final MapContainer mapContainer;
    private final MapServiceContext mapServiceContext;
    private final ClearExpiredRecordsTask clearExpiredRecordsTask;
    private final InvalidationQueue<ExpiredKey> expiredKeys = new InvalidationQueue<>();

    private Iterator<Map.Entry<Data, ExpiryMetadata>> cachedExpirationIterator;
    // This is volatile since it can be initialized at runtime lazily and
    // can be accessed by query threads besides partition ones.
    private volatile Map<Data, ExpiryMetadata> expireTimeByKey;

    public ExpirySystem(RecordStore recordStore,
                        MapContainer mapContainer,
                        MapServiceContext mapServiceContext) {
        this.recordStore = recordStore;
        this.clearExpiredRecordsTask = mapServiceContext.getExpirationManager().getTask();
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        this.logger = nodeEngine.getLogger(getClass());
        HazelcastProperties hazelcastProperties = nodeEngine.getProperties();
        this.expiryDelayMillis = hazelcastProperties.getMillis(ClusterProperty.MAP_EXPIRY_DELAY_SECONDS);
        this.mapContainer = mapContainer;
        this.mapServiceContext = mapServiceContext;
        this.canPrimaryDriveExpiration = mapServiceContext.getClearExpiredRecordsTask().canPrimaryDriveExpiration();
        this.expiredKeyScanTimeoutNanos = nodeEngine.getProperties().getNanos(EXPIRED_KEY_SCAN_TIMEOUT_NANOS);
    }

    public final boolean isEmpty() {
        return MapUtil.isNullOrEmpty(expireTimeByKey);
    }

    // this method is overridden
    protected Map<Data, ExpiryMetadata> createExpiryTimeByKeyMap() {
        // Operation and partition threads can have concurrent access
        // to this class that's why we used CHM here. Also its
        // iterator doesn't throw ConcurrentModificationException
        // and this makes incremental scanning of expirable
        // entries easy(see method `scanAndEvictExpiredKeys`).
        return new ConcurrentHashMap<>();
    }

    // this method is overridden
    public void clear() {
        Map<Data, ExpiryMetadata> map = getOrCreateExpireTimeByKeyMap(false);
        map.clear();
    }

    @Nonnull
    protected Map<Data, ExpiryMetadata> getOrCreateExpireTimeByKeyMap(boolean createIfAbsent) {
        if (expireTimeByKey != null) {
            return expireTimeByKey;
        }

        if (createIfAbsent) {
            expireTimeByKey = createExpiryTimeByKeyMap();
            return expireTimeByKey;
        }

        return Collections.emptyMap();
    }

    // this method is overridden
    protected ExpiryMetadata createExpiryMetadata(long ttlMillis, long maxIdleMillis,
                                                  long expirationTime, long lastUpdateTime) {
        return new ExpiryMetadataImpl(ttlMillis, maxIdleMillis, expirationTime, lastUpdateTime);
    }

    /**
     * Add expirable key to this expiry system.
     */
    public final void add(Data key, long ttl, long maxIdle,
                          long expiryTime, long now, long lastUpdateTime) {
        // If expiry-time <= 0, no expiry-time exists, this is update
        // or first put of the key hence we need to calculate it.
        // If expiry-time > 0, this means we have a previously
        // calculated expiry-time, we see this case in data replications.
        if (expiryTime <= 0) {
            MapConfig mapConfig = mapContainer.getMapConfig();
            ttl = pickTTLMillis(mapConfig, ttl);
            maxIdle = pickMaxIdleMillis(mapConfig, maxIdle);
            expiryTime = nextExpirationTime(ttl, maxIdle, now, lastUpdateTime);
        }

        storeExpiryMetadata(key, ttl, maxIdle, expiryTime, lastUpdateTime);
    }

    private void storeExpiryMetadata(Data key, long ttlMillis, long maxIdleMillis,
                                     long expiryTime, long lastUpdateTime) {
        // If expiryTime is long max, this
        // means key is no longer expirable.
        if (expiryTime == Long.MAX_VALUE) {
            removeKeyFromExpirySystem(key);
            return;
        }

        // RU_COMPAT_4_2
        if (isClusterVersionLessThanV5()) {
            checkIfTtlGreaterThanMaxIdle(recordStore.getName(),
                    lastUpdateTime, maxIdleMillis, ttlMillis);
        }

        createOrUpdateExpiryMetadata(key, ttlMillis, maxIdleMillis,
                expiryTime, lastUpdateTime);

        mapServiceContext.getExpirationManager().scheduleExpirationTask();
    }

    private void createOrUpdateExpiryMetadata(Data key, long ttlMillis, long maxIdleMillis,
                                              long expirationTime, long lastUpdateTime) {
        Map<Data, ExpiryMetadata> expireTimeByKey = getOrCreateExpireTimeByKeyMap(true);
        ExpiryMetadata expiryMetadata = expireTimeByKey.get(key);

        // create expiryMetadata if not exists
        if (expiryMetadata == null) {
            expiryMetadata = createExpiryMetadata(ttlMillis, maxIdleMillis,
                    expirationTime, lastUpdateTime);
            Data nativeKey = recordStore.getStorage().toBackingDataKeyFormat(key);
            expireTimeByKey.put(nativeKey, expiryMetadata);
            return;
        }

        // update existing expiryMetadata
        expiryMetadata.setTtl(ttlMillis)
                .setMaxIdle(maxIdleMillis)
                .setExpirationTime(expirationTime)
                .setLastUpdateTime(lastUpdateTime);
    }

    private boolean isClusterVersionLessThanV5() {
        return mapServiceContext.getNodeEngine().getClusterService()
                .getClusterVersion().isLessThan(Versions.V5_0);
    }

    private static void checkIfTtlGreaterThanMaxIdle(String mapName, long lastUpdateTime,
                                                     long maxIdleMillis, long ttlMillis) {
        if (lastUpdateTime == UNSET && isTtlOrMaxIdleConfigured(ttlMillis)
                && isTtlOrMaxIdleConfigured(maxIdleMillis) && ttlMillis > maxIdleMillis) {
            String message = "Map `%s` has timeToLiveSeconds `%d` which is greater than maxIdleSeconds `%d`, "
                    + "for this configuration to work, please set `perEntryStatsEnabled` field of map-config to `true`.";
            throw new UnsupportedOperationException(String.format(message, mapName,
                    MILLISECONDS.toSeconds(ttlMillis), MILLISECONDS.toSeconds(maxIdleMillis)));
        }
    }

    public final long calculateExpirationTime(long ttl, long maxIdle,
                                              long now, long lastUpdateTime) {
        MapConfig mapConfig = mapContainer.getMapConfig();
        long ttlMillis = pickTTLMillis(mapConfig, ttl);
        long maxIdleMillis = pickMaxIdleMillis(mapConfig, maxIdle);
        return nextExpirationTime(ttlMillis, maxIdleMillis, now, lastUpdateTime);
    }

    public final void removeKeyFromExpirySystem(Data key) {
        if (isEmpty()) {
            return;
        }
        callRemove(key, expireTimeByKey);
    }

    public final void extendExpiryTime(Data dataKey, long now, long lastUpdateTime) {
        Map<Data, ExpiryMetadata> expireTimeByKey = getOrCreateExpireTimeByKeyMap(false);
        ExpiryMetadata expiryMetadata = getExpiryMetadataForExpiryCheck(dataKey, expireTimeByKey);
        if (expiryMetadata == ExpiryMetadata.NULL) {
            return;
        }

        long maxIdle = expiryMetadata.getMaxIdle();
        if (maxIdle == Long.MAX_VALUE) {
            return;
        }

        long ttl = expiryMetadata.getTtl();
        if (ttl <= maxIdle) {
            return;
        }

        // RU_COMPAT_4_2
        lastUpdateTime = isClusterVersionLessThanV5()
                ? lastUpdateTime : expiryMetadata.getLastUpdateTime();

        expiryMetadata.setExpirationTime(nextExpirationTime(ttl,
                maxIdle, now, lastUpdateTime));
    }

    public final ExpiryReason hasExpired(Data key, long now, boolean backup) {
        Map<Data, ExpiryMetadata> expireTimeByKey = getOrCreateExpireTimeByKeyMap(false);
        ExpiryMetadata expiryMetadata = getExpiryMetadataForExpiryCheck(key, expireTimeByKey);
        return hasExpired(expiryMetadata, now, backup);
    }

    private ExpiryReason hasExpired(ExpiryMetadata expiryMetadata, long now, boolean backup) {
        if (expiryMetadata == null
                || expiryMetadata == ExpiryMetadata.NULL) {
            return ExpiryReason.NOT_EXPIRED;
        }

        long nextExpirationTime = backup
                ? expiryMetadata.getExpirationTime() + expiryDelayMillis
                : expiryMetadata.getExpirationTime();

        if (nextExpirationTime > now) {
            return ExpiryReason.NOT_EXPIRED;
        }

        ExpiryReason expiryReason = expiryMetadata.getMaxIdle() <= expiryMetadata.getTtl()
                ? ExpiryReason.MAX_IDLE_SECONDS : ExpiryReason.TTL;

        if (backup && canPrimaryDriveExpiration
                && expiryReason == ExpiryReason.MAX_IDLE_SECONDS) {
            return ExpiryReason.NOT_EXPIRED;
        }
        return expiryReason;
    }

    public final InvalidationQueue<ExpiredKey> getExpiredKeys() {
        return expiredKeys;
    }

    @Nonnull
    public final ExpiryMetadata getExpiredMetadata(Data key) {
        ExpiryMetadata expiryMetadata = getOrCreateExpireTimeByKeyMap(false).get(key);
        return expiryMetadata != null ? expiryMetadata : ExpiryMetadata.NULL;
    }

    @SuppressWarnings("checkstyle:magicnumber")
    public final void evictExpiredEntries(final int percentage, final long now, final boolean backup) {
        // 1. Find how many keys we can scan at max.
        final int maxScannableCount = findMaxScannableCount(percentage);
        if (maxScannableCount == 0) {
            // no expirable entry exists.
            return;
        }

        // 2. Do scanning and evict expired keys.
        int scannedCount = 0;
        int expiredCount = 0;
        try {
            long scanLoopStartNanos = System.nanoTime();
            do {
                scannedCount += findExpiredKeys(now, backup);
                expiredCount += evictExpiredKeys(backup);
            } while (scannedCount < maxScannableCount && getOrInitCachedIterator().hasNext()
                    && (System.nanoTime() - scanLoopStartNanos) < expiredKeyScanTimeoutNanos);
        } catch (Exception e) {
            BATCH_OF_EXPIRED.get().clear();
            throw ExceptionUtil.rethrow(e);
        }

        // 3. Send expired keys to backups(only valid for max-idle-expiry)
        tryToSendBackupExpiryOp();

        if (logger.isFinestEnabled()) {
            logProgress(maxScannableCount, scannedCount, expiredCount);
        }
    }

    private void logProgress(int maxScannableCount, int scannedCount, int expiredCount) {
        logger.finest(String.format("mapName: %s, partitionId: %d, partitionSize: %d, "
                        + "remainingKeyCountToExpire: %d, maxScannableKeyCount: %d, "
                        + "scannedKeyCount: %d, expiredKeyCount: %d"
                , recordStore.getName(), recordStore.getPartitionId(), recordStore.size()
                , expireTimeByKey.size(), maxScannableCount, scannedCount, expiredCount));
    }

    private int findMaxScannableCount(int percentage) {
        Map<Data, ExpiryMetadata> expireTimeByKey = getOrCreateExpireTimeByKeyMap(false);
        int numberOfExpirableKeys = expireTimeByKey.size();
        if (numberOfExpirableKeys <= MIN_TOTAL_NUMBER_OF_KEYS_TO_SCAN) {
            return numberOfExpirableKeys;
        }

        int percentageOfExpirableKeys = (int) (1D * numberOfExpirableKeys * percentage / ONE_HUNDRED_PERCENT);
        return Math.max(MIN_TOTAL_NUMBER_OF_KEYS_TO_SCAN, percentageOfExpirableKeys);
    }

    /**
     * Get cachedExpirationIterator or init it if it has no next entry.
     */
    private Iterator<Map.Entry<Data, ExpiryMetadata>> getOrInitCachedIterator() {
        if (cachedExpirationIterator == null || !cachedExpirationIterator.hasNext()) {
            cachedExpirationIterator = initIteratorOf(expireTimeByKey);
        }
        return cachedExpirationIterator;
    }

    private int findExpiredKeys(long now, boolean backup) {
        List batchOfExpired = BATCH_OF_EXPIRED.get();

        int scannedCount = 0;
        Iterator<Map.Entry<Data, ExpiryMetadata>> cachedIterator = getOrInitCachedIterator();
        while (scannedCount++ < MAX_SAMPLE_AT_A_TIME && cachedIterator.hasNext()) {
            Map.Entry<Data, ExpiryMetadata> entry = cachedIterator.next();
            Data key = entry.getKey();
            ExpiryMetadata expiryMetadata = entry.getValue();

            ExpiryReason expiryReason = hasExpired(expiryMetadata, now, backup);
            if (expiryReason != ExpiryReason.NOT_EXPIRED && !recordStore.isLocked(key)) {
                // add key and expiryReason to list to evict them later
                batchOfExpired.add(key);
                batchOfExpired.add(expiryReason);
            }
        }
        return scannedCount;
    }

    private int evictExpiredKeys(boolean backup) {
        int evictedCount = 0;

        List batchOfExpired = BATCH_OF_EXPIRED.get();
        try {
            for (int i = 0; i < batchOfExpired.size(); i += 2) {
                Data key = (Data) batchOfExpired.get(i);
                ExpiryReason expiryReason = (ExpiryReason) batchOfExpired.get(i + 1);
                recordStore.evictExpiredEntryAndPublishExpiryEvent(key, expiryReason, backup);
                callRemove(key, expireTimeByKey);
                evictedCount++;
            }
        } finally {
            batchOfExpired.clear();
        }
        return evictedCount;
    }

    // this method is overridden
    @Nonnull
    protected ExpiryMetadata getExpiryMetadataForExpiryCheck(Data key,
                                                             Map<Data, ExpiryMetadata> expireTimeByKey) {
        ExpiryMetadata expiryMetadata = expireTimeByKey.get(key);
        return expiryMetadata == null ? ExpiryMetadata.NULL : expiryMetadata;
    }

    // this method is overridden
    protected Iterator<Map.Entry<Data, ExpiryMetadata>> initIteratorOf(Map<Data, ExpiryMetadata> expireTimeByKey) {
        return expireTimeByKey.entrySet().iterator();
    }

    // this method is overridden
    protected void callRemove(Data key, Map<Data, ExpiryMetadata> expireTimeByKey) {
        expireTimeByKey.remove(key);
    }

    // this method is overridden
    public void destroy() {
        getOrCreateExpireTimeByKeyMap(false).clear();
    }

    public final void accumulateOrSendExpiredKey(Data dataKey, long valueHashCode) {
        if (mapContainer.getTotalBackupCount() == 0) {
            return;
        }

        if (dataKey != null) {
            expiredKeys.offer(new ExpiredKey(toHeapData(dataKey), valueHashCode));
        }

        clearExpiredRecordsTask.tryToSendBackupExpiryOp(recordStore, true);
    }

    public final void tryToSendBackupExpiryOp() {
        if (mapContainer.getTotalBackupCount() == 0) {
            return;
        }

        clearExpiredRecordsTask.tryToSendBackupExpiryOp(recordStore, true);
    }
}
