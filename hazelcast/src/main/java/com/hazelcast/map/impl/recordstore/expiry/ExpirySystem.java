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
import com.hazelcast.internal.eviction.ClearExpiredRecordsTask;
import com.hazelcast.internal.eviction.ExpiredKey;
import com.hazelcast.internal.nearcache.impl.invalidation.InvalidationQueue;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.MapUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.ExpirationTimeSetter;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.eviction.Evictor;
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
import static com.hazelcast.map.impl.ExpirationTimeSetter.pickMaxIdleMillis;
import static com.hazelcast.map.impl.ExpirationTimeSetter.pickTTLMillis;
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
    private static final int MIN_SCANNABLE_ENTRY_COUNT = 100;

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
    private Map<Data, ExpiryMetadata> expireTimeByKey;

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

    public boolean isEmpty() {
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
    protected ExpiryMetadata createExpiryMetadata(long ttlMillis, long maxIdleMillis, long expirationTime) {
        return new ExpiryMetadataImpl(ttlMillis, maxIdleMillis, expirationTime);
    }

    public void addKeyIfExpirable(Data key, long ttl, long maxIdle, long expiryTime, long now) {
        if (expiryTime <= 0) {
            MapConfig mapConfig = mapContainer.getMapConfig();
            long ttlMillis = pickTTLMillis(ttl, mapConfig);
            long maxIdleMillis = pickMaxIdleMillis(maxIdle, mapConfig);
            long expirationTime = ExpirationTimeSetter.calculateExpirationTime(ttlMillis, maxIdleMillis, now);
            addExpirableKey(key, ttlMillis, maxIdleMillis, expirationTime);
        } else {
            addExpirableKey(key, ttl, maxIdle, expiryTime);
        }
    }

    private void addExpirableKey(Data key, long ttlMillis, long maxIdleMillis, long expirationTime) {
        if (expirationTime == Long.MAX_VALUE) {
            Map<Data, ExpiryMetadata> map = getOrCreateExpireTimeByKeyMap(false);
            if (!map.isEmpty()) {
                Data nativeKey = recordStore.getStorage().toBackingDataKeyFormat(key);
                callRemove(nativeKey, expireTimeByKey);
            }
            return;
        }

        Map<Data, ExpiryMetadata> expireTimeByKey = getOrCreateExpireTimeByKeyMap(true);
        ExpiryMetadata expiryMetadata = expireTimeByKey.get(key);
        if (expiryMetadata == null) {
            expiryMetadata = createExpiryMetadata(ttlMillis, maxIdleMillis, expirationTime);
            Data nativeKey = recordStore.getStorage().toBackingDataKeyFormat(key);
            expireTimeByKey.put(nativeKey, expiryMetadata);
        } else {
            expiryMetadata.setTtl(ttlMillis)
                    .setMaxIdle(maxIdleMillis)
                    .setExpirationTime(expirationTime);
        }

        mapServiceContext.getExpirationManager().scheduleExpirationTask();
    }

    public long calculateExpirationTime(long ttl, long maxIdle, long now) {
        MapConfig mapConfig = mapContainer.getMapConfig();
        long ttlMillis = pickTTLMillis(ttl, mapConfig);
        long maxIdleMillis = pickMaxIdleMillis(maxIdle, mapConfig);
        return ExpirationTimeSetter.calculateExpirationTime(ttlMillis, maxIdleMillis, now);
    }

    public void removeKeyFromExpirySystem(Data key) {
        Map<Data, ExpiryMetadata> expireTimeByKey = getOrCreateExpireTimeByKeyMap(false);
        if (expireTimeByKey.isEmpty()) {
            return;
        }
        callRemove(key, expireTimeByKey);
    }

    public void extendExpiryTime(Data dataKey, long now) {
        if (isEmpty()) {
            return;
        }

        Map<Data, ExpiryMetadata> expireTimeByKey = getOrCreateExpireTimeByKeyMap(false);
        if (expireTimeByKey.isEmpty()) {
            return;
        }

        ExpiryMetadata expiryMetadata = getExpiryMetadataForExpiryCheck(dataKey, expireTimeByKey);
        if (expiryMetadata == null
                || expiryMetadata.getMaxIdle() == Long.MAX_VALUE) {
            return;
        }

        long expirationTime = ExpirationTimeSetter.calculateExpirationTime(expiryMetadata.getTtl(),
                expiryMetadata.getMaxIdle(), now);
        expiryMetadata.setExpirationTime(expirationTime);
    }

    public ExpiryReason hasExpired(Data key, long now, boolean backup) {
        Map<Data, ExpiryMetadata> expireTimeByKey = getOrCreateExpireTimeByKeyMap(false);
        if (expireTimeByKey.isEmpty()) {
            return ExpiryReason.NOT_EXPIRED;
        }
        ExpiryMetadata expiryMetadata = getExpiryMetadataForExpiryCheck(key, expireTimeByKey);
        return hasExpired(expiryMetadata, now, backup);
    }

    private ExpiryReason hasExpired(ExpiryMetadata expiryMetadata, long now, boolean backup) {
        if (expiryMetadata == null) {
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

    public InvalidationQueue<ExpiredKey> getExpiredKeys() {
        return expiredKeys;
    }

    @Nonnull
    public ExpiryMetadata getExpiredMetadata(Data key) {
        ExpiryMetadata expiryMetadata = getOrCreateExpireTimeByKeyMap(false).get(key);
        return expiryMetadata != null ? expiryMetadata : ExpiryMetadata.NULL;
    }

    @SuppressWarnings("checkstyle:magicnumber")
    public void evictExpiredEntries(int percentage, long now, boolean backup) {
        Map<Data, ExpiryMetadata> expireTimeByKey = getOrCreateExpireTimeByKeyMap(false);
        if (expireTimeByKey.isEmpty()) {
            return;
        }

        // Find max scannable key count
        int expirableKeysMapSize = expireTimeByKey.size();
        int keyCountInPercentage = (int) (1D * expirableKeysMapSize * percentage / ONE_HUNDRED_PERCENT);
        int maxScannableKeyCount = Math.max(MIN_SCANNABLE_ENTRY_COUNT, keyCountInPercentage);

        scanAndEvictExpiredKeys(maxScannableKeyCount, now, backup);

        tryToSendBackupExpiryOp();
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

    private void scanAndEvictExpiredKeys(int maxScannableKeyCount, long now, boolean backup) {
        // Scan to find expired keys.
        long scanLoopStartNanos = System.nanoTime();
        List expiredKeyExpiryReasonList = new ArrayList<>();
        int scannedKeyCount = 0;
        int expiredKeyCount = 0;
        do {
            Map.Entry<Data, ExpiryMetadata> entry = getOrInitCachedIterator().next();
            scannedKeyCount++;
            Data key = entry.getKey();
            ExpiryMetadata expiryMetadata = entry.getValue();

            ExpiryReason expiryReason = hasExpired(expiryMetadata, now, backup);
            if (expiryReason != ExpiryReason.NOT_EXPIRED && !recordStore.isLocked(key)) {
                // add key and expiryReason to list to evict them later
                expiredKeyExpiryReasonList.add(key);
                expiredKeyExpiryReasonList.add(expiryReason);
                // remove expired key from expirySystem
                callIterRemove(cachedExpirationIterator);
                expiredKeyCount++;
            }

            // - If timed out while looping, break this loop to free
            // partition thread.
            // - Scan at least Evictor.SAMPLE_COUNT keys. During
            // eviction we also check this number of keys.
            if (scannedKeyCount % Evictor.SAMPLE_COUNT == 0
                    && (System.nanoTime() - scanLoopStartNanos) >= expiredKeyScanTimeoutNanos) {
                break;
            }

        } while (scannedKeyCount < maxScannableKeyCount && getOrInitCachedIterator().hasNext());

        // Evict expired keys
        for (int i = 0; i < expiredKeyExpiryReasonList.size(); i += 2) {
            Data key = (Data) expiredKeyExpiryReasonList.get(i);
            ExpiryReason reason = (ExpiryReason) expiredKeyExpiryReasonList.get(i + 1);
            recordStore.evictExpiredEntryAndPublishExpiryEvent(key, reason, backup);
        }

        if (logger.isFinestEnabled()) {
            logger.finest(String.format("mapName: %s, partitionId: %d, partitionSize: %d, "
                            + "remainingKeyCountToExpire: %d, maxScannableKeyCount: %d, "
                            + "scannedKeyCount: %d, expiredKeyCount: %d"
                    , recordStore.getName(), recordStore.getPartitionId(), recordStore.size()
                    , expireTimeByKey.size(), maxScannableKeyCount, scannedKeyCount, expiredKeyCount));
        }
    }

    // this method is overridden
    protected ExpiryMetadata getExpiryMetadataForExpiryCheck(Data key,
                                                             Map<Data, ExpiryMetadata> expireTimeByKey) {
        return expireTimeByKey.get(key);
    }

    // this method is overridden
    protected void callIterRemove(Iterator<Map.Entry<Data, ExpiryMetadata>> expirationIterator) {
        expirationIterator.remove();
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

    public void accumulateOrSendExpiredKey(Data dataKey, long valueHashCode) {
        if (mapContainer.getTotalBackupCount() == 0) {
            return;
        }

        if (dataKey != null) {
            expiredKeys.offer(new ExpiredKey(toHeapData(dataKey), valueHashCode));
        }

        clearExpiredRecordsTask.tryToSendBackupExpiryOp(recordStore, true);
    }

    public void tryToSendBackupExpiryOp() {
        if (mapContainer.getTotalBackupCount() == 0) {
            return;
        }

        clearExpiredRecordsTask.tryToSendBackupExpiryOp(recordStore, true);
    }
}
