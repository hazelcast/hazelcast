/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.spi.properties.HazelcastProperties;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.internal.util.ToHeapDataConverter.toHeapData;
import static com.hazelcast.map.impl.ExpirationTimeSetter.calculateExpirationTime;
import static com.hazelcast.map.impl.ExpirationTimeSetter.pickMaxIdleMillis;
import static com.hazelcast.map.impl.ExpirationTimeSetter.pickTTLMillis;
import static com.hazelcast.map.impl.record.Record.UNSET;

/**
 * Always accessed by same single thread.
 * One instance of this class is created for every {@link RecordStore}.
 */
// TODO make expiry system efficient to scan and try to use HashMap iterator
public class ExpirySystem {

    final RecordStore recordStore;
    final MapServiceContext mapServiceContext;

    // TODO expiryDelayMillis
    private final long expiryDelayMillis;
    private final boolean canPrimaryDriveExpiration;
    private final MapContainer mapContainer;
    private final ClearExpiredRecordsTask clearExpiredRecordsTask;
    private final InvalidationQueue<ExpiredKey> expiredKeys = new InvalidationQueue<>();

    private Iterator<Map.Entry<Data, ExpiryMetadata>> expirationIterator;
    private Map<Data, ExpiryMetadata> expireTimeByKey;

    public ExpirySystem(RecordStore recordStore,
                        MapContainer mapContainer,
                        MapServiceContext mapServiceContext) {
        this.recordStore = recordStore;
        this.clearExpiredRecordsTask = mapServiceContext.getExpirationManager().getTask();
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        HazelcastProperties hazelcastProperties = nodeEngine.getProperties();
        this.expiryDelayMillis = hazelcastProperties.getMillis(ClusterProperty.MAP_EXPIRY_DELAY_SECONDS);
        this.mapContainer = mapContainer;
        this.mapServiceContext = mapServiceContext;
        this.canPrimaryDriveExpiration = mapServiceContext.getClearExpiredRecordsTask().canPrimaryDriveExpiration();
    }

    public boolean isEmpty() {
        return MapUtil.isNullOrEmpty(expireTimeByKey);
    }

    // this method is overridden
    protected Map<Data, ExpiryMetadata> createExpiryTimeByKeyMap() {
        // only reason we use CHM is, its iterator
        // doesn't throw concurrent modification exception.
        return new ConcurrentHashMap<>();
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

    public void addExpiry(Data key, long ttl, long maxIdle, long now) {
        MapConfig mapConfig = mapContainer.getMapConfig();
        long ttlMillis = pickTTLMillis(ttl, mapConfig);
        long maxIdleMillis = pickMaxIdleMillis(maxIdle, mapConfig);
        long expirationTime = calculateExpirationTime(ttlMillis, maxIdleMillis, now);

        addExpiry0(key, ttlMillis, maxIdleMillis, expirationTime);
    }

    public long getExpirationTime(long ttl, long maxIdle, long now) {
        MapConfig mapConfig = mapContainer.getMapConfig();
        long ttlMillis = pickTTLMillis(ttl, mapConfig);
        long maxIdleMillis = pickMaxIdleMillis(maxIdle, mapConfig);
        return calculateExpirationTime(ttlMillis, maxIdleMillis, now);
    }

    public void addExpiry0(Data key, long ttlMillis, long maxIdleMillis, long expirationTime) {
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

    // TODO add test for this.
    public void extendExpiryTime(Data dataKey, long now) {
        if(isEmpty()){
            return;
        }

        Map<Data, ExpiryMetadata> expireTimeByKey = getOrCreateExpireTimeByKeyMap(false);
        if (expireTimeByKey.isEmpty()) {
            return;
        }

        ExpiryMetadata expiryMetadata = expireTimeByKey.get(dataKey);
        if (expiryMetadata == null
                || expiryMetadata.getMaxIdle() == Long.MAX_VALUE) {
            return;
        }

        long expirationTime = calculateExpirationTime(expiryMetadata.getTtl(),
                expiryMetadata.getMaxIdle(), now);
        expiryMetadata.setExpirationTime(expirationTime);
    }

    public ExpiryReason hasExpired(Data key, long now, boolean backup) {
        Map<Data, ExpiryMetadata> expireTimeByKey = getOrCreateExpireTimeByKeyMap(false);
        if (expireTimeByKey.isEmpty()) {
            return ExpiryReason.NOT_EXPIRED;
        }
        ExpiryMetadata expiryMetadata = expireTimeByKey.get(key);
        return hasExpired0(expiryMetadata, now, backup);
    }

    // TODO add expiry delay for backup replica
    private ExpiryReason hasExpired0(ExpiryMetadata expiryMetadata, long now, boolean backup) {
        boolean expired = expiryMetadata != null
                && expiryMetadata.getExpirationTime() <= now;
        if (expired) {
            ExpiryReason expiryReason = expiryMetadata.getTtl() > expiryMetadata.getMaxIdle()
                    ? ExpiryReason.IDLENESS : ExpiryReason.TTL;
            if (expiryReason == ExpiryReason.IDLENESS
                    && backup && canPrimaryDriveExpiration) {
                return ExpiryReason.NOT_EXPIRED;
            }
            return expiryReason;
        }
        return ExpiryReason.NOT_EXPIRED;
    }

    public InvalidationQueue<ExpiredKey> getExpiredKeys() {
        return expiredKeys;
    }

    @Nonnull
    public ExpiryMetadata getExpiredMetadata(Data key) {
        ExpiryMetadata expiryMetadata = getOrCreateExpireTimeByKeyMap(false).get(key);
        return expiryMetadata != null ? expiryMetadata : ExpiryMetadata.NULL;
    }

    /**
     * Returns {@code true} if this record store has at least one candidate entry
     * for expiration (idle or tll) otherwise returns {@code false}.
     */
    public boolean isRecordStoreExpirable() {
        Map<Data, ExpiryMetadata> expireTimeByKey = getOrCreateExpireTimeByKeyMap(false);
        return !expireTimeByKey.isEmpty();
    }

    @SuppressWarnings("checkstyle:magicnumber")
    public void evictExpiredEntries(long now, int percentage, boolean backup) {
        int maxIterationCount = 100;
        int maxRetry = 3;
        int loop = 0;
        int evictedEntryCount = 0;
        while (true) {
            evictedEntryCount += evictExpiredEntriesInternal(maxIterationCount, now, backup);
            if (evictedEntryCount >= maxIterationCount) {
                break;
            }
            loop++;
            if (loop > maxRetry) {
                break;
            }
        }

        accumulateOrSendExpiredKey(null);
    }

    private int evictExpiredEntriesInternal(int maxIterationCount, long now, boolean backup) {
        Map<Data, ExpiryMetadata> expireTimeByKey = getOrCreateExpireTimeByKeyMap(false);
        if (expireTimeByKey.isEmpty()) {
            return 0;
        }

        int evictedEntryCount = 0;
//        if (expirationIterator == null || !expirationIterator.hasNext()) {
        expirationIterator = getIterator(expireTimeByKey);
//        }

        List dataKeyAndExpiryReason = new ArrayList<>();
        int checkedEntryCount = 0;
        // TODO how many entry will be checked?
        while (expirationIterator.hasNext()) {
            if (checkedEntryCount++ == maxIterationCount) {
                break;
            }
            Map.Entry<Data, ExpiryMetadata> entry = expirationIterator.next();
            Data key = entry.getKey();
            ExpiryMetadata expiryMetadata = entry.getValue();

            ExpiryReason expiryReason = hasExpired0(expiryMetadata, now, backup);
            if (expiryReason != ExpiryReason.NOT_EXPIRED && !recordStore.isLocked(key)) {
                dataKeyAndExpiryReason.add(key);
                dataKeyAndExpiryReason.add(expiryReason);
                callIterRemove(expirationIterator);
                evictedEntryCount++;
            }
        }

        for (int i = 0; i < dataKeyAndExpiryReason.size(); i += 2) {
            Data key = (Data) dataKeyAndExpiryReason.get(i);
            ExpiryReason reason = (ExpiryReason) dataKeyAndExpiryReason.get(i + 1);
            recordStore.evictExpiredEntryAndPublishExpiryEvent(key, reason, backup);
        }

        return evictedEntryCount;
    }

    // this method is overridden
    protected void callIterRemove(Iterator<Map.Entry<Data, ExpiryMetadata>> expirationIterator) {
        expirationIterator.remove();
    }

    // this method is overridden
    protected Iterator<Map.Entry<Data, ExpiryMetadata>> getIterator(Map<Data, ExpiryMetadata> expireTimeByKey) {
        return expireTimeByKey.entrySet().iterator();
    }

    public void removeKeyFromExpirySystem(Data key) {
        Map<Data, ExpiryMetadata> expireTimeByKey = getOrCreateExpireTimeByKeyMap(false);
        if (expireTimeByKey.isEmpty()) {
            return;
        }
        callRemove(key, expireTimeByKey);
    }

    // this method is overridden
    protected void callRemove(Data key, Map<Data, ExpiryMetadata> expireTimeByKey) {
        expireTimeByKey.remove(key);
    }

    // this method is overridden
    public void destroy() {
        getOrCreateExpireTimeByKeyMap(false).clear();
    }

    /**
     * Intended to put an upper bound to iterations. Used in evictions.
     *
     * @param size       of iterate-able.
     * @param percentage percentage of size.
     * @return 100 If calculated iteration count is less than 100, otherwise returns calculated iteration count.
     */
    private static int getMaxIterationCount(int size, int percentage) {
        final int defaultMaxIterationCount = 100;
        final float oneHundred = 100F;
        float maxIterationCount = size * (percentage / oneHundred);
        if (maxIterationCount <= defaultMaxIterationCount) {
            return defaultMaxIterationCount;
        }
        return Math.round(maxIterationCount);
    }

    // null dataKey is used to trigger backup operation sending...
    public void accumulateOrSendExpiredKey(Data dataKey) {
        if (mapContainer.getTotalBackupCount() == 0) {
            return;
        }

        if (dataKey != null) {
            expiredKeys.offer(new ExpiredKey(toHeapData(dataKey), UNSET));
        }

        clearExpiredRecordsTask.tryToSendBackupExpiryOp(recordStore, true);
    }

    // this method is overridden
    public void clear() {
        Map<Data, ExpiryMetadata> map = getOrCreateExpireTimeByKeyMap(false);
        map.clear();
    }
}













