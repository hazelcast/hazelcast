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

package com.hazelcast.map.impl.recordstore;

import com.hazelcast.config.MapConfig;
import com.hazelcast.internal.eviction.ClearExpiredRecordsTask;
import com.hazelcast.internal.eviction.ExpiredKey;
import com.hazelcast.internal.nearcache.impl.invalidation.InvalidationQueue;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.spi.properties.HazelcastProperties;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.util.ToHeapDataConverter.toHeapData;
import static com.hazelcast.map.impl.ExpirationTimeSetter.calculateExpirationTime;
import static com.hazelcast.map.impl.ExpirationTimeSetter.pickMaxIdleMillis;
import static com.hazelcast.map.impl.ExpirationTimeSetter.pickTTLMillis;
import static com.hazelcast.map.impl.record.Record.UNSET;

/**
 * Always accessed by 1 thread.
 */
public class ExpirySystem {
    final RecordStore recordStore;
    final MapServiceContext mapServiceContext;

    private final long expiryDelayMillis;
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
    }

    public Map<Data, ExpiryMetadata> getExpireTimeByKey() {
        return expireTimeByKey;
    }

    protected Map<Data, ExpiryMetadata> createExpiryTimeByKeyMap() {
        // only reason we use CHM is, its iterator
        // doesn't throw concurrent modification exception.
        return new ConcurrentHashMap<>();
    }

    private Map<Data, ExpiryMetadata> getOrCreateExpireTimeByKeyMap(boolean createIfAbsent) {
        if (expireTimeByKey != null) {
            return expireTimeByKey;
        }

        if (createIfAbsent) {
            expireTimeByKey = createExpiryTimeByKeyMap();
            return expireTimeByKey;
        }

        return Collections.emptyMap();
    }

    @NotNull
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

    public void addExpiry0(Data key, long ttlMillis, long maxIdleMillis, long expirationTime) {
        if (expirationTime == Long.MAX_VALUE) {
            Map<Data, ExpiryMetadata> map = getOrCreateExpireTimeByKeyMap(false);
            if (!map.isEmpty()) {
                Data nativeKey = recordStore.getStorage().toBackingDataKeyFormat(key);
                map.remove(nativeKey);
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

    public ExpiryReason hasExpired(Data key, long now) {
        Map<Data, ExpiryMetadata> expireTimeByKey = getOrCreateExpireTimeByKeyMap(false);
        if (expireTimeByKey.isEmpty()) {
            return ExpiryReason.NOT_EXPIRED;
        }
        ExpiryMetadata expiryMetadata = expireTimeByKey.get(key);
        //System.err.println("hasExpired --> now: " + now + ", " + expiryMetadata);
        return hasExpired0(expiryMetadata, now);
    }

    // TODO add expiry delay for backup replica
    public ExpiryReason hasExpired0(ExpiryMetadata expiryMetadata, long now) {
        //System.err.println(expiryMetadata);
        boolean expired = expiryMetadata != null
                && expiryMetadata.getExpirationTime() <= now;
        if (expired) {
            return expiryMetadata.getTtl() > expiryMetadata.getMaxIdle()
                    ? ExpiryReason.TTL : ExpiryReason.IDLENESS;
        }
        return ExpiryReason.NOT_EXPIRED;
    }

    public InvalidationQueue<ExpiredKey> getExpiredKeys() {
        return expiredKeys;
    }

    /**
     * Returns {@code true} if this record store has at least one candidate entry
     * for expiration (idle or tll) otherwise returns {@code false}.
     */
    public boolean isRecordStoreExpirable() {
        Map<Data, ExpiryMetadata> expireTimeByKey = getOrCreateExpireTimeByKeyMap(false);
        return !expireTimeByKey.isEmpty();
    }

    public void evictExpiredEntries(long now, int percentage, boolean backup) {
        int size = recordStore.size();
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

        //System.err.println("evictedEntryCount: " + evictedEntryCount);

        accumulateOrSendExpiredKey(null);
    }

    private int evictExpiredEntriesInternal(int maxIterationCount, long now, boolean backup) {
//        if (backup) {
//            System.err.println("backup");
//        }


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

            ExpiryReason expiryReason = hasExpired0(expiryMetadata, now);
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
            recordStore.evictExpiredAndPublishExpiryEvent(key, reason, backup);
        }

        return evictedEntryCount;
    }

    protected void callIterRemove(Iterator<Map.Entry<Data, ExpiryMetadata>> expirationIterator) {
        expirationIterator.remove();
    }

    @NotNull
    protected Iterator<Map.Entry<Data, ExpiryMetadata>> getIterator(Map<Data, ExpiryMetadata> expireTimeByKey) {
        return expireTimeByKey.entrySet().iterator();
    }

    public void informEvicted(Data key) {
        Map<Data, ExpiryMetadata> expireTimeByKey = getOrCreateExpireTimeByKeyMap(false);
        if (expireTimeByKey.isEmpty()) {
            return;
        }
        callRemove(key, expireTimeByKey);
    }

    protected void callRemove(Data key, Map<Data, ExpiryMetadata> expireTimeByKey) {
        expireTimeByKey.remove(key);
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

    public void markRecordStoreExpirable(Data key, long ttl, long maxIdle, long now) {
        addExpiry(key, ttl, maxIdle, now);
    }

    public boolean isTtlOrMaxIdleDefined(Record record) {
        long ttl = record.getTtl();
        long maxIdle = record.getMaxIdle();
        return isTtlDefined(ttl) || isMaxIdleDefined(maxIdle);
    }

    // this method is overridden on ee

    protected boolean isTtlDefined(long ttl) {
        return ttl > 0L && ttl < Long.MAX_VALUE;
    }

    protected boolean isMaxIdleDefined(long maxIdle) {
        return maxIdle > 0L && maxIdle < Long.MAX_VALUE;
    }

    boolean isIdleExpired(Data dataKey, long now, boolean backup) {
        assert dataKey != null;

        if (backup && mapServiceContext.getClearExpiredRecordsTask().canPrimaryDriveExpiration()) {
            // don't check idle expiry on backup
            return false;
        }

        long nextExpiryTime = backup ? now + expiryDelayMillis : now;
        ExpiryReason expiryReason = hasExpired(dataKey, nextExpiryTime);
        return expiryReason == ExpiryReason.IDLENESS;
    }

    boolean isTTLExpired(Data dataKey, long now, boolean backup) {
        assert dataKey != null;
        long nextExpiryTime = backup ? now + expiryDelayMillis : now;
        ExpiryReason expiryReason = hasExpired(dataKey, nextExpiryTime);
        return expiryReason == ExpiryReason.TTL;
    }

    private long getRecordMaxIdleOrConfig(Record record) {
        if (record.getMaxIdle() != UNSET) {
            return record.getMaxIdle();
        }

        return TimeUnit.SECONDS.toMillis(mapContainer.getMapConfig().getMaxIdleSeconds());
    }

    private long getRecordTTLOrConfig(Record record) {
        if (record.getTtl() != UNSET) {
            return record.getTtl();
        }

        return TimeUnit.SECONDS.toMillis(mapContainer.getMapConfig().getTimeToLiveSeconds());
    }

    // null dataKey is used to trigger backup operation sending...
    void accumulateOrSendExpiredKey(Data dataKey) {
        if (mapContainer.getTotalBackupCount() == 0) {
            return;
        }

        if (dataKey != null) {
            expiredKeys.offer(new ExpiredKey(toHeapData(dataKey), UNSET));
        }

        clearExpiredRecordsTask.tryToSendBackupExpiryOp(recordStore, true);
    }

    public enum ExpiryReason {
        TTL,
        IDLENESS,
        NOT_EXPIRED
    }

    public static class ExpiryMetadataImpl implements ExpiryMetadata {
        long ttl;
        long maxIdle;
        long expirationTime;

        public ExpiryMetadataImpl(long ttl, long maxIdle, long expirationTime) {
            this.ttl = ttl;
            this.maxIdle = maxIdle;
            this.expirationTime = expirationTime;
        }

        @Override
        public long getTtl() {
            return ttl;
        }

        @Override
        public ExpiryMetadata setTtl(long ttl) {
            this.ttl = ttl;
            return this;
        }

        @Override
        public long getMaxIdle() {
            return maxIdle;
        }

        @Override
        public ExpiryMetadata setMaxIdle(long maxIdle) {
            this.maxIdle = maxIdle;
            return this;
        }

        @Override
        public long getExpirationTime() {
            return expirationTime;
        }

        @Override
        public ExpiryMetadata setExpirationTime(long expirationTime) {
            this.expirationTime = expirationTime;
            return this;
        }

        @Override
        public String toString() {
            return "ExpiryMetadata{"
                    + "ttl=" + ttl
                    + ", maxIdle=" + maxIdle
                    + ", expirationTime=" + expirationTime
                    + '}';
        }
    }

    public interface ExpiryMetadata {

        long getTtl();

        ExpiryMetadata setTtl(long ttl);

        long getMaxIdle();

        ExpiryMetadata setMaxIdle(long maxIdle);

        long getExpirationTime();

        ExpiryMetadata setExpirationTime(long expirationTime);
    }


}













