/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.nearcache.impl.invalidation;

import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.internal.nearcache.impl.DefaultNearCache;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static com.hazelcast.util.Preconditions.checkNotNegative;
import static java.lang.String.format;
import static java.lang.System.nanoTime;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * This task runs on near-cache-end and only one instance is created per data-structure type like imap and icache.
 * Repairing responsibilities of this task are:
 * <ul>
 * <li>
 * To scan {@link RepairingHandler}s to see if any near-cache needs to be invalidated
 * according to missed invalidation counts. Controlled via {@link RepairingTask#MAX_TOLERATED_MISS_COUNT}
 * </li>
 * <li>
 * To send periodic generic-operations to cluster members in order to fetch latest partition sequences and UUIDs.
 * Controlled via {@link RepairingTask#MIN_RECONCILIATION_INTERVAL_SECONDS}
 * </li>
 * </ul>
 */
public final class RepairingTask implements Runnable {

    public static final HazelcastProperty MAX_TOLERATED_MISS_COUNT
            = new HazelcastProperty("hazelcast.invalidation.max.tolerated.miss.count", 10);
    public static final HazelcastProperty RECONCILIATION_INTERVAL_SECONDS
            = new HazelcastProperty("hazelcast.invalidation.reconciliation.interval.seconds", 60, SECONDS);

    private static final long GET_UUID_TASK_SCHEDULE_MILLIS = 500;
    private static final long HALF_MINUTE_MILLIS = SECONDS.toMillis(30);
    private static final long MIN_RECONCILIATION_INTERVAL_SECONDS = 30;

    private final int partitionCount;
    private final int maxToleratedMissCount;
    private final long reconciliationIntervalNanos;
    private final String localUuid;
    private final ILogger logger;
    private final ExecutionService executionService;
    private final AtomicReferenceArray<UUID> partitionUuids;
    private final MinimalPartitionService partitionService;
    private final MetaDataFetcher metaDataFetcher;
    private final ConcurrentMap<String, RepairingHandler> handlers = new ConcurrentHashMap<String, RepairingHandler>();
    private final AtomicBoolean running = new AtomicBoolean(false);

    private volatile long lastAntiEntropyRunNanos;

    public RepairingTask(MetaDataFetcher metaDataFetcher, ExecutionService executionService,
                         MinimalPartitionService partitionService, HazelcastProperties properties,
                         String localUuid, ILogger logger) {
        this.logger = logger;
        this.reconciliationIntervalNanos = SECONDS.toNanos(checkAndGetReconciliationIntervalSeconds(properties));
        this.partitionCount = partitionService.getPartitionCount();
        this.maxToleratedMissCount = checkMaxToleratedMissCount(properties);
        this.metaDataFetcher = metaDataFetcher;
        this.executionService = executionService;
        this.partitionService = partitionService;
        this.partitionUuids = new AtomicReferenceArray<UUID>(partitionCount);
        this.localUuid = localUuid;
    }

    protected int checkMaxToleratedMissCount(HazelcastProperties properties) {
        int maxToleratedMissCount = properties.getInteger(MAX_TOLERATED_MISS_COUNT);
        return checkNotNegative(maxToleratedMissCount,
                format("max-tolerated-miss-count cannot be < 0 but found %d", maxToleratedMissCount));
    }

    private int checkAndGetReconciliationIntervalSeconds(HazelcastProperties properties) {
        int reconciliationIntervalSeconds = properties.getInteger(RECONCILIATION_INTERVAL_SECONDS);
        if (reconciliationIntervalSeconds < 0
                || reconciliationIntervalSeconds > 0L && reconciliationIntervalSeconds < MIN_RECONCILIATION_INTERVAL_SECONDS) {
            String msg = format("Reconciliation interval can be at least %d seconds if it is not zero but found %d. "
                            + "Note that giving zero disables reconciliation task.",
                    MIN_RECONCILIATION_INTERVAL_SECONDS, reconciliationIntervalSeconds);

            throw new IllegalArgumentException(msg);
        }

        return reconciliationIntervalSeconds;
    }

    @Override
    public void run() {
        try {
            fixSequenceGaps();
            runAntiEntropyIfNeeded();
        } finally {
            if (running.get()) {
                scheduleNextRun();
            }
        }
    }

    /**
     * Marks relevant data as stale if missed invalidation event count is above the max tolerated miss count.
     */
    private void fixSequenceGaps() {
        for (RepairingHandler handler : handlers.values()) {
            if (isAboveMaxToleratedMissCount(handler)) {
                updateLastKnownStaleSequences(handler);
            }
        }
    }

    /**
     * Periodically sends generic operations to cluster members to get latest invalidation metadata.
     */
    private void runAntiEntropyIfNeeded() {
        if (reconciliationIntervalNanos == 0) {
            return;
        }

        long sinceLastRun = nanoTime() - lastAntiEntropyRunNanos;
        if (sinceLastRun >= reconciliationIntervalNanos) {
            metaDataFetcher.fetchMetadata(handlers);
            lastAntiEntropyRunNanos = nanoTime();
        }
    }

    private void scheduleNextRun() {
        try {
            executionService.schedule(this, 1, SECONDS);
        } catch (RejectedExecutionException e) {
            if (logger.isFinestEnabled()) {
                logger.finest(e.getMessage());
            }
        }
    }

    public <K, V> RepairingHandler registerAndGetHandler(String name, NearCache<K, V> nearCache) {
        boolean started = running.compareAndSet(false, true);
        if (started) {
            assignAndGetUuids();
        }

        RepairingHandler repairingHandler = new RepairingHandler(name, nearCache, partitionService, localUuid, logger);
        repairingHandler.initUnknownUuids(partitionUuids);

        StaleReadDetector staleReadDetector = new StaleReadDetectorImpl(repairingHandler, partitionService);
        nearCache.unwrap(DefaultNearCache.class).getNearCacheRecordStore().setStaleReadDetector(staleReadDetector);

        handlers.put(name, repairingHandler);

        if (started) {
            scheduleNextRun();
            lastAntiEntropyRunNanos = nanoTime();
        }

        return repairingHandler;
    }

    public void deregisterHandler(String mapName) {
        handlers.remove(mapName);
    }

    /**
     * Makes initial population of partition uuids synchronously.
     * <p>
     * This operation can be done only one time per service (e.g. MapService, CacheService) on an end (e.g. client, member)
     * when registering first {@link RepairingHandler} for a service. For example, if there is 100 near-caches on a client,
     * this operation will be done only one time.
     */
    private void assignAndGetUuids() {
        logger.finest("Making initial population of partition uuids");

        boolean initialized = false;
        try {
            List<Object> objects = metaDataFetcher.assignAndGetUuids();
            for (int i = 0; i < objects.size(); ) {
                Integer partition = (Integer) objects.get(i++);
                UUID uuid = ((UUID) objects.get(i++));
                partitionUuids.set(partition, uuid);

                if (logger.isFinestEnabled()) {
                    logger.finest(partition + "-" + uuid);
                }
            }
            initialized = true;
        } catch (Exception e) {
            logger.warning(e);
        } finally {
            if (!initialized) {
                assignAndGetUuidsAsync();
            }
        }
    }

    /**
     * Makes initial population of partition uuids asynchronously.
     * This is the fallback operation when {@link #assignAndGetUuids} is failed.
     */
    private void assignAndGetUuidsAsync() {
        executionService.schedule(new Runnable() {
            private final AtomicInteger round = new AtomicInteger();

            @Override
            public void run() {
                int roundNumber = round.incrementAndGet();
                boolean initialized = false;
                try {
                    assignAndGetUuids();
                    for (RepairingHandler repairingHandler : handlers.values()) {
                        repairingHandler.initUnknownUuids(partitionUuids);
                    }
                    initialized = true;
                } catch (Exception e) {
                    if (logger.isFinestEnabled()) {
                        logger.finest(e);
                    }
                } finally {
                    if (!initialized) {
                        long delay = roundNumber * GET_UUID_TASK_SCHEDULE_MILLIS;
                        if (delay > HALF_MINUTE_MILLIS) {
                            round.set(0);
                        }

                        executionService.schedule(this, delay, MILLISECONDS);
                    }
                }
            }
        }, GET_UUID_TASK_SCHEDULE_MILLIS, MILLISECONDS);
    }

    /**
     * Calculates number of missed invalidations and checks if repair is needed for the supplied handler.
     * Every handler represents a single near-cache.
     */
    private boolean isAboveMaxToleratedMissCount(RepairingHandler handler) {
        int partition = 0;
        long missCount = 0;

        do {
            MetaDataContainer metaData = handler.getMetaDataContainer(partition);
            missCount += metaData.getMissedSequenceCount();

            if (missCount > maxToleratedMissCount) {
                if (logger.isFinestEnabled()) {
                    logger.finest(format("%s:[map=%s,missCount=%d,maxToleratedMissCount=%d]",
                            "Above tolerated miss count", handler.getName(), missCount, maxToleratedMissCount));
                }
                return true;
            }
        } while (++partition < partitionCount);

        return false;
    }

    private void updateLastKnownStaleSequences(RepairingHandler handler) {
        for (int partition = 0; partition < partitionCount; partition++) {
            MetaDataContainer metaData = handler.getMetaDataContainer(partition);
            long missCount = metaData.getMissedSequenceCount();
            if (missCount != 0) {
                metaData.addAndGetMissedSequenceCount(-missCount);
                handler.updateLastKnownStaleSequence(metaData, partition);
            }
        }
    }

    @Override
    public String toString() {
        return "RepairingTask{}";
    }
}
