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

package com.hazelcast.internal.nearcache.impl.invalidation;

import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.internal.nearcache.impl.DefaultNearCache;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.TaskScheduler;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.ConstructorFunction;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.util.ConcurrencyUtil.getOrPutIfAbsent;
import static com.hazelcast.util.Preconditions.checkNotNegative;
import static java.lang.String.format;
import static java.lang.System.nanoTime;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * This task runs on Near Cache side and only one instance is created per data-structure type like IMap and ICache.
 * Repairing responsibilities of this task are:
 * <ul>
 * <li>
 * To scan {@link RepairingHandler}s to see if any Near Cache needs to be invalidated
 * according to missed invalidation counts. Controlled via {@link RepairingTask#MAX_TOLERATED_MISS_COUNT}
 * </li>
 * <li>
 * To send periodic generic-operations to cluster members in order to fetch latest partition sequences and UUIDs.
 * Controlled via {@link RepairingTask#MIN_RECONCILIATION_INTERVAL_SECONDS}
 * </li>
 * </ul>
 */
public final class RepairingTask implements Runnable {

    static final HazelcastProperty MAX_TOLERATED_MISS_COUNT
            = new HazelcastProperty("hazelcast.invalidation.max.tolerated.miss.count", 10);
    static final HazelcastProperty RECONCILIATION_INTERVAL_SECONDS
            = new HazelcastProperty("hazelcast.invalidation.reconciliation.interval.seconds", 60, SECONDS);
    // only used for testing
    static final HazelcastProperty MIN_RECONCILIATION_INTERVAL_SECONDS
            = new HazelcastProperty("hazelcast.invalidation.min.reconciliation.interval.seconds", 30, SECONDS);

    static final long RESCHEDULE_FAILED_INITIALIZATION_AFTER_MILLIS = 500;

    final int maxToleratedMissCount;
    final long reconciliationIntervalNanos;

    private final int partitionCount;
    private final String localUuid;
    private final ILogger logger;
    private final TaskScheduler scheduler;
    private final MetaDataFetcher metaDataFetcher;
    private final SerializationService serializationService;
    private final MinimalPartitionService partitionService;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ConcurrentMap<String, RepairingHandler> handlers = new ConcurrentHashMap<String, RepairingHandler>();

    private volatile long lastAntiEntropyRunNanos;

    public RepairingTask(HazelcastProperties properties, MetaDataFetcher metaDataFetcher, TaskScheduler scheduler,
                         SerializationService serializationService, MinimalPartitionService partitionService, String localUuid,
                         ILogger logger) {
        this.reconciliationIntervalNanos = SECONDS.toNanos(getReconciliationIntervalSeconds(properties));
        this.maxToleratedMissCount = getMaxToleratedMissCount(properties);
        this.metaDataFetcher = metaDataFetcher;
        this.scheduler = scheduler;
        this.serializationService = serializationService;
        this.partitionService = partitionService;
        this.partitionCount = partitionService.getPartitionCount();
        this.localUuid = localUuid;
        this.logger = logger;
    }

    private int getMaxToleratedMissCount(HazelcastProperties properties) {
        int maxToleratedMissCount = properties.getInteger(MAX_TOLERATED_MISS_COUNT);
        return checkNotNegative(maxToleratedMissCount,
                format("max-tolerated-miss-count cannot be < 0 but found %d", maxToleratedMissCount));
    }

    private int getReconciliationIntervalSeconds(HazelcastProperties properties) {
        int reconciliationIntervalSeconds = properties.getInteger(RECONCILIATION_INTERVAL_SECONDS);
        int minReconciliationIntervalSeconds = properties.getInteger(MIN_RECONCILIATION_INTERVAL_SECONDS);
        if (reconciliationIntervalSeconds < 0
                || reconciliationIntervalSeconds > 0 && reconciliationIntervalSeconds < minReconciliationIntervalSeconds) {
            String msg = format("Reconciliation interval can be at least %s seconds if it is not zero, but %d was configured."
                            + " Note: Configuring a value of zero seconds disables the reconciliation task.",
                    MIN_RECONCILIATION_INTERVAL_SECONDS.getDefaultValue(), reconciliationIntervalSeconds);
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
            scheduler.schedule(this, 1, SECONDS);
        } catch (RejectedExecutionException e) {
            if (logger.isFinestEnabled()) {
                logger.finest(e.getMessage());
            }
        }
    }

    private class HandlerConstructor<K, V> implements ConstructorFunction<String, RepairingHandler> {

        private final NearCache<K, V> nearCache;

        public HandlerConstructor(NearCache nearCache) {
            this.nearCache = nearCache;
        }

        @Override
        public RepairingHandler createNew(String dataStructureName) {
            RepairingHandler handler = new RepairingHandler(logger, localUuid, dataStructureName,
                    nearCache, serializationService, partitionService);
            StaleReadDetector staleReadDetector = new StaleReadDetectorImpl(handler, partitionService);
            nearCache.unwrap(DefaultNearCache.class).getNearCacheRecordStore().setStaleReadDetector(staleReadDetector);

            initRepairingHandler(handler);

            return handler;
        }
    }

    public <K, V> RepairingHandler registerAndGetHandler(String dataStructureName, NearCache<K, V> nearCache) {
        RepairingHandler handler = getOrPutIfAbsent(handlers, dataStructureName, new HandlerConstructor(nearCache));

        if (running.compareAndSet(false, true)) {
            scheduleNextRun();
            lastAntiEntropyRunNanos = nanoTime();
        }

        return handler;
    }

    public void deregisterHandler(String dataStructureName) {
        handlers.remove(dataStructureName);
    }

    /**
     * Synchronously makes initial population of partition uuids & sequences.
     * This initialization is done for every near-cached data structure.
     */
    private void initRepairingHandler(RepairingHandler handler) {
        logger.finest("Initializing repairing handler");

        boolean initialized = false;
        try {
            metaDataFetcher.init(handler);
            initialized = true;
        } catch (Exception e) {
            logger.warning(e);
        } finally {
            if (!initialized) {
                initRepairingHandlerAsync(handler);
            }
        }
    }

    /**
     * Asynchronously makes initial population of partition uuids & sequences.
     * This is the fallback operation when {@link #initRepairingHandler} is failed.
     */
    private void initRepairingHandlerAsync(final RepairingHandler handler) {
        scheduler.schedule(new Runnable() {
            private final AtomicInteger round = new AtomicInteger();

            @Override
            public void run() {
                int roundNumber = round.incrementAndGet();
                boolean initialized = false;
                try {
                    initRepairingHandler(handler);
                    initialized = true;
                } catch (Exception e) {
                    if (logger.isFinestEnabled()) {
                        logger.finest(e);
                    }
                } finally {
                    if (!initialized) {
                        long totalDelaySoFarNanos = totalDelaySoFarNanos(roundNumber);
                        if (reconciliationIntervalNanos > totalDelaySoFarNanos) {
                            long delay = roundNumber * RESCHEDULE_FAILED_INITIALIZATION_AFTER_MILLIS;
                            scheduler.schedule(this, delay, MILLISECONDS);
                        }
                        // else don't reschedule this task again and fallback to anti-entropy (see #runAntiEntropyIfNeeded)
                        // if we haven't managed to initialize repairing handler so far.
                    }
                }
            }
        }, RESCHEDULE_FAILED_INITIALIZATION_AFTER_MILLIS, MILLISECONDS);
    }

    private static long totalDelaySoFarNanos(int roundNumber) {
        long totalDelayMillis = 0;
        for (int i = 1; i < roundNumber; i++) {
            totalDelayMillis += roundNumber * RESCHEDULE_FAILED_INITIALIZATION_AFTER_MILLIS;
        }
        return MILLISECONDS.toNanos(totalDelayMillis);
    }

    /**
     * Calculates number of missed invalidations and checks if repair is needed for the supplied handler.
     * Every handler represents a single Near Cache.
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

    // used in tests.
    public MetaDataFetcher getMetaDataFetcher() {
        return metaDataFetcher;
    }

    // used in tests.
    public ConcurrentMap<String, RepairingHandler> getHandlers() {
        return handlers;
    }

    @Override
    public String toString() {
        return "RepairingTask{}";
    }
}
