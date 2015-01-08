package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.client.impl.client.PartitionClientRequest;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.Operation;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


/**
 * A service responsible for figuring out if back pressure needs to be applied to a certain operation.
 * <p/>
 * The current implementation only applies back-pressure on async backup operations of synchronous
 * operation. E.g. the map.put with a async-backup. It does not apply back-pressure for regular
 * async operations or async operations with an async backup. This probably will be added in HZ 3.4.1
 * <p/>
 * For information about the implementation see:
 * https://hazelcast.atlassian.net/wiki/display/EN/Back+Pressure+Design
 */
public class BackPressureService {

    /**
     * The percentage above and below a certain sync-window we should randomize.
     */
    static final float RANGE = 0.25f;

    // A thread-local containing a random. Unfortunately we need to support Java 6 and there is no
    // ThreadLocalRandom. Once we move to Java 7, we can make use of that class.
    private static final ThreadLocal<Random> THREAD_LOCAL_RANDOM = new ThreadLocal<Random>() {
        @Override
        protected Random initialValue() {
            return new Random();
        }
    };

    private final AtomicInteger[] syncDelays;
    private final boolean backPressureEnabled;
    private final int syncWindow;
    private final int partitionCount;

    // Although this counter is shared between all threads, it isn't very contended because only 1 in every 'syncWindow' this
    // counter is being incremented.
    private final AtomicLong backPressureCounter = new AtomicLong();

    public BackPressureService(GroupProperties properties, ILogger logger) {
        this.backPressureEnabled = properties.BACKPRESSURE_ENABLED.getBoolean();
        this.syncWindow = getSyncWindow(properties);
        this.partitionCount = properties.PARTITION_COUNT.getInteger();

        if (backPressureEnabled) {
            logger.info("Backpressure is enabled, syncWindow is " + syncWindow);
            this.syncDelays = newSyncDelays();
        } else {
            logger.info("Backpressure is disabled");
            this.syncDelays = null;
        }
    }

    private AtomicInteger[] newSyncDelays() {
        int length = partitionCount + 1;
        AtomicInteger[] syncDelays = new AtomicInteger[length];
        for (int k = 0; k < length; k++) {
            syncDelays[k] = new AtomicInteger();
        }
        return syncDelays;
    }

    private int getSyncWindow(GroupProperties properties) {
        int syncWindow = properties.BACKPRESSURE_SYNCWINDOW.getInteger();
        if (backPressureEnabled && syncWindow < 1) {
            throw new IllegalArgumentException(""
                    + properties.BACKPRESSURE_SYNCWINDOW.getName() + " can't be smaller than 1. Found " + syncWindow);
        }
        return syncWindow;
    }

    /**
     * Checks if back-pressure is enabled.
     * <p/>
     * This method is only used for testing.
     */
    boolean isBackPressureEnabled() {
        return backPressureEnabled;
    }

    /**
     * Returns the number of times back pressure has been applied.
     *
     * @return number of times backpressure is applied.
     */
    public long backPressureCount() {
        return backPressureCounter.get();
    }

    /**
     * Gets the current sync delays for a given connection/partition.
     * <p/>
     * This method is only used for testing.
     */
    AtomicInteger getSyncDelay(int partitionId) {
        if (!backPressureEnabled) {
            return null;
        }
        partitionId = partitionId == -1 ? partitionCount : partitionId;
        return syncDelays[partitionId];
    }

    /**
     * Checks if back pressure is required.
     *
     * @param op the Operation to check
     * @return true of back-pressure is required.
     */
    public boolean requiresBackPressure(Operation op) {
        if (!backPressureAllowed(op)) {
            return false;
        }

        AtomicInteger syncDelay = getSyncDelay(op);
        for (; ; ) {
            int prev = syncDelay.get();
            if (prev > 0) {
                int next = prev - 1;
                if (syncDelay.compareAndSet(prev, next)) {
                    return false;
                }
            }

            int next = calcSyncDelay();
            if (syncDelay.compareAndSet(prev, next)) {
                backPressureCounter.incrementAndGet();
                return true;
            }
        }
    }

    private boolean backPressureAllowed(Operation op) {
        if (!backPressureEnabled) {
            return false;
        }

        // Clients can't deal with back pressure, so lets disable it when we see the operation is send by a client.
        if (op.getResponseHandler() instanceof PartitionClientRequest.CallbackImpl) {
            return false;
        }

        // we never apply back-pressure on urgent operations.
        if (op.isUrgent()) {
            return false;
        }
        return true;
    }

    private AtomicInteger getSyncDelay(Operation op) {
        int partitionId = op.getPartitionId();
        if (partitionId < 0) {
            return syncDelays[partitionCount];
        } else {
            return syncDelays[partitionId];
        }
    }

    private int calcSyncDelay() {
        Random random = THREAD_LOCAL_RANDOM.get();
        return Math.round((1 - RANGE) * syncWindow + random.nextInt(Math.round(2 * RANGE * syncWindow)));
    }
}
