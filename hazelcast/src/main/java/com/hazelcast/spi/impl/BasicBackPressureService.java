package com.hazelcast.spi.impl;

import com.hazelcast.instance.GroupProperties;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.Operation;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;


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
public class BasicBackPressureService {

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

    // The key is either a connection instance (remote executed operations) or 'this'. This is needed because there is
    // no local connection.
    // The value is an array of AtomicInteger with a length of partitioncount+1. The last item is for generic-operations.
    private final ConcurrentMap<Object, AtomicInteger[]> syncDelaysPerConnection
            = new ConcurrentHashMap<Object, AtomicInteger[]>();
    private final boolean backPressureEnabled;
    private final int syncWindow;
    private final int partitionCount;

    public BasicBackPressureService(GroupProperties properties, ILogger logger) {
        this.backPressureEnabled = properties.BACKPRESSURE_ENABLED.getBoolean();
        this.partitionCount = properties.PARTITION_COUNT.getInteger();
        this.syncWindow = getSyncWindow(properties);

        if (backPressureEnabled) {
            logger.info("Backpressure is enabled, syncWindow is " + syncWindow);
        } else {
            logger.info("Backpressure is disabled");
        }
    }

    private int getSyncWindow(GroupProperties properties) {
        int syncWindow = properties.BACKPRESSURE_SYNCWINDOW.getInteger();
        if (backPressureEnabled && syncWindow < 1) {
            throw new IllegalArgumentException("Can't have '"
                    + properties.BACKPRESSURE_SYNCWINDOW.getName() + "' with a value smaller than 1");
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
     * Gets the array of sync-delays for every partition.
     * <p/>
     * This method is only used for testing.
     */
    AtomicInteger[] getSyncDelays(Connection connection) {
        Object key = connection == null ? this : connection;
        return syncDelaysPerConnection.get(key);
    }

    /**
     * Gets the current sync delays for a given connection/partition.
     * <p/>
     * This method is only used for testing.
     */
    AtomicInteger getSyncDelay(Connection connection, int partitionId) {
        Object key = connection == null ? this : connection;
        AtomicInteger[] syncDelays = syncDelaysPerConnection.get(key);
        if (syncDelays == null) {
            return null;
        }
        partitionId = partitionId == -1 ? partitionCount : partitionId;
        return syncDelays[partitionId];
    }

    /**
     * Checks if back pressure is needed.
     *
     * @param op
     * @return
     */
    public boolean isBackPressureNeeded(Operation op) {
        if (!backPressureEnabled) {
            return false;
        }

        // we never apply back-pressure on urgent operations.
        if (op.isUrgent()) {
            return false;
        }

        AtomicInteger syncDelay = getSyncDelay(op);

        int currentSyncDelay = syncDelay.get();

        if (currentSyncDelay > 0) {
            syncDelay.decrementAndGet();
            return false;
        }

        syncDelay.set(calcSyncDelay());
        return true;
    }

    private AtomicInteger getSyncDelay(Operation op) {
        Object key = getConnectionKey(op);

        AtomicInteger[] syncDelayPerPartition;
        syncDelayPerPartition = syncDelaysPerConnection.get(key);
        if (syncDelayPerPartition == null) {
            AtomicInteger[] newSyncDelayPerPartition = new AtomicInteger[partitionCount + 1];
            for (int k = 0; k < newSyncDelayPerPartition.length; k++) {
                newSyncDelayPerPartition[k] = new AtomicInteger(syncWindow);
            }
            AtomicInteger[] found = syncDelaysPerConnection.putIfAbsent(key, newSyncDelayPerPartition);
            syncDelayPerPartition = found != null ? found : newSyncDelayPerPartition;
        }

        int partitionId = op.getPartitionId();
        if (partitionId < 0) {
            return syncDelayPerPartition[partitionCount];
        } else {
            return syncDelayPerPartition[partitionId];
        }
    }

    private Object getConnectionKey(Operation op) {
        Connection connection = op.getConnection();
        return connection == null ? this : connection;
    }

    /**
     * Cleans up all sync delay administration for dead connections. Without this cleanup, eventually the system could
     * run into an OOME.
     */
    public void cleanup() {
        if (!backPressureEnabled) {
            return;
        }

        for (Map.Entry<Object, AtomicInteger[]> entry : syncDelaysPerConnection.entrySet()) {
            Object key = entry.getKey();
            if (!(key instanceof Connection)) {
                continue;
            }

            Connection connection = (Connection) key;
            if (!connection.isAlive()) {
                syncDelaysPerConnection.remove(key);
            }
        }
    }

    private int calcSyncDelay() {
        Random random = THREAD_LOCAL_RANDOM.get();
        return Math.round((1 - RANGE) * syncWindow + random.nextInt(Math.round(2 * RANGE * syncWindow)));
    }

}
