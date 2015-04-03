package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.instance.GroupProperties;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Math.round;


/**
 * The BackpressureRegulator is responsible for regulating invocation 'pressure'. If sees that the system
 * is getting overloaded, it will apply back pressure, so the the system won't crash.
 * <p/>
 * The BackpressureRegulator is responsible for regulating invocation pressure on the Hazelcast system to prevent it from
 * crashing on overload. Most Hazelcast invocations on Hazelcast are simple; you do e.g. a map.get and you wait for the
 * response (synchronous call) So won't get more requests than you have threads.
 * <p/>
 * But if there is no balance between the number of invocations and the number of threads, then it is very easy to produce
 * more invocations that the system can handle. To prevent the system crashing under overload, back pressure is applied
 * so that the invocation pressure is bound to a certain maximum and can't lead to the system crashing.
 * <p/>
 * The BackpressureRegulator needs to be hooked into 2 parts:
 * <ol>
 * <li>when a new invocation is about to be made; if there are too many requests, then the invocation is delayed
 * until there is space or eventually a timeout happens and the {@link com.hazelcast.core.HazelcastOverloadException}
 * is thrown.
 * </li>
 * <li>
 * when asynchronous backups are made. In this case we rely no periodically making the async backups sync. By
 * doing this we force the invocation to wait for operation queues to drain and this prevent them from getting
 * overloaded.
 * </li>
 * </ol>
 */
public class BackpressureRegulator {

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

    // we have a counter per partition + we have an additional counter for generic operations.
    private final AtomicInteger[] syncDelays;

    private final boolean enabled;
    private final boolean disabled;
    private final int syncWindow;
    private final int partitionCount;
    private final ILogger logger;
    private final int maxConcurrentInvocations;
    private final int backoffTimeoutMs;

    public BackpressureRegulator(GroupProperties properties, ILogger logger) {
        this.enabled = properties.BACKPRESSURE_ENABLED.getBoolean();
        this.disabled = !enabled;
        this.logger = logger;
        this.partitionCount = properties.PARTITION_COUNT.getInteger();
        this.syncWindow = getSyncWindow(properties);
        this.maxConcurrentInvocations = getMaxConcurrentInvocations(properties);
        this.backoffTimeoutMs = getBackoffTimeoutMs(properties);
        this.syncDelays = new AtomicInteger[partitionCount + 1];
        for (int k = 0; k < syncDelays.length; k++) {
            syncDelays[k] = new AtomicInteger(randomSyncDelay());
        }

        if (enabled) {
            logger.info("Backpressure is enabled"
                    + ", maxConcurrentInvocations:" + maxConcurrentInvocations
                    + ", syncWindow: " + syncWindow);
        } else {
            logger.info("Backpressure is disabled");
        }
    }

    private int getSyncWindow(GroupProperties props) {
        int syncWindow = props.BACKPRESSURE_SYNCWINDOW.getInteger();
        if (enabled && syncWindow < 0) {
            throw new IllegalArgumentException("Can't have '"
                    + props.BACKPRESSURE_SYNCWINDOW.getName() + "' with a value smaller than 0");
        }
        return syncWindow;
    }

    private int getBackoffTimeoutMs(GroupProperties props) {
        int backoffTimeoutMs = props.BACKPRESSURE_BACKOFF_TIMEOUT_MILLIS.getInteger();
        if (enabled && backoffTimeoutMs < 0) {
            throw new IllegalArgumentException("Can't have '"
                    + props.BACKPRESSURE_BACKOFF_TIMEOUT_MILLIS.getName() + "' with a value smaller than 0");
        }
        return backoffTimeoutMs;
    }

    private int getMaxConcurrentInvocations(GroupProperties props) {
        GroupProperties.GroupProperty invocationsPerPartitionProp = props.BACKPRESSURE_MAX_CONCURRENT_INVOCATIONS_PER_PARTITION;

        int invocationsPerPartition = invocationsPerPartitionProp.getInteger();
        if (invocationsPerPartition < 1) {
            throw new IllegalArgumentException("Can't have '"
                    + invocationsPerPartitionProp.getName() + "' with a value smaller than 1");
        }

        return (props.PARTITION_COUNT.getInteger() + 1) * invocationsPerPartition;
    }

    /**
     * Checks if back-pressure is enabled.
     * <p/>
     * This method is only used for testing.
     */
    boolean isEnabled() {
        return enabled;
    }

    int getMaxConcurrentInvocations() {
        if (enabled) {
            return maxConcurrentInvocations;
        } else {
            return Integer.MAX_VALUE;
        }
    }

    public CallIdSequence newCallIdSequence() {
        if (enabled) {
            return new CallIdSequence.CallIdSequenceWithBackpressure(maxConcurrentInvocations, backoffTimeoutMs);
        } else {
            return new CallIdSequence.CallIdSequenceWithoutBackpressure();
        }
    }

    /**
     * Checks if a sync is forced for the given BackupAwareOperation.
     * <p/>
     * Once and a while for every BackupAwareOperation with one or more async backups, these async backups are transformed
     * into a sync backup.
     *
     * For {@link com.hazelcast.spi.UrgentSystemOperation} no sync will be forced.
     *
     * @param backupAwareOp the BackupAwareOperation to check
     * @return true if a sync needs to be forced, false otherwise.
     */
    public boolean isSyncForced(BackupAwareOperation backupAwareOp) {
        if (disabled) {
            return false;
        }

        // if there are no asynchronous backups, there is nothing to regulate.
        if (backupAwareOp.getAsyncBackupCount() == 0) {
            return false;
        }

        Operation op = (Operation) backupAwareOp;

        // we never apply back-pressure on urgent operations.
        if (op.isUrgent()) {
            return false;
        }

        AtomicInteger syncDelayCounter = syncDelayCounter(op);

        for (; ; ) {
            int prev = syncDelayCounter.get();
            int next;

            boolean syncForced;
            if (prev > 0) {
                next = prev - 1;
                syncForced = false;
            } else {
                next = randomSyncDelay();
                syncForced = true;
            }

            if (syncDelayCounter.compareAndSet(prev, next)) {
                return syncForced;
            }
        }
    }

    AtomicInteger syncDelayCounter(Operation op) {
        int partitionId = op.getPartitionId();
        if (partitionId < 0) {
            return syncDelays[partitionCount];
        } else {
            return syncDelays[partitionId];
        }
    }

    // just for testing
    int syncDelay(Operation op) {
        return syncDelayCounter(op).get();
    }

    private int randomSyncDelay() {
        if (syncWindow == 0) {
            return 0;
        }

        Random random = THREAD_LOCAL_RANDOM.get();
        return round((1 - RANGE) * syncWindow + random.nextInt(round(2 * RANGE * syncWindow)));
    }
}
