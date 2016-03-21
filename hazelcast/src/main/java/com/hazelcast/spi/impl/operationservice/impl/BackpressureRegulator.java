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

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.internal.util.ThreadLocalRandom;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.spi.properties.HazelcastProperties;

import java.util.Random;

import static com.hazelcast.nio.Bits.CACHE_LINE_LENGTH;
import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.spi.properties.GroupProperty.BACKPRESSURE_BACKOFF_TIMEOUT_MILLIS;
import static com.hazelcast.spi.properties.GroupProperty.BACKPRESSURE_MAX_CONCURRENT_INVOCATIONS_PER_PARTITION;
import static com.hazelcast.spi.properties.GroupProperty.BACKPRESSURE_SYNCWINDOW;
import static java.lang.Math.max;
import static java.lang.Math.round;

/**
 * The BackpressureRegulator is responsible for regulating invocation 'pressure'. If it sees that the system
 * is getting overloaded, it will apply back pressure so the the system won't crash.
 * <p/>
 * The BackpressureRegulator is responsible for regulating invocation pressure on the Hazelcast system to prevent it from
 * crashing on overload. Most Hazelcast invocations on Hazelcast are simple; you do (for example) a map.get and you wait for the
 * response (synchronous call) so you won't get more requests than you have threads.
 * <p/>
 * But if there is no balance between the number of invocations and the number of threads, then it is very easy to produce
 * more invocations that the system can handle. To prevent the system crashing under overload, back pressure is applied
 * so that the invocation pressure is bound to a certain maximum and can't lead to the system crashing.
 * <p/>
 * The BackpressureRegulator needs to be hooked into 2 parts:
 * <ol>
 * <li>when a new invocation is about to be made. If there are too many requests, then the invocation is delayed
 * until there is space or eventually a timeout happens and the {@link com.hazelcast.core.HazelcastOverloadException}
 * is thrown.
 * </li>
 * <li>
 * when asynchronous backups are made. In this case, we rely on periodically making the async backups sync. By
 * doing this, we force the invocation to wait for operation queues to drain and this prevents them from getting
 * overloaded.
 * </li>
 * </ol>
 */
public class BackpressureRegulator {

    /**
     * The percentage above and below a certain sync-window we should randomize.
     */
    static final float RANGE = 0.25f;

    // number of ints in a single cache-line.
    private static final int INTS_PER_CACHE_LINE = CACHE_LINE_LENGTH / INT_SIZE_IN_BYTES;

    // There is syncDelay counter per partition. To prevent false sharing, the syncDelay for each partition is padded additional
    // int's so that each sync delay is on its own cache-line.
    // Each partition will always be handled by 1 thread at any given moment, so thread-safety is not required.
    private final int[] syncDelays;

    private final boolean enabled;
    private final boolean disabled;
    private final int syncWindow;
    private final int partitionCount;
    private final int maxConcurrentInvocations;
    private final int backoffTimeoutMs;

    public BackpressureRegulator(HazelcastProperties properties, ILogger logger) {
        this.enabled = properties.getBoolean(GroupProperty.BACKPRESSURE_ENABLED);
        this.disabled = !enabled;
        this.partitionCount = properties.getInteger(GroupProperty.PARTITION_COUNT);
        this.syncWindow = getSyncWindow(properties);
        this.maxConcurrentInvocations = getMaxConcurrentInvocations(properties);
        this.backoffTimeoutMs = getBackoffTimeoutMs(properties);

        this.syncDelays = new int[INTS_PER_CACHE_LINE * partitionCount];
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            syncDelays[partitionId * INTS_PER_CACHE_LINE] = randomSyncDelay();
        }

        if (enabled) {
            logger.info("Backpressure is enabled"
                    + ", maxConcurrentInvocations:" + maxConcurrentInvocations
                    + ", syncWindow: " + syncWindow);
        } else {
            logger.info("Backpressure is disabled");
        }
    }

    private int getSyncWindow(HazelcastProperties props) {
        int syncWindow = props.getInteger(BACKPRESSURE_SYNCWINDOW);
        if (enabled && syncWindow <= 0) {
            throw new IllegalArgumentException("Can't have '" + BACKPRESSURE_SYNCWINDOW + "' with a value smaller than 1");
        }
        return syncWindow;
    }

    private int getBackoffTimeoutMs(HazelcastProperties props) {
        int backoffTimeoutMs = (int) props.getMillis(BACKPRESSURE_BACKOFF_TIMEOUT_MILLIS);
        if (enabled && backoffTimeoutMs < 0) {
            throw new IllegalArgumentException("Can't have '" + BACKPRESSURE_BACKOFF_TIMEOUT_MILLIS
                    + "' with a value smaller than 0");
        }
        return backoffTimeoutMs;
    }

    private int getMaxConcurrentInvocations(HazelcastProperties props) {
        int invocationsPerPartition = props.getInteger(BACKPRESSURE_MAX_CONCURRENT_INVOCATIONS_PER_PARTITION);
        if (invocationsPerPartition < 1) {
            throw new IllegalArgumentException("Can't have '" + BACKPRESSURE_MAX_CONCURRENT_INVOCATIONS_PER_PARTITION
                    + "' with a value smaller than 1");
        }

        return (partitionCount + 1) * invocationsPerPartition;
    }

    /**
     * Checks if back-pressure is enabled.
     * <p/>
     * This method is only used for testing.
     */
    boolean isEnabled() {
        return enabled;
    }

    // just for testing
    int syncDelay(Operation op) {
        return syncDelays[op.getPartitionId() * INTS_PER_CACHE_LINE];
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
     * @param backupAwareOp The BackupAwareOperation to check.
     * @return true if a sync needs to be forced, false otherwise.
     */
    public boolean isSyncForced(BackupAwareOperation backupAwareOp) {
        if (disabled) {
            return false;
        }

        if (backupAwareOp.getPartitionId() < 0) {
            throw new IllegalArgumentException("A BackupAwareOperation can't have a negative partitionId, "
                    + backupAwareOp);
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

        int index = op.getPartitionId() * INTS_PER_CACHE_LINE;
        int oldSyncDelay = syncDelays[index];

        if (oldSyncDelay == 1) {
            int newSyncDelay = randomSyncDelay();
            syncDelays[index] = newSyncDelay;
            return true;
        }

        syncDelays[index] = oldSyncDelay - 1;
        return false;
    }

    private int randomSyncDelay() {
        if (syncWindow == 1) {
            return 1;
        }

        Random random = ThreadLocalRandom.current();
        int randomSyncWindow = round((1 - RANGE) * syncWindow + random.nextInt(round(2 * RANGE * syncWindow)));
        return max(1, randomSyncWindow);
    }
}
