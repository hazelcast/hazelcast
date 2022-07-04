/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.util.ConcurrencyDetection;
import com.hazelcast.internal.util.ThreadLocalRandomProvider;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.operationservice.BackupAwareOperation;
import com.hazelcast.spi.impl.operationservice.UrgentSystemOperation;
import com.hazelcast.spi.impl.sequence.CallIdFactory;
import com.hazelcast.spi.impl.sequence.CallIdSequence;
import com.hazelcast.spi.properties.HazelcastProperties;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.spi.properties.ClusterProperty.BACKPRESSURE_BACKOFF_TIMEOUT_MILLIS;
import static com.hazelcast.spi.properties.ClusterProperty.BACKPRESSURE_ENABLED;
import static com.hazelcast.spi.properties.ClusterProperty.BACKPRESSURE_MAX_CONCURRENT_INVOCATIONS_PER_PARTITION;
import static com.hazelcast.spi.properties.ClusterProperty.BACKPRESSURE_SYNCWINDOW;
import static com.hazelcast.spi.properties.ClusterProperty.OPERATION_BACKUP_TIMEOUT_MILLIS;
import static com.hazelcast.spi.properties.ClusterProperty.PARTITION_COUNT;
import static java.lang.Math.max;
import static java.lang.Math.round;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * The BackpressureRegulator is responsible for regulating invocation 'pressure'. If it sees that the system
 * is getting overloaded, it will apply back pressure so the the system won't crash.
 * <p>
 * The BackpressureRegulator is responsible for regulating invocation pressure on the Hazelcast system to prevent it from
 * crashing on overload. Most Hazelcast invocations on Hazelcast are simple; you do (for example) a map.get and you wait for the
 * response (synchronous call) so you won't get more requests than you have threads.
 * <p>
 * But if there is no balance between the number of invocations and the number of threads, then it is very easy to produce
 * more invocations that the system can handle. To prevent the system crashing under overload, back pressure is applied
 * so that the invocation pressure is bound to a certain maximum and can't lead to the system crashing.
 * <p>
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
class BackpressureRegulator {

    /**
     * The percentage above and below a certain sync-window we should randomize.
     */
    static final float RANGE = 0.25f;

    private final AtomicInteger syncCountdown = new AtomicInteger();
    private final boolean enabled;
    private final boolean disabled;
    private final int syncWindow;
    private final int partitionCount;
    private final int maxConcurrentInvocations;
    private final int backoffTimeoutMs;

    BackpressureRegulator(HazelcastProperties properties, ILogger logger) {
        this.enabled = properties.getBoolean(BACKPRESSURE_ENABLED);
        this.disabled = !enabled;
        this.partitionCount = properties.getInteger(PARTITION_COUNT);
        this.syncWindow = getSyncWindow(properties);
        this.syncCountdown.set(syncWindow);
        this.maxConcurrentInvocations = getMaxConcurrentInvocations(properties);
        this.backoffTimeoutMs = getBackoffTimeoutMs(properties);

        if (enabled) {
            logger.info("Backpressure is enabled"
                    + ", maxConcurrentInvocations:" + maxConcurrentInvocations
                    + ", syncWindow: " + syncWindow);

            int backupTimeoutMillis = properties.getInteger(OPERATION_BACKUP_TIMEOUT_MILLIS);
            if (backupTimeoutMillis < MINUTES.toMillis(1)) {
                logger.warning(
                        format("Back pressure is enabled, but '%s' is too small. ",
                                OPERATION_BACKUP_TIMEOUT_MILLIS.getName()));
            }
        } else {
            if (logger.isFineEnabled()) {
                logger.fine("Backpressure is disabled");
            }
        }
    }

    int syncCountDown() {
        return syncCountdown.get();
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
        if (disabled) {
            return Integer.MAX_VALUE;
        }

        int invocationsPerPartition = props.getInteger(BACKPRESSURE_MAX_CONCURRENT_INVOCATIONS_PER_PARTITION);
        if (invocationsPerPartition < 1) {
            throw new IllegalArgumentException("Can't have '" + BACKPRESSURE_MAX_CONCURRENT_INVOCATIONS_PER_PARTITION
                    + "' with a value smaller than 1");
        }

        return (partitionCount + 1) * invocationsPerPartition;
    }

    /**
     * Checks if back-pressure is enabled.
     * <p>
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

    CallIdSequence newCallIdSequence(ConcurrencyDetection concurrencyDetection) {
        return CallIdFactory.newCallIdSequence(maxConcurrentInvocations, backoffTimeoutMs, concurrencyDetection);
    }

    /**
     * Checks if a sync is forced for the given BackupAwareOperation.
     * <p>
     * Once and a while for every BackupAwareOperation with one or more async backups, these async backups are transformed
     * into a sync backup.
     *
     * @param backupAwareOp the BackupAwareOperation to check
     * @return {@code true} if a sync needs to be forced, {@code false} otherwise
     */
    boolean isSyncForced(BackupAwareOperation backupAwareOp) {
        if (disabled) {
            return false;
        }

        // if there are no asynchronous backups, there is nothing to regulate.
        if (backupAwareOp.getAsyncBackupCount() == 0) {
            return false;
        }

        if (backupAwareOp instanceof UrgentSystemOperation) {
            return false;
        }

        for (; ; ) {
            int current = syncCountdown.decrementAndGet();
            if (current > 0) {
                return false;
            }

            if (syncCountdown.compareAndSet(current, randomSyncDelay())) {
                return true;
            }
        }
    }

    private int randomSyncDelay() {
        if (syncWindow == 1) {
            return 1;
        }

        Random random = ThreadLocalRandomProvider.get();
        int randomSyncWindow = round((1 - RANGE) * syncWindow + random.nextInt(round(2 * RANGE * syncWindow)));
        return max(1, randomSyncWindow);
    }
}
