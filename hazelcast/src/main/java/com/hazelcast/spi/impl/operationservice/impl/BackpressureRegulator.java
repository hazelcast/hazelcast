/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.OutboundFrame;
import com.hazelcast.internal.networking.nio.NioChannel;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.tcp.TcpIpConnection;
import com.hazelcast.spi.impl.operationservice.BackupAwareOperation;
import com.hazelcast.spi.impl.operationservice.UrgentSystemOperation;
import com.hazelcast.spi.impl.sequence.CallIdFactory;
import com.hazelcast.spi.impl.sequence.CallIdSequence;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.util.Clock;

import java.util.Iterator;
import java.util.Queue;
import java.util.Random;

import static com.hazelcast.spi.properties.GroupProperty.BACKPRESSURE_ASYNCBACKUP_CHECKINTERVAL_MILLIS;
import static com.hazelcast.spi.properties.GroupProperty.BACKPRESSURE_ASYNCBACKUP_HIGHWATERMARK;
import static com.hazelcast.spi.properties.GroupProperty.BACKPRESSURE_ASYNCBACKUP_LOWWATERMARK;
import static com.hazelcast.spi.properties.GroupProperty.BACKPRESSURE_ASYNCBACKUP_WINDOW_MILLIS;
import static com.hazelcast.spi.properties.GroupProperty.BACKPRESSURE_BACKOFF_TIMEOUT_MILLIS;
import static com.hazelcast.spi.properties.GroupProperty.BACKPRESSURE_ENABLED;
import static com.hazelcast.spi.properties.GroupProperty.BACKPRESSURE_MAX_CONCURRENT_INVOCATIONS_PER_PARTITION;
import static com.hazelcast.spi.properties.GroupProperty.OPERATION_BACKUP_TIMEOUT_MILLIS;
import static com.hazelcast.spi.properties.GroupProperty.PARTITION_COUNT;
import static com.hazelcast.util.Clock.currentTimeMillis;
import static com.hazelcast.util.ThreadUtil.createThreadName;
import static java.lang.Long.min;
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

    private static final int HUNDRED = 100;

    private final boolean enabled;
    private final boolean disabled;
    private final int partitionCount;
    private final int maxConcurrentInvocations;
    private final int backoffTimeoutMs;
    private final Random random = new Random();
    private final Node node;
    private final AsyncBackupOverloadDetectionThread asyncBackupOverloadDetectionThread;
    private final int asyncBackupOverloadCheckIntervalMs;
    private final ILogger logger;
    private final long asyncBackupLowWaterMark;
    private final long asyncBackupHighWaterMark;
    private final long asyncBackupBackpressureWindowMs;
    private volatile long timeoutMs = currentTimeMillis();
    private volatile int asyncToSyncBackupRatio;

    BackpressureRegulator(HazelcastProperties properties, ILogger logger, Node node) {
        this.enabled = properties.getBoolean(BACKPRESSURE_ENABLED);
        this.logger = logger;
        this.disabled = !enabled;
        this.node = node;
        this.partitionCount = properties.getInteger(PARTITION_COUNT);
        this.maxConcurrentInvocations = getMaxConcurrentInvocations(properties);
        this.backoffTimeoutMs = getBackoffTimeoutMs(properties);
        this.asyncBackupLowWaterMark = getAsyncBackupLowWaterMark(properties);
        this.asyncBackupHighWaterMark = getAsyncBackupHighWaterMark(properties);
        this.asyncBackupBackpressureWindowMs = getAsyncBackupBackpressureWindowMs(properties);
        this.asyncBackupOverloadCheckIntervalMs = getAsyncBackupOverloadCheckIntervalMs(properties);
        this.asyncBackupOverloadDetectionThread = new AsyncBackupOverloadDetectionThread(node.hazelcastInstance.getName());

        if (enabled) {
            logger.info("Backpressure on invocations is enabled"
                    + ", maxConcurrentInvocations:" + maxConcurrentInvocations);

            int backupTimeoutMillis = properties.getInteger(OPERATION_BACKUP_TIMEOUT_MILLIS);
            if (backupTimeoutMillis < MINUTES.toMillis(1)) {
                logger.warning(format("Back pressure is enabled, but '%s' is too small. ",
                        OPERATION_BACKUP_TIMEOUT_MILLIS.getName()));
            }
        } else {
            logger.info("Backpressure on invocations is disabled");
        }

        if (asyncBackupOverloadCheckIntervalMs > 0) {
            logger.fine("Backpressure on async backups is enabled.");
        } else {
            logger.warning("Backpressure on async backups is disabled!");
        }
    }

    private int getAsyncBackupOverloadCheckIntervalMs(HazelcastProperties properties) {
        return properties.getInteger(BACKPRESSURE_ASYNCBACKUP_CHECKINTERVAL_MILLIS);
    }

    private int getAsyncBackupBackpressureWindowMs(HazelcastProperties properties) {
        int window = properties.getInteger(BACKPRESSURE_ASYNCBACKUP_WINDOW_MILLIS);
        if (window < 1) {
            throw new IllegalArgumentException("Can't have '" + BACKPRESSURE_ASYNCBACKUP_WINDOW_MILLIS
                    + "' with a value smaller than 1");
        }
        return window;
    }

    private int getAsyncBackupHighWaterMark(HazelcastProperties properties) {
        int highWaterMark = properties.getInteger(BACKPRESSURE_ASYNCBACKUP_HIGHWATERMARK);
        if (highWaterMark < asyncBackupLowWaterMark) {
            throw new IllegalArgumentException("Can't have '" + BACKPRESSURE_ASYNCBACKUP_HIGHWATERMARK
                    + "' with a value smaller than the '" + BACKPRESSURE_ASYNCBACKUP_LOWWATERMARK + "'");
        }
        return highWaterMark;
    }

    private int getAsyncBackupLowWaterMark(HazelcastProperties properties) {
        int lowWaterMark = properties.getInteger(BACKPRESSURE_ASYNCBACKUP_LOWWATERMARK);
        if (lowWaterMark < 0) {
            throw new IllegalArgumentException("Can't have '" + BACKPRESSURE_ASYNCBACKUP_LOWWATERMARK
                    + "' with a value smaller than 0");
        }
        return lowWaterMark;
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
     * <p>
     * This method is only used for testing.
     */
    boolean isEnabled() {
        return enabled;
    }

    // for testing only
    int getMaxConcurrentInvocations() {
        if (enabled) {
            return maxConcurrentInvocations;
        } else {
            return Integer.MAX_VALUE;
        }
    }

    CallIdSequence newCallIdSequence() {
        return CallIdFactory.newCallIdSequence(enabled, maxConcurrentInvocations, backoffTimeoutMs);
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
        // if there are no asynchronous backups, there is nothing to regulate.
        if (backupAwareOp.getAsyncBackupCount() == 0) {
            return false;
        }

        if (backupAwareOp instanceof UrgentSystemOperation) {
            return false;
        }

        if (currentTimeMillis() > timeoutMs) {
            // no overload is detected.
            return false;
        }

        return random.nextInt(HUNDRED) <= asyncToSyncBackupRatio;
    }

    public void start() {
        if (asyncBackupOverloadCheckIntervalMs > 0) {
            asyncBackupOverloadDetectionThread.start();
        }
    }

    public void shutdown() {
        asyncBackupOverloadDetectionThread.shutdown();
    }

    private final class AsyncBackupOverloadDetectionThread extends Thread {
        private volatile boolean stop;

        private AsyncBackupOverloadDetectionThread(String hzName) {
            super(createThreadName(hzName, "AsyncBackupOverloadDetectionThread"));
        }

        @Override
        public void run() {
            try {
                while (!stop) {
                    sleep();

                    long startMs = System.currentTimeMillis();
                    long maxBytesPending = maxBytesPending();
                    if (maxBytesPending < asyncBackupLowWaterMark) {
                        continue;
                    }

                    // overload detected.
                    timeoutMs = Clock.currentTimeMillis() + asyncBackupBackpressureWindowMs;
                    maxBytesPending = min(maxBytesPending, asyncBackupHighWaterMark);
                    asyncToSyncBackupRatio = Math.round(HUNDRED * ((float) maxBytesPending) / asyncBackupHighWaterMark);
                    long duration = System.currentTimeMillis() - startMs;
                    logger.info("ratio:" + asyncToSyncBackupRatio + " max:" + maxBytesPending + " duration:" + duration + " ms");
                }
            } catch (Throwable t) {
                logger.severe("Overload DetectionThread failed", t);
            }
        }

        private void sleep() {
            try {
                Thread.sleep(asyncBackupOverloadCheckIntervalMs);
            } catch (InterruptedException e) {
                // we can ignore it since we check in a loop if this thread needs to be stopped.
            }
        }

        private long maxBytesPending() {
            long max = 0;
            for (Iterator<Channel> it = node.networkingService.getNetworking().channels(); it.hasNext(); ) {
                Channel channel = it.next();
                TcpIpConnection c = (TcpIpConnection) channel.attributeMap().get(TcpIpConnection.class);
                if (c.isClient()) {
                    continue;
                }

                Queue<OutboundFrame> writeQueue = ((NioChannel) channel).outboundPipeline().writeQueue;
                long pending = 0;
                for (OutboundFrame frame : writeQueue) {
                    if (frame instanceof Packet) {
                        pending += frame.getFrameLength();
                    }

                    // if we exceed the high watermark for one of the connections, we are done. We don't need
                    // to check the rest of the connections. Also we don't need to check the rest of the packets
                    // of the current connection.
                    if (pending > asyncBackupHighWaterMark) {
                        return pending;
                    }
                }
            }
            return max;
        }

        private void shutdown() {
            stop = true;
            interrupt();
        }
    }
}
