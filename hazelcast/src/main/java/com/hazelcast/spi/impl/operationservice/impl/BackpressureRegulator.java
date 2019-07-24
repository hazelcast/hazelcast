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
import com.hazelcast.internal.metrics.Probe;
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

import java.util.Iterator;
import java.util.Queue;
import java.util.Random;

import static com.hazelcast.spi.properties.GroupProperty.BACKPRESSURE_ASYNCBACKUP_CHECKINTERVAL_MILLIS;
import static com.hazelcast.spi.properties.GroupProperty.BACKPRESSURE_ASYNCBACKUP_DURATION;
import static com.hazelcast.spi.properties.GroupProperty.BACKPRESSURE_ASYNCBACKUP_HIGHWATERMARK;
import static com.hazelcast.spi.properties.GroupProperty.BACKPRESSURE_ASYNCBACKUP_LOWWATERMARK;
import static com.hazelcast.spi.properties.GroupProperty.BACKPRESSURE_BACKOFF_TIMEOUT_MILLIS;
import static com.hazelcast.spi.properties.GroupProperty.BACKPRESSURE_ENABLED;
import static com.hazelcast.spi.properties.GroupProperty.BACKPRESSURE_MAX_CONCURRENT_INVOCATIONS_PER_PARTITION;
import static com.hazelcast.spi.properties.GroupProperty.OPERATION_BACKUP_TIMEOUT_MILLIS;
import static com.hazelcast.spi.properties.GroupProperty.PARTITION_COUNT;
import static com.hazelcast.util.Clock.currentTimeMillis;
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

    private final boolean enabled;
    private final boolean disabled;
    private final int partitionCount;
    private final int maxConcurrentInvocations;
    private final int backoffTimeoutMs;
    private final Random random = new Random();
    private final Node node;
    private final DetectionThread thread = new DetectionThread();
    private final int checkIntervalMs;
    private volatile boolean stop;
    private final long lowWaterMark ;
    private final long highWaterMark;
    private final long asyncBackpressureDurationMs;
    private volatile long timeoutMs = currentTimeMillis();
    @Probe
    private volatile int ratio;

    BackpressureRegulator(HazelcastProperties properties, ILogger logger, Node node) {
        this.enabled = properties.getBoolean(BACKPRESSURE_ENABLED);
        this.disabled = !enabled;
        this.partitionCount = properties.getInteger(PARTITION_COUNT);
        this.maxConcurrentInvocations = getMaxConcurrentInvocations(properties);
        this.backoffTimeoutMs = getBackoffTimeoutMs(properties);
        this.lowWaterMark = properties.getInteger(BACKPRESSURE_ASYNCBACKUP_LOWWATERMARK);
        this.highWaterMark = properties.getInteger(BACKPRESSURE_ASYNCBACKUP_HIGHWATERMARK);
        this.asyncBackpressureDurationMs = properties.getInteger(BACKPRESSURE_ASYNCBACKUP_DURATION);
        this.checkIntervalMs = properties.getInteger(BACKPRESSURE_ASYNCBACKUP_CHECKINTERVAL_MILLIS);

        this.node = node;
        if (enabled) {
            logger.info("Backpressure is enabled"
                    + ", maxConcurrentInvocations:" + maxConcurrentInvocations);

            int backupTimeoutMillis = properties.getInteger(OPERATION_BACKUP_TIMEOUT_MILLIS);
            if (backupTimeoutMillis < MINUTES.toMillis(1)) {
                logger.warning(
                        format("Back pressure is enabled, but '%s' is too small. ",
                                OPERATION_BACKUP_TIMEOUT_MILLIS.getName()));
            }
        } else {
            logger.info("Backpressure is disabled");
        }
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

        int ratio = getRatio();
        if (ratio == 0) {
            return false;
        }
        return random.nextInt(100) <= ratio;
    }

    private int getRatio() {
        if (currentTimeMillis() > timeoutMs) {
            return 0;
        }

        return ratio;
    }

    public void start() {
        thread.start();
    }

    public void shutdown() {
        stop = true;
        thread.interrupt();
    }

    private class DetectionThread extends Thread {
        @Override
        public void run() {
            while (!stop) {
                try {
                    Thread.sleep(checkIntervalMs);
                } catch (InterruptedException e) {
                }

                long startMs = System.currentTimeMillis();
                long max = maxPending();
                if (max < lowWaterMark) {
                    continue;
                }

                // overload detected.
                timeoutMs = System.currentTimeMillis() + asyncBackpressureDurationMs;
                max = min(max, highWaterMark);
                ratio = Math.round(100 * ((float) max) / highWaterMark);
                long duration = System.currentTimeMillis() - startMs;
                System.out.println("ratio:" + ratio + " max:" + max + " duration:" + duration + " ms");
            }
        }

        private long maxPending() {
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
                    // to check the rest of the connections. Also we don't need to check the rest of the packet
                    // of the current connection.
                    if (pending > highWaterMark) {
                        return pending;
                    }
                }
            }
            return max;
        }
    }
}
