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

import com.hazelcast.instance.Node;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.OutboundFrame;
import com.hazelcast.internal.networking.nio.NioChannel;
import com.hazelcast.internal.networking.nio.NioInboundPipeline;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.tcp.TcpIpConnection;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.UrgentSystemOperation;
import com.hazelcast.spi.impl.sequence.CallIdFactory;
import com.hazelcast.spi.impl.sequence.CallIdSequence;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.util.Clock;

import java.util.Iterator;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.nio.ConnectionType.NONE;
import static com.hazelcast.spi.properties.GroupProperty.BACKPRESSURE_ASYNCBACKUP_CHECKINTERVAL_MILLIS;
import static com.hazelcast.spi.properties.GroupProperty.BACKPRESSURE_ASYNCBACKUP_HIGHWATERMARK;
import static com.hazelcast.spi.properties.GroupProperty.BACKPRESSURE_ASYNCBACKUP_LOWWATERMARK;
import static com.hazelcast.spi.properties.GroupProperty.BACKPRESSURE_ASYNCBACKUP_WINDOW_MILLIS;
import static com.hazelcast.spi.properties.GroupProperty.BACKPRESSURE_BACKOFF_TIMEOUT_MILLIS;
import static com.hazelcast.spi.properties.GroupProperty.BACKPRESSURE_CLIENT_OUTBOUND_QUEUE_HIGHWATERMARK;
import static com.hazelcast.spi.properties.GroupProperty.BACKPRESSURE_CLIENT_OUTBOUND_QUEUE_LOWWATERMARK;
import static com.hazelcast.spi.properties.GroupProperty.BACKPRESSURE_ENABLED;
import static com.hazelcast.spi.properties.GroupProperty.BACKPRESSURE_MAX_CONCURRENT_INVOCATIONS_PER_PARTITION;
import static com.hazelcast.spi.properties.GroupProperty.OPERATION_BACKUP_TIMEOUT_MILLIS;
import static com.hazelcast.spi.properties.GroupProperty.PARTITION_COUNT;
import static com.hazelcast.util.Clock.currentTimeMillis;
import static com.hazelcast.util.ThreadUtil.createThreadName;
import static java.lang.Math.min;
import static java.lang.Math.max;
import static java.lang.String.format;
import static java.lang.System.nanoTime;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

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
public class BackpressureRegulator {

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
    private final long clientLowWaterMark;
    private final long clientHighWaterMark;
    private volatile long timeoutMs = currentTimeMillis();
    private volatile int asyncToSyncBackupRatio;
    public final NioRegulatorMonitor monitor;

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
        this.clientLowWaterMark = getClientBackpressureOutboundLowWaterMark(properties);
        this.clientHighWaterMark = getClientBackpressureOutboundHighWaterMark(properties);
        this.asyncBackupOverloadCheckIntervalMs = getAsyncBackupOverloadCheckIntervalMs(properties);
        this.asyncBackupOverloadDetectionThread = new AsyncBackupOverloadDetectionThread(node.hazelcastInstance.getName());
        this.monitor = new NioRegulatorMonitor();

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
            logger.info("Backpressure on async backups & clients is enabled.");
        } else {
            logger.warning("Backpressure on async backups & clients is disabled!");
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

    private int getClientBackpressureOutboundLowWaterMark(HazelcastProperties properties) {
        int lowWaterMark = properties.getInteger(BACKPRESSURE_CLIENT_OUTBOUND_QUEUE_LOWWATERMARK);
        if (lowWaterMark < 0) {
            throw new IllegalArgumentException("Can't have '" + BACKPRESSURE_CLIENT_OUTBOUND_QUEUE_LOWWATERMARK
                    + "' with a value smaller than 0");
        }
        return lowWaterMark;
    }

    private int getClientBackpressureOutboundHighWaterMark(HazelcastProperties properties) {
        int highWaterMark = properties.getInteger(BACKPRESSURE_CLIENT_OUTBOUND_QUEUE_HIGHWATERMARK);
        if (highWaterMark < clientLowWaterMark) {
            throw new IllegalArgumentException("Can't have '" + BACKPRESSURE_CLIENT_OUTBOUND_QUEUE_HIGHWATERMARK
                    + "' with a value smaller than the '" + BACKPRESSURE_CLIENT_OUTBOUND_QUEUE_LOWWATERMARK + "'");
        }
        return highWaterMark;
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
        // if there are no asynchronous backups, there is nothing to process.
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

    public final class NioRegulatorMonitor {

        private final double asyncBackupHwmOverflow = (asyncBackupHighWaterMark * 1.3);
        private final ConcurrentMap<NioChannel, Long> adding = new ConcurrentHashMap<>();
        private final ConcurrentMap<NioChannel, Long> added = new ConcurrentHashMap<>();

        private final AtomicBoolean clusterOverloadDetected = new AtomicBoolean(false);

        public boolean clusterOverloadDetected() {
            return clusterOverloadDetected.get();
        }

        public void process(NioChannel channel) {
            if (asyncBackupOverloadCheckIntervalMs <= 0) {
                // disabled
                return;
            }

            TcpIpConnection conn = ((TcpIpConnection) channel.attributeMap().get(TcpIpConnection.class));

            if (conn.getType().equals(NONE)) {
                // No type assigned yet, ignore it, to allow all handshakes to finish uninterrupted.
                return;
            }

            boolean clusterOverload = clusterOverloadDetected.get();

            if (conn.getType().isClient()) {
                checkAndYieldInbound(conn, channel, clusterOverload);
            } else {
                if (clusterOverload) {
                    return;
                }

                long pending = maxBytesPending(channel, asyncBackupHwmOverflow);
                if (pending > asyncBackupHwmOverflow) {
                    // Only enable client throttling if backups async-to-sync doesn't release the pressure
                    logger.info("Cluster overload detected on " + channel + " pending: " + pending);
                    clusterOverloadDetected.set(true);
                }
            }
        }

        void checkAndYieldInbound(TcpIpConnection conn, NioChannel channel, boolean clusterOverload) {
            NioInboundPipeline inboundPipeline = channel.inboundPipeline();
            int pendingFrames = channel.outboundPipeline().totalFramesPending();

            // TODO: Should be byte based.
            // Will be done with the upcoming SendQueue offering pending bytes in O(1)
            if ((clusterOverload || pendingFrames >= clientHighWaterMark) && !adding.containsKey(channel)) {
                // Avoid multiple calls
                if (adding.putIfAbsent(channel, nanoTime()) == null) {
                    logger.info("Yielding inbound of: " + conn + " q_len: " + pendingFrames
                            + ", watermarks: (" + clientLowWaterMark + "/" + clientHighWaterMark + ") bytes"
                            + ", cluster_overload: " + clusterOverload);
                    inboundPipeline.yield();
                    added.put(channel, 0L);
                }
            }
        }

        void checkAndWakeUpInbound(TcpIpConnection c, NioChannel channel) {
            Queue<OutboundFrame> writeQueue = channel.outboundPipeline().writeQueue;
            long qSize = writeQueue.size();

            NioInboundPipeline inboundPipeline = channel.inboundPipeline();

            if (qSize <= clientLowWaterMark) {
                Long start = added.remove(channel);
                if (start != null) {
                    start = adding.get(channel);
                    inboundPipeline.wakeup();

                    logger.info("Woke up " + c
                            + " q_len: " + qSize
                            + " time_to_lwm: " + NANOSECONDS.toMillis(nanoTime() - start) + " ms");

                    adding.remove(channel);
                }
            }

        }

        private long maxBytesPending(NioChannel channel, double fastfailCeiling) {
            TcpIpConnection c = (TcpIpConnection) channel.attributeMap().get(TcpIpConnection.class);
            Queue<OutboundFrame> writeQueue = channel.outboundPipeline().writeQueue;

            long pending = 0;
            for (OutboundFrame frame : writeQueue) {
                if (frame instanceof Packet) {
                    pending += frame.getFrameLength();
                }

                // if we exceed the high watermark for one of the connections, we are done. We don't need
                // to check the rest of the connections. Also we don't need to check the rest of the packets
                // of the current connection.
                if (pending > fastfailCeiling) {
                    return pending;
                }
            }

            return pending;
        }

    }

    private final class AsyncBackupOverloadDetectionThread extends Thread {
        private volatile boolean stop;
        private long counter;
        private int prechecksWarningCounter = 0;

        private AsyncBackupOverloadDetectionThread(String hzName) {
            super(createThreadName(hzName, "AsyncBackupOverloadDetectionThread"));
        }

        @Override
        public void run() {
            try {
                while (!stop) {
                    counter += asyncBackupOverloadCheckIntervalMs;
                    sleep();

                    if (!verifyNodeBootstrapped()) {
                        continue;
                    }

                    long startMs = System.currentTimeMillis();
                    long maxBytesPending = maxBytesPending();
                    if (maxBytesPending >= asyncBackupLowWaterMark) {
                        // overload detected.
                        timeoutMs = Clock.currentTimeMillis() + asyncBackupBackpressureWindowMs;
                        maxBytesPending = min(maxBytesPending, asyncBackupHighWaterMark);
                        asyncToSyncBackupRatio = Math.round(HUNDRED * ((float) maxBytesPending) / asyncBackupHighWaterMark);
                    } else {
                        asyncToSyncBackupRatio = 0;
                        if (monitor.clusterOverloadDetected.compareAndSet(true, false)) {
                            logger.info("Cluster recovered maxBytesPending: " + maxBytesPending);
                        }
                    }

                    // Don't do it only on successful CAS, we need to also do it for individual yielded clients.
                    if (!monitor.clusterOverloadDetected.get()) {
                        wakeUpYieldedClients();
                    }

                    long duration = System.currentTimeMillis() - startMs;
                    if (counter % 2000 == 0) {
                        logger.info("Sampling ratio:" + asyncToSyncBackupRatio
                                + "%, pending:" + maxBytesPending
                                + " bytes, duration:" + duration + " ms");
                    }
                }
            } catch (Throwable t) {
                logger.severe("Overload DetectionThread failed", t);
            }
        }

        private boolean verifyNodeBootstrapped() {
            if (node.networkingService.getNetworking() == null) {
                logger.warning("NetworkingService not initialized yet.");
                prechecksWarningCounter++;

                if (prechecksWarningCounter < 10) {
                    return false;
                } else {
                    throw new NullPointerException("NetworkingService not initialized yet.");
                }
            }

            return true;
        }

        private void sleep() {
            try {
                Thread.sleep(asyncBackupOverloadCheckIntervalMs);
            } catch (InterruptedException e) {
                // we can ignore it since we check in a loop if this thread needs to be stopped.
            }
        }

        private void wakeUpYieldedClients() {
            for (Iterator<Channel> it = node.networkingService.getNetworking().channels(); it.hasNext(); ) {
                NioChannel channel = (NioChannel) it.next();
                TcpIpConnection c = (TcpIpConnection) channel.attributeMap().get(TcpIpConnection.class);

                if (c == null || !c.isClient()) {
                    continue;
                }

                monitor.checkAndWakeUpInbound(c, channel);
            }
        }

        private long maxBytesPending() {
            long max = 0;
            for (Iterator<Channel> it = node.networkingService.getNetworking().channels(); it.hasNext(); ) {
                Channel channel = it.next();
                TcpIpConnection c = (TcpIpConnection) channel.attributeMap().get(TcpIpConnection.class);

                if (((NioChannel) channel).outboundPipeline() == null) {
                    continue;
                }

                if (c == null || c.isClient()) {
                    continue;
                }

                Queue<OutboundFrame> writeQueue = ((NioChannel) channel).outboundPipeline().writeQueue;

                if (c == null || c.isClient()) {
                    continue;
                }

                long pending = 0;
                for (OutboundFrame frame : writeQueue) {
                    if (frame instanceof Packet) {
                        pending += frame.getFrameLength();
                    }

                    //if we exceed the high watermark for one of the connections, we are done. We don't need
                    //to check the rest of the connections. Also we don't need to check the rest of the packets
                    //of the current connection.
                    if (pending > asyncBackupHighWaterMark) {
                        return pending;
                    }
                }

                max = max(max, pending);
            }

            return max;

        }

        private void shutdown() {
            stop = true;
            interrupt();
        }
    }
}
