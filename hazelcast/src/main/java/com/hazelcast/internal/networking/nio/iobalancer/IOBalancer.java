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

package com.hazelcast.internal.networking.nio.iobalancer;

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.networking.nio.MigratablePipeline;
import com.hazelcast.internal.networking.nio.NioInboundPipeline;
import com.hazelcast.internal.networking.nio.NioOutboundPipeline;
import com.hazelcast.internal.networking.nio.NioThread;
import com.hazelcast.internal.nio.ConnectionListener;
import com.hazelcast.internal.server.ServerConnectionManager;
import com.hazelcast.internal.util.counters.MwCounter;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.spi.properties.ClusterProperty;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.NETWORKING_METRIC_NIO_IO_BALANCER_IMBALANCE_DETECTED_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.NETWORKING_METRIC_NIO_IO_BALANCER_MIGRATION_COMPLETED_COUNT;
import static com.hazelcast.internal.metrics.ProbeLevel.DEBUG;
import static com.hazelcast.internal.util.counters.MwCounter.newMwCounter;
import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;
import static com.hazelcast.spi.properties.ClusterProperty.IO_BALANCER_INTERVAL_SECONDS;
import static com.hazelcast.spi.properties.ClusterProperty.IO_THREAD_COUNT;

/**
 * It attempts to detect and fix a selector imbalance problem.
 *
 * By default, Hazelcast uses 3 threads to read data from TCP connections and
 * 3 threads to write data to connections. We have measured significant fluctuations
 * of performance when the threads are not utilized equally.
 *
 * <code>IOBalancer</code> tries to detect such situations and fix them by moving
 * {@link NioInboundPipeline} and {@link NioOutboundPipeline} between {@link NioThread}
 * instances.
 *
 * It measures load serviced by each pipeline in a given interval and
 * if imbalance is detected then it schedules pipeline migration to fix the situation.
 * The exact migration strategy can be customized via
 * {@link com.hazelcast.internal.networking.nio.iobalancer.MigrationStrategy}.
 *
 * Measuring interval can be customized via {@link ClusterProperty#IO_BALANCER_INTERVAL_SECONDS}
 *
 * It doesn't leverage {@link ConnectionListener} capability
 * provided by {@link ServerConnectionManager} to observe connections
 * as it has to be notified right after a physical TCP connection is created whilst
 * <code>ConnectionListener</code> is notified only after a successful (Hazelcast)
 * binding process.
 */
public class IOBalancer {
    private static final String PROP_MONKEY_BALANCER = "hazelcast.io.balancer.monkey";
    private final ILogger logger;

    private final int balancerIntervalSeconds;
    private final MigrationStrategy strategy;

    private final LoadTracker inLoadTracker;
    private final LoadTracker outLoadTracker;
    private final String hzName;
    private final BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>();
    private volatile boolean enabled;
    private IOBalancerThread ioBalancerThread;

    // only IOBalancerThread will write to this field.
    @Probe(name = NETWORKING_METRIC_NIO_IO_BALANCER_IMBALANCE_DETECTED_COUNT, level = DEBUG)
    private final SwCounter imbalanceDetectedCount = newSwCounter();

    // multiple threads can update this field.
    @Probe(name = NETWORKING_METRIC_NIO_IO_BALANCER_MIGRATION_COMPLETED_COUNT, level = DEBUG)
    private final MwCounter migrationCompletedCount = newMwCounter();

    public IOBalancer(NioThread[] inputThreads,
                      NioThread[] outputThreads,
                      String hzName,
                      int balancerIntervalSeconds, LoggingService loggingService) {
        this.logger = loggingService.getLogger(IOBalancer.class);
        this.balancerIntervalSeconds = balancerIntervalSeconds;

        this.strategy = createMigrationStrategy();
        this.hzName = hzName;

        this.inLoadTracker = new LoadTracker(inputThreads, logger);
        this.outLoadTracker = new LoadTracker(outputThreads, logger);

        this.enabled = isEnabled(inputThreads, outputThreads);
    }

    // just for testing
    LoadTracker getInLoadTracker() {
        return inLoadTracker;
    }

    // just for testing
    LoadTracker getOutLoadTracker() {
        return outLoadTracker;
    }

    // just for testing
    BlockingQueue<Runnable> getWorkQueue() {
        return workQueue;
    }

    public void channelAdded(MigratablePipeline inboundPipeline, MigratablePipeline outboundPipeline) {
        // if not enabled, then don't schedule tasks that will not get processed.
        // See https://github.com/hazelcast/hazelcast/issues/11501
        if (enabled) {
            workQueue.add(new AddPipelineTask(inboundPipeline, outboundPipeline));
        }
    }

    public void channelRemoved(MigratablePipeline inboundPipeline, MigratablePipeline outboundPipeline) {
        // if not enabled, then don't schedule tasks that will not get processed.
        // See https://github.com/hazelcast/hazelcast/issues/11501
        if (enabled) {
            workQueue.add(new RemovePipelineTask(inboundPipeline, outboundPipeline));
        }
    }

    public void start() {
        if (enabled) {
            ioBalancerThread = new IOBalancerThread(this, balancerIntervalSeconds, hzName, logger, workQueue);
            ioBalancerThread.start();
        }
    }

    public void stop() {
        if (ioBalancerThread != null) {
            ioBalancerThread.shutdown();
        }
    }

    void rebalance() {
        scheduleMigrationIfNeeded(inLoadTracker);
        scheduleMigrationIfNeeded(outLoadTracker);
    }

    private void scheduleMigrationIfNeeded(LoadTracker loadTracker) {
        LoadImbalance loadImbalance = loadTracker.updateImbalance();
        if (strategy.imbalanceDetected(loadImbalance)) {
            imbalanceDetectedCount.inc();
            tryMigrate(loadImbalance);
        } else {
            if (logger.isFinestEnabled()) {
                long min = loadImbalance.minimumLoad;
                long max = loadImbalance.maximumLoad;
                if (max == Long.MIN_VALUE) {
                    logger.finest("There is at most 1 pipeline associated with each thread. "
                            + "There is nothing to balance");
                } else {
                    logger.finest("No imbalance has been detected. Max. load: " + max + " Min load: " + min + ".");
                }
            }
        }
    }

    private MigrationStrategy createMigrationStrategy() {
        if (Boolean.getBoolean(PROP_MONKEY_BALANCER)) {
            logger.warning("Using Monkey IO Balancer Strategy. This is for stress tests only. Do not user in production! "
                    + "Disable by not setting the property '" + PROP_MONKEY_BALANCER + "' to true.");
            return new MonkeyMigrationStrategy();
        } else {
            logger.finest("Using normal IO Balancer Strategy.");
            return new LoadMigrationStrategy();
        }
    }

    private boolean isEnabled(NioThread[] inputThreads, NioThread[] outputThreads) {
        if (balancerIntervalSeconds <= 0) {
            logger.warning("I/O Balancer is disabled as the '" + IO_BALANCER_INTERVAL_SECONDS + "' property is set to "
                    + balancerIntervalSeconds + ". Set the property to a value larger than 0 to enable the I/O Balancer.");
            return false;
        }

        if (inputThreads.length == 1 && outputThreads.length == 1) {
            logger.finest("I/O Balancer is disabled as there is only a single a pair of I/O threads. Use the '"
                    + IO_THREAD_COUNT + "' property to increase number of I/O Threads.");
            return false;
        }

        if (logger.isFinestEnabled()) {
            logger.finest("I/O Balancer is enabled. Scanning every " + balancerIntervalSeconds + " seconds for imbalances.");
        }

        return true;
    }

    private void tryMigrate(LoadImbalance loadImbalance) {
        MigratablePipeline pipeline = strategy.findPipelineToMigrate(loadImbalance);
        if (pipeline == null) {
            logger.finest("I/O imbalance is detected, but no suitable migration candidate is found.");
            return;
        }

        NioThread dstOwner = loadImbalance.dstOwner;
        if (logger.isFinestEnabled()) {
            NioThread srcOwner = loadImbalance.srcOwner;
            logger.finest("Scheduling migration of pipeline " + pipeline
                    + " from " + srcOwner + " to " + dstOwner);
        }
        pipeline.requestMigration(dstOwner);
    }

    public void signalMigrationComplete() {
        migrationCompletedCount.inc();
    }

    private final class RemovePipelineTask implements Runnable {

        private final MigratablePipeline inboundPipeline;
        private final MigratablePipeline outboundPipeline;

        private RemovePipelineTask(MigratablePipeline inboundPipeline, MigratablePipeline outboundPipeline) {
            this.inboundPipeline = inboundPipeline;
            this.outboundPipeline = outboundPipeline;
        }

        @Override
        public void run() {
            if (logger.isFinestEnabled()) {
                logger.finest("Removing pipelines: " + inboundPipeline + ", " + outboundPipeline);
            }

            inLoadTracker.removePipeline(inboundPipeline);
            outLoadTracker.removePipeline(outboundPipeline);
        }
    }

    private final class AddPipelineTask implements Runnable {

        private final MigratablePipeline inboundPipeline;
        private final MigratablePipeline outboundPipeline;

        private AddPipelineTask(MigratablePipeline inboundPipeline, MigratablePipeline outboundPipeline) {
            this.inboundPipeline = inboundPipeline;
            this.outboundPipeline = outboundPipeline;
        }

        @Override
        public void run() {
            if (logger.isFinestEnabled()) {
                logger.finest("Adding pipelines: " + inboundPipeline + ", " + outboundPipeline);
            }

            inLoadTracker.addPipeline(inboundPipeline);
            outLoadTracker.addPipeline(outboundPipeline);
        }
    }
}
