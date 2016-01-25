/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio.tcp.nonblocking.iobalancer;

import com.hazelcast.instance.HazelcastThreadGroup;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.nio.tcp.TcpIpConnection;
import com.hazelcast.nio.tcp.nonblocking.MigratableHandler;
import com.hazelcast.nio.tcp.nonblocking.NonBlockingIOThread;
import com.hazelcast.nio.tcp.nonblocking.NonBlockingSocketReader;
import com.hazelcast.nio.tcp.nonblocking.NonBlockingSocketWriter;
import com.hazelcast.util.counters.MwCounter;
import com.hazelcast.util.counters.SwCounter;

import static com.hazelcast.instance.GroupProperty.IO_BALANCER_INTERVAL_SECONDS;
import static com.hazelcast.instance.GroupProperty.IO_THREAD_COUNT;
import static com.hazelcast.util.counters.MwCounter.newMwCounter;
import static com.hazelcast.util.counters.SwCounter.newSwCounter;

/**
 * It attempts to detect and fix a selector imbalance problem.
 *
 * By default Hazelcast uses 3 threads to read data from TCP connections and 3 threads to write data to connections.
 * We have measured significant fluctuations of performance when the threads are not utilized equally.
 *
 * <code>com.hazelcast.nio.tcp.iobalancer.HandlerBalancer</code> tries to detect such situations and fix
 * them by moving {@link NonBlockingSocketReader} and {@link NonBlockingSocketWriter} between
 * threads.
 *
 * It measures number of events serviced by each handler in a given interval and if imbalance is detected then it
 * schedules handler migration to fix the situation. The exact migration strategy can be customized via
 * {@link com.hazelcast.nio.tcp.nonblocking.iobalancer.MigrationStrategy}.
 *
 * Measuring interval can be customized via {@link com.hazelcast.instance.GroupProperty#IO_BALANCER_INTERVAL_SECONDS}
 *
 * It doesn't leverage {@link com.hazelcast.nio.ConnectionListener} capability
 * provided by {@link com.hazelcast.nio.ConnectionManager} to observe connections as it has to be notified
 * right after a physical TCP connection is created whilst <code>ConnectionListener</code> is notified only
 * after a successful (Hazelcast) binding process.
 */
public class IOBalancer {
    private static final String PROP_MONKEY_BALANCER = "hazelcast.io.balancer.monkey";
    private final ILogger logger;

    private final int balancerIntervalSeconds;
    private final MigrationStrategy strategy;

    private final LoadTracker inLoadTracker;
    private final LoadTracker outLoadTracker;

    private final HazelcastThreadGroup threadGroup;
    private volatile boolean enabled;
    private IOBalancerThread ioBalancerThread;

    // only IOBalancerThread will write to this field.
    @Probe
    private final SwCounter imbalanceDetectedCount = newSwCounter();

    // multiple threads can update this field.
    @Probe
    private final MwCounter migrationCompletedCount = newMwCounter();

    public IOBalancer(NonBlockingIOThread[] inputThreads,
                      NonBlockingIOThread[] outputThreads,
                      HazelcastThreadGroup threadGroup,
                      int balancerIntervalSeconds, LoggingService loggingService) {
        this.logger = loggingService.getLogger(IOBalancer.class);
        this.balancerIntervalSeconds = balancerIntervalSeconds;

        this.strategy = createMigrationStrategy();
        this.threadGroup = threadGroup;

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

    public void connectionAdded(TcpIpConnection connection) {
        NonBlockingSocketReader socketReader = (NonBlockingSocketReader) connection.getSocketReader();
        NonBlockingSocketWriter socketWriter = (NonBlockingSocketWriter) connection.getSocketWriter();

        if (logger.isFinestEnabled()) {
            logger.finest("Added handlers for: " + connection);
        }

        inLoadTracker.addHandler(socketReader);
        outLoadTracker.addHandler(socketWriter);
    }

    public void connectionRemoved(TcpIpConnection connection) {
        NonBlockingSocketReader socketReader = (NonBlockingSocketReader) connection.getSocketReader();
        NonBlockingSocketWriter socketWriter = (NonBlockingSocketWriter) connection.getSocketWriter();

        if (logger.isFinestEnabled()) {
            logger.finest("Removing handlers from: " + connection);
        }

        inLoadTracker.removeHandler(socketReader);
        outLoadTracker.removeHandler(socketWriter);
    }

    public void start() {
        if (enabled) {
            ioBalancerThread = new IOBalancerThread(this, balancerIntervalSeconds, threadGroup, logger);
            ioBalancerThread.start();
        }
    }

    public void stop() {
        if (ioBalancerThread != null) {
            ioBalancerThread.shutdown();
        }
    }

    void checkWriteHandlers() {
        scheduleMigrationIfNeeded(outLoadTracker);
    }

    void checkReadHandlers() {
        scheduleMigrationIfNeeded(inLoadTracker);
    }

    private void scheduleMigrationIfNeeded(LoadTracker loadTracker) {
        LoadImbalance loadImbalance = loadTracker.updateImbalance();
        if (strategy.imbalanceDetected(loadImbalance)) {
            imbalanceDetectedCount.inc();
            tryMigrate(loadImbalance);
        } else {
            if (logger.isFinestEnabled()) {
                long min = loadImbalance.minimumEvents;
                long max = loadImbalance.maximumEvents;
                logger.finest("No imbalance has been detected. Max. events: " + max + " Min events: " + min + ".");
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
            return new EventCountBasicMigrationStrategy();
        }
    }

    private boolean isEnabled(NonBlockingIOThread[] inputThreads, NonBlockingIOThread[] outputThreads) {
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
        MigratableHandler handler = strategy.findHandlerToMigrate(loadImbalance);
        if (handler == null) {
            logger.finest("I/O imbalance is detected, but no suitable migration candidate is found.");
            return;
        }

        NonBlockingIOThread destinationSelector = loadImbalance.destinationSelector;
        if (logger.isFinestEnabled()) {
            NonBlockingIOThread sourceSelector = loadImbalance.sourceSelector;
            logger.finest("Scheduling migration of handler " + handler
                    + " from selector thread " + sourceSelector + " to " + destinationSelector);
        }
        handler.requestMigration(destinationSelector);
    }

    public void signalMigrationComplete() {
        migrationCompletedCount.inc();
    }
}
