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

package com.hazelcast.nio.tcp.handlermigration;

import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.HazelcastThreadGroup;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.tcp.IOSelector;
import com.hazelcast.nio.tcp.InSelectorImpl;
import com.hazelcast.nio.tcp.MigratableHandler;
import com.hazelcast.nio.tcp.OutSelectorImpl;
import com.hazelcast.nio.tcp.ReadHandler;
import com.hazelcast.nio.tcp.TcpIpConnection;
import com.hazelcast.nio.tcp.WriteHandler;
import com.hazelcast.util.EmptyStatement;

import java.util.concurrent.TimeUnit;

/**
 * It attempts to detect and fix a selector imbalance problem.
 *
 * By default Hazelcast uses 3 threads to read data from TCP connections and 3 threads to write data to connections.
 * We have measured significant fluctuations of performance when the threads are not utilized equally.
 *
 * <code>com.hazelcast.nio.tcp.handlermigration.HandlerBalancer</code> tries to detect such situations and fix
 * them by moving {@link com.hazelcast.nio.tcp.ReadHandler} and {@link com.hazelcast.nio.tcp.WriteHandler} between
 * threads.
 *
 * It measures number of events serviced by each handler in a given interval and if imbalance is detected then it
 * schedules handler migration to fix the situation. The exact migration strategy can be customized via
 * {@link com.hazelcast.nio.tcp.handlermigration.MigrationStrategy}.
 *
 * Measuring interval can be customized via {@link com.hazelcast.instance.GroupProperties#IO_BALANCER_INTERVAL_SECONDS}
 *
 * It doesn't leverage {@link com.hazelcast.nio.ConnectionListener} capability
 * provided by {@link com.hazelcast.nio.ConnectionManager} to observe connections as it has to be notified
 * right after a physical TCP connection is created whilst <code>ConnectionListener</code> is notified only
 * after a successful (Hazelcast) binding process.
 *
 *
 */
public class IOBalancer {
    private final ILogger log;

    private final int migrationIntervalSeconds;
    private final MigrationStrategy strategy;

    private final LoadTracker inLoadTracker;
    private final LoadTracker outLoadTracker;

    private final HazelcastThreadGroup threadGroup;
    private volatile boolean isActive;

    public IOBalancer(InSelectorImpl[] inSelectors, OutSelectorImpl[] outSelectors, HazelcastThreadGroup threadGroup,
                      int migrationIntervalSeconds, LoggingService loggingService) {
        this.log = loggingService.getLogger(IOBalancer.class);
        this.migrationIntervalSeconds = migrationIntervalSeconds;
        this.strategy = new MigrationStrategy();
        this.threadGroup = threadGroup;

        this.inLoadTracker = new LoadTracker(inSelectors, loggingService);
        this.outLoadTracker = new LoadTracker(outSelectors, loggingService);

        this.isActive = shouldStart(inSelectors, outSelectors);
    }

    public void connectionAdded(Connection connection) {
        if (!(connection instanceof TcpIpConnection)) {
            return;
        }

        ReadHandler readHandler = ((TcpIpConnection) connection).getReadHandler();
        if (log.isFinestEnabled()) {
            log.finest("Adding a read handler " + readHandler);
        }
        inLoadTracker.addHandler(readHandler);

        WriteHandler writeHandler = ((TcpIpConnection) connection).getWriteHandler();
        if (log.isFinestEnabled()) {
            log.info("Adding a write handler " + writeHandler);
        }
        outLoadTracker.addHandler(writeHandler);
    }

    public void connectionRemoved(Connection connection) {
        if (!(connection instanceof TcpIpConnection)) {
            return;
        }

        ReadHandler readHandler = ((TcpIpConnection) connection).getReadHandler();
        if (log.isFinestEnabled()) {
            log.finest("Removing a read handler " + readHandler);
        }
        inLoadTracker.removeHandler(readHandler);

        WriteHandler writeHandler = ((TcpIpConnection) connection).getWriteHandler();
        if (log.isFinestEnabled()) {
            log.finest("Removing a write handler " + readHandler);
        }
        outLoadTracker.removeHandler(writeHandler);
    }

    public void stop() {
        isActive = false;
    }

    public void start() {
        Thread ioBalancerThread = new IOBalancerThread();
        ioBalancerThread.start();
    }

    private void checkWriteHandlers() {
        scheduleMigrationIfNeeded(outLoadTracker);
    }

    private void checkReadHandlers() {
        scheduleMigrationIfNeeded(inLoadTracker);
    }

    public void scheduleMigrationIfNeeded(LoadTracker loadTracker) {
        LoadImbalance loadImbalance = loadTracker.updateImbalance();
        if (strategy.imbalanceDetected(loadImbalance)) {
            tryMigrate(loadImbalance);
        } else {
            if (log.isFinestEnabled()) {
                long min = loadImbalance.minimumEvents;
                long max = loadImbalance.maximumEvents;
                log.finest("No imbalance has been detected. Max. events: " + max + " Min events: " + min + ".");
            }
        }
    }

    private boolean shouldStart(InSelectorImpl[] inSelectors, OutSelectorImpl[] outSelectors) {
        if (migrationIntervalSeconds < 0) {
            if (log.isFinestEnabled()) {
                log.finest("I/O Balancer is disabled as the '"
                        + GroupProperties.PROP_PERFORMANCE_MONITORING_ENABLED + "' property is set to "
                        + migrationIntervalSeconds + ". Set the property to a positive value to enable I/O Balancer.");
            }
            return false;
        }
        if (inSelectors.length == 1 && outSelectors.length == 1) {
            log.finest("I/O Balancer is disabled as there is only a single a pair of I/O threads. Use the '"
                    + GroupProperties.PROP_IO_THREAD_COUNT + "' property to increase number of I/O Threads.");
            return false;
        }
        return true;
    }

    private void tryMigrate(LoadImbalance loadImbalance) {
        MigratableHandler handler = strategy.findHandlerToMigrate(loadImbalance);
        if (handler == null) {
            log.finest("There had been I/O imbalance detected, but no suitable migration candidate was found.");
            return;
        }

        IOSelector destinationSelector = loadImbalance.destinationSelector;
        if (log.isFinestEnabled()) {
            IOSelector sourceSelector = loadImbalance.sourceSelector;
            log.finest("Scheduling a migration of a handler " + handler
                    + " from a selector thread " + sourceSelector + " to " + destinationSelector);
        }
        handler.migrate(destinationSelector);
    }

    private final class IOBalancerThread extends Thread {
        private IOBalancerThread() {
            super(threadGroup.getInternalThreadGroup(), threadGroup.getThreadNamePrefix("IOBalancerThread"));
        }

        @Override
        public void run() {
            try {
                while (isActive) {
                    log.finest("Starting IOBalancer thread");
                    checkReadHandlers();
                    checkWriteHandlers();
                    TimeUnit.SECONDS.sleep(migrationIntervalSeconds);
                }
            } catch (InterruptedException e) {
                log.finest("IOBalancer thread stopped");
                //this thread is about to exit, no reason restoring the interrupt flag
                EmptyStatement.ignore(e);
            }
        }
    }
}
