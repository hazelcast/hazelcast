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
 * Hazelcast by default uses 3 threads to read data from TCP connections and 3 threads to write data to connections.
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
 * Measuring interval can be customized via {@link com.hazelcast.instance.GroupProperties#HANDLER_MIGRATION_INTERVAL_SECONDS}
 *
 * It doesn't leverage {@link com.hazelcast.nio.ConnectionListener} capability
 * provided by {@link com.hazelcast.nio.ConnectionManager} to observe connections as it has to be notified
 * right after a physical TCP connection is created whilst <code>ConnectionListener</code> is notified only
 * after a successful (Hazelcast) binding process.
 *
 *
 */
public class HandlerBalancer implements Runnable {
    private final ILogger log;

    private final int migrationIntervalSeconds;
    private final MigrationStrategy strategy;

    private final IOSelectorLoadCalculator<ReadHandler> inCalculator;
    private final IOSelectorLoadCalculator<WriteHandler> outCalculator;

    private volatile boolean isActive = true;

    public HandlerBalancer(InSelectorImpl[] inSelectors, OutSelectorImpl[] outSelectors, int migrationIntervalSeconds,
                           LoggingService loggingService) {
        this.log = loggingService.getLogger(HandlerBalancer.class);
        this.migrationIntervalSeconds = migrationIntervalSeconds;
        this.strategy = new MigrationStrategy();

        inCalculator = new IOSelectorLoadCalculator<ReadHandler>(inSelectors, loggingService);
        outCalculator = new IOSelectorLoadCalculator<WriteHandler>(outSelectors, loggingService);
    }

    public void connectionAdded(Connection connection) {
        if (!(connection instanceof TcpIpConnection)) {
            return;
        }

        ReadHandler readHandler = ((TcpIpConnection) connection).getReadHandler();
        if (log.isFinestEnabled()) {
            log.finest("Adding a read handler " + readHandler);
        }
        inCalculator.addHandler(readHandler);

        WriteHandler writeHandler = ((TcpIpConnection) connection).getWriteHandler();
        if (log.isFinestEnabled()) {
            log.info("Adding a write handler " + writeHandler);
        }
        outCalculator.addHandler(writeHandler);
    }

    public void connectionRemoved(Connection connection) {
        if (!(connection instanceof TcpIpConnection)) {
            return;
        }

        ReadHandler readHandler = ((TcpIpConnection) connection).getReadHandler();
        if (log.isFinestEnabled()) {
            log.finest("Removing a read handler " + readHandler);
        }
        inCalculator.removeHandler(readHandler);

        WriteHandler writeHandler = ((TcpIpConnection) connection).getWriteHandler();
        if (log.isFinestEnabled()) {
            log.finest("Removing a write handler " + readHandler);
        }
        outCalculator.removeHandler(writeHandler);
    }

    @Override
    public void run() {
        try {
            while (isActive) {
                log.finest("Starting selector thread handler balance check");
                checkReadHandlers();
                checkWriteHandlers();
                TimeUnit.SECONDS.sleep(migrationIntervalSeconds);
            }
        } catch (InterruptedException e) {
            //this thread is about to exit, no reason restoring the interrupt flag
            EmptyStatement.ignore(e);
        }
    }

    public void stop() {
        isActive = false;
    }

    private void checkWriteHandlers() {
        scheduleMigration(outCalculator);
    }

    private void checkReadHandlers() {
        scheduleMigration(inCalculator);
    }

    public void scheduleMigration(IOSelectorLoadCalculator<?> calculator) {
        BalancerState<?> balancerState = calculator.calculate();

        if (strategy.shouldAttemptToScheduleMigration(balancerState)) {
            MigratableHandler handler = strategy.findHandlerToMigrate(balancerState);
            if (handler != null) {
                IOSelector destinationSelector = balancerState.destinationSelector;
                if (log.isFinestEnabled()) {
                    IOSelector sourceSelector = balancerState.sourceSelector;
                    log.finest("Scheduling a migration of a handler " + handler
                            + " from a selector thread " + sourceSelector + " to " + destinationSelector);
                }
                handler.migrate(destinationSelector);
            } else {
                log.finest("There has been imbalance detected, but no suitable migration candidate has been found!");
            }
        } else {
            if (log.isFinestEnabled()) {
                long min = balancerState.minimumEvents;
                long max = balancerState.maximumEvents;
                log.finest("No imbalance has been detected. Max: " + max + " Min: " + min);
            }
        }
    }

}
