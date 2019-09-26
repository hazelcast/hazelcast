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

package com.hazelcast.internal.partition.impl;

import com.hazelcast.instance.impl.OutOfMemoryErrorDispatcher;
import com.hazelcast.internal.partition.impl.MigrationManager.MigrateTask;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.properties.GroupProperty;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.util.ThreadUtil.createThreadName;
import static java.lang.Math.max;

/**
 * MigrationThread is responsible to execute migration related tasks submitted to its
 * migration-queue.
 */
class MigrationThread extends Thread implements Runnable {

    private static final long DEFAULT_MIGRATION_SLEEP_INTERVAL = 250L;

    private final MigrationManager migrationManager;
    private final MigrationQueue queue;
    private final ILogger logger;
    /**
     * Time in milliseconds to sleep after {@link MigrateTask}
     */
    private final long partitionMigrationInterval;
    /**
     * Time in milliseconds to sleep when the migration queue is empty or migrations are not allowed
     */
    private final long sleepTime;

    private volatile MigrationRunnable activeTask;
    private volatile boolean running = true;

    MigrationThread(MigrationManager migrationManager, String hzName, ILogger logger,
                    MigrationQueue queue) {
        super(createThreadName(hzName, "migration"));

        this.migrationManager = migrationManager;
        this.queue = queue;
        partitionMigrationInterval = migrationManager.partitionMigrationInterval;
        sleepTime = max(DEFAULT_MIGRATION_SLEEP_INTERVAL, partitionMigrationInterval);
        this.logger = logger;
    }

    @Override
    public void run() {
        try {
            while (running) {
                doRun();
            }
        } catch (InterruptedException e) {
            if (logger.isFinestEnabled()) {
                logger.finest("MigrationThread is interrupted: " + e.getMessage());
            }
        } catch (OutOfMemoryError e) {
            OutOfMemoryErrorDispatcher.onOutOfMemory(e);
        } finally {
            queue.clear();
        }
    }

    /**
     * Polls the migration queue and processes the tasks, sleeping if there are no tasks, if migration is not allowed or
     * if configured to do so (see {@link GroupProperty#PARTITION_MIGRATION_INTERVAL}).
     *
     * @throws InterruptedException if the sleep was interrupted
     */
    private void doRun() throws InterruptedException {
        boolean migrating = false;
        for (; ; ) {
            if (!migrationManager.areMigrationTasksAllowed()) {
                break;
            }
            MigrationRunnable runnable = queue.poll(1, TimeUnit.SECONDS);
            if (runnable == null) {
                break;
            }

            migrating |= runnable instanceof MigrationManager.MigrateTask;
            processTask(runnable);
            if (migrating && partitionMigrationInterval > 0) {
                Thread.sleep(partitionMigrationInterval);
            }
        }
        boolean hasNoTasks = !queue.hasMigrationTasks();
        if (hasNoTasks) {
            if (migrating) {
                logger.info("All migration tasks have been completed. ("
                        + migrationManager.getStats().formatToString(logger.isFineEnabled()) + ")");
            }
            Thread.sleep(sleepTime);
        } else if (!migrationManager.areMigrationTasksAllowed()) {
            Thread.sleep(sleepTime);
        }
    }

    private boolean processTask(MigrationRunnable runnable) {
        try {
            if (runnable == null || !running) {
                return false;
            }

            activeTask = runnable;
            runnable.run();
        } catch (Throwable t) {
            logger.warning(t);
        } finally {
            queue.afterTaskCompletion(runnable);
            activeTask = null;
        }

        return true;
    }

    MigrationRunnable getActiveTask() {
        return activeTask;
    }

    /**
     * Interrupts the migration thread and joins on it.
     * <strong>Must not be called on the migration thread itself</strong> because it will result in infinite blocking.
     */
    void stopNow() {
        assert currentThread() != this : "stopNow must not be called on the migration thread";
        running = false;
        queue.clear();
        interrupt();
        boolean currentThreadInterrupted = false;
        while (true) {
            try {
                join();
            } catch (InterruptedException e) {
                currentThreadInterrupted = true;
                continue;
            }
            break;
        }
        if (currentThreadInterrupted) {
            currentThread().interrupt();
        }
    }

}
