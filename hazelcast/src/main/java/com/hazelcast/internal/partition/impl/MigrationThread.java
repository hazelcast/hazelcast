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

package com.hazelcast.internal.partition.impl;

import com.hazelcast.instance.HazelcastThreadGroup;
import com.hazelcast.instance.OutOfMemoryErrorDispatcher;
import com.hazelcast.logging.ILogger;

import java.util.concurrent.TimeUnit;

import static java.lang.Math.max;

/**
 * TODO: Javadoc Pending...
 *
 */
class MigrationThread extends Thread implements Runnable {
    private final MigrationManager migrationManager;
    private final MigrationQueue queue;
    private final ILogger logger;
    private final long partitionMigrationInterval;
    private final long sleepTime;

    MigrationThread(MigrationManager migrationManager, HazelcastThreadGroup hazelcastThreadGroup, ILogger logger) {
        super(hazelcastThreadGroup.getInternalThreadGroup(), hazelcastThreadGroup.getThreadNamePrefix("migration"));

        this.migrationManager = migrationManager;
        queue = migrationManager.migrationQueue;
        partitionMigrationInterval = migrationManager.partitionMigrationInterval;
        sleepTime = max(250L, partitionMigrationInterval);
        this.logger = logger;
    }

    @Override
    public void run() {
        try {
            while (!isInterrupted()) {
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

    private void doRun() throws InterruptedException {
        boolean migrating = false;
        for (; ; ) {
            if (!migrationManager.isMigrationAllowed()) {
                break;
            }
            Runnable r = queue.poll(1, TimeUnit.SECONDS);
            if (r == null) {
                break;
            }

            migrating |= r instanceof MigrationManager.MigrateTask;
            processTask(r);
            if (partitionMigrationInterval > 0) {
                Thread.sleep(partitionMigrationInterval);
            }
        }
        boolean hasNoTasks = !queue.hasMigrationTasks();
        if (hasNoTasks) {
            if (migrating) {
                logger.info("All migration tasks have been completed, queues are empty.");
            }
            migrationManager.evictCompletedMigrations();
            Thread.sleep(sleepTime);
        } else if (!migrationManager.isMigrationAllowed()) {
            Thread.sleep(sleepTime);
        }
    }

    boolean processTask(Runnable r) {
        try {
            if (r == null || isInterrupted()) {
                return false;
            }

            r.run();
        } catch (Throwable t) {
            logger.warning(t);
        } finally {
            queue.afterTaskCompletion(r);
        }

        return true;
    }

    void stopNow() {
        queue.clear();
        interrupt();
    }

}
