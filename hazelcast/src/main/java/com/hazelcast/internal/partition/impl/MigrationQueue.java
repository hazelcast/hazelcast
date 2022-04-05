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

package com.hazelcast.internal.partition.impl;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manages migration tasks and migration status flag for {@link InternalPartitionServiceImpl} safely.
 * Once a migration task is added to the queue, queue has to be notified
 * via {@link MigrationQueue#afterTaskCompletion(MigrationRunnable)} after its execution.
 */
class MigrationQueue {

    private final AtomicInteger migrateTaskCount = new AtomicInteger();

    private final BlockingQueue<MigrationRunnable> queue = new LinkedBlockingQueue<>();

    @SuppressFBWarnings(value = "RV_RETURN_VALUE_IGNORED",
            justification = "offer will always be successful since queue is unbounded")
    public void add(MigrationRunnable task) {
        migrateTaskCount.incrementAndGet();
        queue.offer(task);
    }

    public MigrationRunnable poll(int timeout, TimeUnit unit)
            throws InterruptedException {
        return queue.poll(timeout, unit);
    }

    public void clear() {
        List<MigrationRunnable> sink = new ArrayList<>();
        queue.drainTo(sink);

        for (MigrationRunnable task : sink) {
            afterTaskCompletion(task);
        }
    }

    /**
     * Marks a task as completed.
     *
     * @throws IllegalStateException if the migration task count was reduced below 0
     */
    public void afterTaskCompletion(MigrationRunnable task) {
        if (migrateTaskCount.decrementAndGet() < 0) {
            throw new IllegalStateException();
        }
    }

    public int migrationTaskCount() {
        return migrateTaskCount.get();
    }

    public boolean hasMigrationTasks() {
        return migrateTaskCount.get() > 0;
    }

    @Override
    public String toString() {
        return "MigrationQueue{" + "migrateTaskCount=" + migrateTaskCount
                + ", queue=" + queue + '}';
    }

}
