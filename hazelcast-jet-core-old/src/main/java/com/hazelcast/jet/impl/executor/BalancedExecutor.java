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

package com.hazelcast.jet.impl.executor;

import com.hazelcast.spi.NodeEngine;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Executor which allocates tasks initially by round-robin to each worker,
 * and does load balancing afterwards. When a worker completed a task, it will be allocated other
 * tasks from workers with more tasks in their queue.
 */
public class BalancedExecutor extends AbstractExecutor<BalancedWorker> {
    private final AtomicReference<Worker> availableWorker = new AtomicReference<>(null);

    private final AtomicInteger taskCount = new AtomicInteger();

    public BalancedExecutor(String name, int threadNum, int awaitingTimeOut, NodeEngine nodeEngine) {
        super(name, threadNum, awaitingTimeOut, nodeEngine);

        startThreads();
        startWorkers();
    }

    @Override
    protected BalancedWorker createWorker() {
        return new BalancedWorker(logger, this);
    }

    /**
     * Check if there are any available works to receive tasks that needs additional work
     */
    public boolean isAnyWorkerAvailable() {
        return availableWorker.get() == null;
    }

    /**
     * Reset available worker
     */
    public void resetBalanced() {
        availableWorker.set(null);
    }

    /**
     * Submit tasks represented by taskContext to be executed
     */
    public void submitTaskContext(List<Task> tasks) {
        int idx = 0;

        for (Task task : tasks) {
            workers.get(idx).consumeTask(task);
            taskCount.incrementAndGet();
            idx = (idx + 1) % (workers.size());
        }

        wakeUp();
    }

    /**
     * @return number of task in executor
     */
    public int getTaskCount() {
        return taskCount.get();
    }

    /**
     * Register worker which will be used to re-balance tasks among another threads
     *
     * @param unbalancedWorker corresponding worker
     * @return true worker has been successfully registered, false otherwise
     */
    public boolean registerAvailable(BalancedWorker unbalancedWorker) {
        boolean result = availableWorker.compareAndSet(null, unbalancedWorker);

        if (!result) {
            return false;
        }

        BalancedWorker maxLoadedWorker = null;

        for (BalancedWorker worker : workers) {
            if (worker == unbalancedWorker) {
                continue;
            }

            if (maxLoadedWorker == null) {
                maxLoadedWorker = worker;
            } else if (maxLoadedWorker.getWorkingTaskCount() < worker.getWorkingTaskCount()) {
                maxLoadedWorker = worker;
            }
        }

        if ((maxLoadedWorker != null)
                && (maxLoadedWorker.getWorkingTaskCount() - unbalancedWorker.getWorkingTaskCount() > 2)) {
            return maxLoadedWorker.setAvailableWorker(unbalancedWorker);
        } else {
            this.availableWorker.set(null);
            return true;
        }
    }

    /**
     * Will be invoked before task will be deleted from executor
     */
    public void onTaskDeactivation() {
        this.taskCount.decrementAndGet();
    }

}
