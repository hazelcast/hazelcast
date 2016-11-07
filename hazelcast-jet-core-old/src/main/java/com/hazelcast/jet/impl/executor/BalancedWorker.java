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

import com.hazelcast.logging.ILogger;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

public class BalancedWorker extends Worker {

    private final AtomicReference<BalancedWorker> availableWorker = new AtomicReference<>(null);
    private final Queue<Task> additionalTasks = new ConcurrentLinkedQueue<>();
    private final BalancedExecutor executor;

    private volatile boolean isAvailable;
    private volatile boolean hasIncomingAdditionalTasks;


    public BalancedWorker(ILogger logger, BalancedExecutor executor) {
        super(logger);
        this.executor = executor;
    }

    /**
     * Notify the worker that there are additional tasks
     */
    public void notifyAdditionalTasks() {
        hasIncomingAdditionalTasks = true;
    }

    public void start() {
        super.start();
        isAvailable = false;
    }

    /**
     * Set worker which is available to offload tasks to
     */
    public boolean setAvailableWorker(BalancedWorker availableWorker) {
        return this.availableWorker.compareAndSet(null, availableWorker);
    }

    /**
     * Set this worker to available state, meaning it is ready to accept more tasks
     */
    public void setAvailable() {
        isAvailable = true;
    }

    public void consumeAdditionalTask(Task task) {
        additionalTasks.offer(task);
        hasIncomingAdditionalTasks = true;
    }

    @Override
    protected void onTaskDeactivation() {
        setAvailable();
        executor.onTaskDeactivation();
        logger.fine("executor.onTaskDeactivation();="
                + executor.getTaskCount() + " idx=" + this
        );
    }

    @Override
    protected void checkExecutorActivity() {
        if (executor.getTaskCount() == 0) {
            try {
                lockingQueue.take();
            } catch (InterruptedException e) {
                logger.finest(e.getMessage(), e);
            }
        }
    }

    @Override
    protected boolean execute() throws Exception {
        balance();
        checkAdditionalTasks();
        return super.execute();
    }

    private boolean checkAdditionalTasks() {
        if (hasIncomingAdditionalTasks) {
            hasIncomingAdditionalTasks = false;
            readIncomingTasks(additionalTasks, false);
            return true;
        } else {
            return false;
        }
    }

    private void balance() {
        if (!executor.isAnyWorkerAvailable()) {
            BalancedWorker available = availableWorker.getAndSet(null);

            if (available != null) {
                if (offloadWork(available)) {
                    return;
                }
            }

            executor.resetBalanced();
        } else if (isAvailable) {
            isAvailable = !executor.registerAvailable(this);
        }
    }

    private boolean offloadWork(BalancedWorker targetWorker) {
        int tasksSize = tasks.size();
        int delta = tasksSize - targetWorker.getWorkingTaskCount();

        if (delta >= 2) {
            for (int i = 0; i < delta / 2; i++) {
                if (tasks.size() > 0) {
                    targetWorker.consumeAdditionalTask(tasks.remove(tasks.size() - 1));

                    workingTaskCount.decrementAndGet();
                }
            }

            /*
                Despite just a flag we also provide memory barrier -
                all memory which were changed by current thread (by all outcome tasks)
                Will be visible in accepted thread because of reading of this.hasIncoming variable
            */
            targetWorker.notifyAdditionalTasks();
        } else {
            targetWorker.setAvailable();

            if (!checkAdditionalTasks()) {
                executor.resetBalanced();
            }

            return true;
        }

        return false;
    }
}
