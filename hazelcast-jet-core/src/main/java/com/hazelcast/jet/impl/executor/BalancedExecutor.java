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

import com.hazelcast.jet.impl.executor.processor.BalancedExecutorProcessor;
import com.hazelcast.jet.impl.executor.processor.ExecutorProcessor;
import com.hazelcast.spi.NodeEngine;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class BalancedExecutor extends AbstractExecutor<BalancedExecutorProcessor> {
    private final AtomicReference<ExecutorProcessor> unBalancedProcessor = new AtomicReference<ExecutorProcessor>(null);

    private final AtomicInteger taskCount = new AtomicInteger();

    public BalancedExecutor(String name, int threadNum, int awaitingTimeOut, NodeEngine nodeEngine) {
        super(name, threadNum, awaitingTimeOut, nodeEngine);

        startWorkers();
        startProcessors();
    }

    @Override
    protected BalancedExecutorProcessor[] createWorkingProcessors(int threadNum) {
        return new BalancedExecutorProcessor[threadNum];
    }

    @Override
    protected BalancedExecutorProcessor createWorkingProcessor(int threadNum) {
        return new BalancedExecutorProcessor(threadNum, this.logger, this);
    }

    /**
     * @return - true if executor is balanced, false -otherwise;
     */
    public boolean isBalanced() {
        return this.unBalancedProcessor.get() == null;
    }

    /**
     * Set flag to executor - that it was balanced;
     */
    public void setBalanced() {
        this.unBalancedProcessor.set(null);
    }

    /**
     * Submit tasks represented by taskContext to be executed;
     *
     * @param context - corresponding task-context;
     */
    public void submitTaskContext(ApplicationTaskContext context) {
        int idx = 0;

        for (Task task : context.getTasks()) {
            this.processors[idx].consumeTask(task);
            this.taskCount.incrementAndGet();
            idx = (idx + 1) % (this.processors.length);
        }

        wakeUp();
    }

    /**
     * @return - number of task in executor;
     */
    public int getTaskCount() {
        return this.taskCount.get();
    }

    /**
     * Register processor which will be used to re-balance tasks among another threads;
     *
     * @param taskProcessor - corresponding processor
     * @return - true - processor has been successfully registered, false - otherwise;
     */
    public boolean registerUnbalanced(BalancedExecutorProcessor taskProcessor) {
        boolean result = this.unBalancedProcessor.compareAndSet(null, taskProcessor);

        if (!result) {
            return false;
        }

        BalancedExecutorProcessor maxLoadedProcessor = null;

        for (BalancedExecutorProcessor processor : this.processors) {
            if (processor == taskProcessor) {
                continue;
            }

            if (maxLoadedProcessor == null) {
                maxLoadedProcessor = processor;
            } else if (maxLoadedProcessor.getWorkingTaskCount() < processor.getWorkingTaskCount()) {
                maxLoadedProcessor = processor;
            }
        }

        if ((maxLoadedProcessor != null)
                && (maxLoadedProcessor.getWorkingTaskCount() - taskProcessor.getWorkingTaskCount() > 2)) {
            return maxLoadedProcessor.balanceWith(taskProcessor);
        } else {
            this.unBalancedProcessor.set(null);
            return true;
        }
    }

    /**
     * Will be invoked before task will be deleted from executor;
     */
    public void onTaskDeactivation() {
        this.taskCount.decrementAndGet();
    }

}
