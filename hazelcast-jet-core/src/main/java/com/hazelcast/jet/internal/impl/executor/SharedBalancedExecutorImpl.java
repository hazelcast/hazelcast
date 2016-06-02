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

package com.hazelcast.jet.internal.impl.executor;

import com.hazelcast.jet.internal.api.executor.ApplicationTaskContext;
import com.hazelcast.jet.internal.api.executor.BalancedWorkingProcessor;
import com.hazelcast.jet.internal.api.executor.SharedApplicationExecutor;
import com.hazelcast.jet.internal.api.executor.Task;
import com.hazelcast.jet.internal.api.executor.WorkingProcessor;
import com.hazelcast.jet.internal.impl.executor.processor.SharedBalancedExecutorProcessor;
import com.hazelcast.spi.NodeEngine;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class SharedBalancedExecutorImpl
        extends AbstractExecutorImpl<BalancedWorkingProcessor>
        implements SharedApplicationExecutor {
    private final AtomicReference<WorkingProcessor> unBalancedProcessor =
            new AtomicReference<WorkingProcessor>(null);

    private final AtomicInteger taskCount = new AtomicInteger();

    public SharedBalancedExecutorImpl(String name,
                                      int threadNum,
                                      int awaitingTimeOut,
                                      NodeEngine nodeEngine) {
        super(name, threadNum, awaitingTimeOut, nodeEngine);

        startWorkers();
        startProcessors();
    }

    @Override
    protected BalancedWorkingProcessor[] createWorkingProcessors(int threadNum) {
        return new BalancedWorkingProcessor[threadNum];
    }

    @Override
    protected BalancedWorkingProcessor createWorkingProcessor(int threadNum) {
        return new SharedBalancedExecutorProcessor(threadNum, this.logger, this);
    }

    @Override
    public boolean isBalanced() {
        return this.unBalancedProcessor.get() == null;
    }

    @Override
    public void setBalanced() {
        this.unBalancedProcessor.set(null);
    }

    @Override
    public void submitTaskContext(ApplicationTaskContext context) {
        int idx = 0;

        for (Task task : context.getTasks()) {
            this.processors[idx].consumeTask(task);
            this.taskCount.incrementAndGet();
            idx = (idx + 1) % (this.processors.length);
        }

        wakeUp();
    }

    @Override
    public int getTaskCount() {
        return this.taskCount.get();
    }

    @Override
    public boolean registerUnBalanced(BalancedWorkingProcessor taskProcessor) {
        boolean result = this.unBalancedProcessor.compareAndSet(null, taskProcessor);

        if (!result) {
            return false;
        }

        BalancedWorkingProcessor maxLoadedProcessor = null;

        for (BalancedWorkingProcessor processor : this.processors) {
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

    @Override
    public void onTaskDeactivation() {
        this.taskCount.decrementAndGet();
    }
}
