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

package com.hazelcast.jet.impl.executor.processor;

import com.hazelcast.jet.api.executor.BalancedExecutor;
import com.hazelcast.jet.api.executor.BalancedWorkingProcessor;
import com.hazelcast.jet.api.executor.Task;
import com.hazelcast.logging.ILogger;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

public class SharedBalancedExecutorProcessor
        extends AbstractExecutorProcessor<BalancedExecutor>
        implements BalancedWorkingProcessor {
    protected final AtomicReference<BalancedWorkingProcessor> unLoadedBalancingProcessor =
            new AtomicReference<BalancedWorkingProcessor>(null);
    protected final Queue<Task> incomingBalancingTask = new ConcurrentLinkedQueue<Task>();
    protected volatile boolean balanced = true;
    protected volatile boolean balancingIncoming;

    public SharedBalancedExecutorProcessor(int threadNum,
                                           ILogger logger,
                                           BalancedExecutor taskExecutor) {
        super(threadNum, logger, taskExecutor);
    }

    @Override
    public void setBalancingIncoming(boolean balancingIncoming) {
        this.balancingIncoming = balancingIncoming;
    }

    public void start() {
        super.start();
        this.balanced = true;
    }

    @Override
    public boolean balanceWith(BalancedWorkingProcessor unBalancedProcessor) {
        return this.unLoadedBalancingProcessor.compareAndSet(null, unBalancedProcessor);
    }

    @Override
    public void setBalanced(boolean balanced) {
        this.balanced = balanced;
    }

    @Override
    protected void onTaskDeactivation() {
        setBalanced(false);
        this.taskExecutor.onTaskDeactivation();
        System.out.println("this.taskExecutor.onTaskDeactivation();="
                + this.taskExecutor.getTaskCount() + " idx=" + this
        );
    }

    @Override
    protected void checkExecutorActivity() {
        if (this.taskExecutor.getTaskCount() == 0) {
            try {
                this.lockingQueue.take();
            } catch (InterruptedException e) {
                this.logger.finest(e.getMessage(), e);
            }
        }
    }

    protected boolean checkBalancingIncoming() {
        if (this.balancingIncoming) {
            this.balancingIncoming = false;
            readIncomingTasks(this.incomingBalancingTask, false);
            return true;
        } else {
            return false;
        }
    }

    @Override
    protected boolean execute() throws Exception {
        balance();
        checkBalancingIncoming();
        return super.execute();
    }

    private void balance() {
        if (!this.taskExecutor.isBalanced()) {
            BalancedWorkingProcessor unLoadedBalancer
                    = this.unLoadedBalancingProcessor.getAndSet(null);

            if (unLoadedBalancer != null) {
                if (executeBalancing(unLoadedBalancer)) {
                    return;
                }
            }

            this.taskExecutor.setBalanced();
        } else if (!this.balanced) {
            this.balanced = this.taskExecutor.registerUnBalanced(this);
        }
    }

    private boolean executeBalancing(BalancedWorkingProcessor unLoadedBalancer) {
        int tasksSize = this.tasks.size();
        int delta = tasksSize - unLoadedBalancer.getWorkingTaskCount();

        if (delta >= 2) {
            for (int i = 0; i < delta / 2; i++) {
                if (this.tasks.size() > 0) {
                    unLoadedBalancer.acceptIncomingBalancingTask(
                            this.tasks.remove(this.tasks.size() - 1)
                    );

                    this.workingTaskCount.decrementAndGet();
                }
            }

            /*
                Despite just a flag we also provide memory barrier -
                all memory which were changed by current thread (by all outcome tasks)
                Will be visible in accepted thread because of reading of this.hasIncoming variable
            */
            unLoadedBalancer.setBalancingIncoming(true);
        } else {
            unLoadedBalancer.setBalanced(false);

            if (!checkBalancingIncoming()) {
                this.taskExecutor.setBalanced();
            }

            return true;
        }

        return false;
    }

    @Override
    public void acceptIncomingBalancingTask(Task task) {
        this.incomingBalancingTask.offer(task);
        this.balancingIncoming = true;
    }
}
