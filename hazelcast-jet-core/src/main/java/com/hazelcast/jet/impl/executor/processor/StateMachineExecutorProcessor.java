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


import com.hazelcast.jet.impl.executor.TaskConsumer;
import com.hazelcast.jet.impl.executor.TaskExecutor;
import com.hazelcast.jet.impl.executor.Task;
import com.hazelcast.logging.ILogger;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.ArrayList;
import java.util.List;

public class StateMachineExecutorProcessor extends AbstractExecutorProcessor<TaskExecutor>
        implements TaskConsumer {
    protected final List<Task> originTasks = new ArrayList<Task>();

    public StateMachineExecutorProcessor(int threadNum,
                                         ILogger logger,
                                         TaskExecutor taskExecutor) {
        super(threadNum, logger, taskExecutor);
    }

    @Override
    public void consumeTask(Task task) {
        this.originTasks.add(task);
        super.consumeTask(task);
        wakeUp();
    }

    @Override
    @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
    public void start() {
        if (!this.started) {
            this.tasks.clear();
            this.tasks.addAll(this.originTasks);
            this.workingTaskCount.set(this.tasks.size());
            this.incomingTasks.clear();
            this.lockingQueue.offer(true);
        }
    }

    public void run() {
        this.workingThread = Thread.currentThread();

        try {
            while (!this.shutdown) {
                try {
                    if (this.shutdown) {
                        break;
                    }

                    if (!execute()) {
                        await();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Throwable e) {
                    this.logger.warning(e.getMessage(), e);
                }
            }
        } finally {
            this.shutdownFuture.set(true);
        }
    }
}
