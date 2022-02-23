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

package com.hazelcast.durableexecutor.impl;

import com.hazelcast.durableexecutor.impl.operations.PutResultOperation;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.ExecutorStats;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;

import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;

import static com.hazelcast.durableexecutor.impl.DistributedDurableExecutorService.SERVICE_NAME;

public class DurableExecutorContainer {

    private final int durability;
    private final int partitionId;
    private final boolean statisticsEnabled;

    private final String name;
    private final ILogger logger;
    private final TaskRingBuffer ringBuffer;
    private final NodeEngineImpl nodeEngine;
    private final ExecutorStats executorStats;
    private final ExecutionService executionService;

    public DurableExecutorContainer(NodeEngineImpl nodeEngine, String name, int partitionId,
                                    int durability, boolean statisticsEnabled, TaskRingBuffer ringBuffer) {
        this.name = name;
        this.nodeEngine = nodeEngine;
        this.executionService = nodeEngine.getExecutionService();
        this.partitionId = partitionId;
        this.logger = nodeEngine.getLogger(DurableExecutorContainer.class);
        this.durability = durability;
        this.ringBuffer = ringBuffer;
        this.statisticsEnabled = statisticsEnabled;
        this.executorStats = ((DistributedDurableExecutorService) nodeEngine.getService(SERVICE_NAME)).getExecutorStats();
    }

    public int execute(Callable callable) {
        try {
            int sequence = ringBuffer.add(callable);
            TaskProcessor processor = new TaskProcessor(sequence, callable);
            executionService.executeDurable(name, processor);
            return sequence;
        } catch (RejectedExecutionException e) {
            if (statisticsEnabled) {
                executorStats.rejectExecution(name);
            }
            throw e;
        }
    }

    void executeAll() {
        try {
            TaskRingBuffer.DurableIterator iterator = ringBuffer.iterator();
            while (iterator.hasNext()) {
                Object item = iterator.next();
                boolean isCallable = iterator.isTask();
                if (!isCallable) {
                    continue;
                }
                Callable callable = (Callable) item;
                int sequence = iterator.getSequence();
                TaskProcessor processor = new TaskProcessor(sequence, callable);
                executionService.executeDurable(name, processor);
            }
        } catch (RejectedExecutionException e) {
            if (statisticsEnabled) {
                executorStats.rejectExecution(name);
            }
            throw e;
        }
    }

    public void putBackup(int sequence, Callable callable) {
        ringBuffer.putBackup(sequence, callable);
    }

    public Object retrieveResult(int sequence) {
        return ringBuffer.retrieve(sequence);
    }

    public void disposeResult(int sequence) {
        ringBuffer.dispose(sequence);
    }

    public Object retrieveAndDisposeResult(int sequence) {
        return ringBuffer.retrieveAndDispose(sequence);
    }

    public void putResult(int sequence, Object result) {
        ringBuffer.replaceTaskWithResult(sequence, result);
    }

    public boolean shouldWait(int sequence) {
        return ringBuffer.isTask(sequence);
    }

    public TaskRingBuffer getRingBuffer() {
        return ringBuffer;
    }

    public int getDurability() {
        return durability;
    }

    public String getName() {
        return name;
    }

    public final class TaskProcessor extends FutureTask implements Runnable {

        private final int sequence;
        private final long creationTime = Clock.currentTimeMillis();
        private final String callableString;

        private TaskProcessor(int sequence, Callable callable) {
            //noinspection unchecked
            super(callable);
            this.callableString = String.valueOf(callable);
            this.sequence = sequence;

            if (statisticsEnabled) {
                executorStats.startPending(name);
            }
        }

        @Override
        public void run() {
            long start = Clock.currentTimeMillis();
            if (statisticsEnabled) {
                executorStats.startExecution(name, start - creationTime);
            }

            Object response = null;
            try {
                super.run();
                if (!isCancelled()) {
                    response = get();
                }
            } catch (Exception e) {
                logger.warning("While executing callable: " + callableString, e);
                response = e;
            } finally {
                if (!isCancelled()) {
                    setResponse(response);
                    if (statisticsEnabled) {
                        executorStats.finishExecution(name, Clock.currentTimeMillis() - start);
                    }
                }
            }
        }

        private void setResponse(Object response) {
            OperationService operationService = nodeEngine.getOperationService();
            Operation op = new PutResultOperation(name, sequence, response);
            operationService.createInvocationBuilder(SERVICE_NAME, op, partitionId)
                    .setCallTimeout(Long.MAX_VALUE)
                    .invoke();
        }
    }
}
