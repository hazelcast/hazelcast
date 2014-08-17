/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl;

import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.NIOThread;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.util.executor.AbstractExecutorThreadFactory;
import com.hazelcast.util.executor.ExecutorType;
import com.hazelcast.util.executor.HazelcastManagedThread;

import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.instance.OutOfMemoryErrorDispatcher.inspectOutputMemoryError;
import static com.hazelcast.instance.OutOfMemoryErrorDispatcher.onOutOfMemory;

/**
 * The BasicOperationProcessor belongs to the BasicOperationService and is responsible for scheduling
 * operations/packets to the correct threads. They can be assigned to partition specific threads e.g.
 * a map.put, but they can also be assigned to global threads.
 * <p/>
 * The actual processing of the 'task' that is scheduled, is forwarded to the {@link BasicOperationProcessor}. So
 * this class is purely responsible for assigning a 'task' to a particular thread.
 * <p/>
 * The {@link #execute(Object, int, boolean)} accepts an Object instead of a runnable to prevent needing to
 * create wrapper runnables around tasks. This is done to reduce the amount of object litter and therefor
 * reduce pressure on the gc.
 */
public final class BasicOperationScheduler {

    private final ILogger logger;

    private final Node node;
    private final Executor globalExecutor;
    private final ConcurrentLinkedQueue globalExecutorPriorityQueue;
    private final int operationThreadCount;
    private final BasicOperationProcessor processor;
    private final PartitionThread[] partitionThreads;
    private final Runnable triggerTask = new Runnable() {
        @Override
        public void run() {
        }
    };

    public BasicOperationScheduler(Node node, ExecutionService executionService,
                                   int operationThreadCount, BasicOperationProcessor processor) {
        this.logger = node.getLogger(BasicOperationScheduler.class);
        this.node = node;
        this.processor = processor;
        this.operationThreadCount = operationThreadCount;
        this.partitionThreads = new PartitionThread[operationThreadCount];
        for (int operationThreadId = 0; operationThreadId < operationThreadCount; operationThreadId++) {
            PartitionThread partitionThread = createPartitionThread(operationThreadId);
            partitionThreads[operationThreadId] = partitionThread;
            partitionThread.start();
        }

        int coreSize = Runtime.getRuntime().availableProcessors();
        this.globalExecutorPriorityQueue = new ConcurrentLinkedQueue();
        this.globalExecutor = executionService.register(ExecutionService.OPERATION_EXECUTOR,
                coreSize * 2, coreSize * 100000, ExecutorType.CONCRETE);
    }

    private PartitionThread createPartitionThread(int operationThreadId) {
        PartitionThreadFactory threadFactory = new PartitionThreadFactory(operationThreadId);
        return threadFactory.createThread(null);
    }

    boolean isAllowedToRunInCurrentThread(int partitionId) {
        Thread currentThread = Thread.currentThread();

        // IO threads are not allowed to run any operation
        if (currentThread instanceof NIOThread) {
            return false;
        }

        if (partitionId > -1) {
            if (currentThread instanceof PartitionThread) {
                int threadId = ((BasicOperationScheduler.PartitionThread) currentThread).threadId;
                return toPartitionThreadIndex(partitionId) == threadId;
            }
            return false;
        }
        return true;
    }

    boolean isInvocationAllowedFromCurrentThread(int partitionId) {
        Thread currentThread = Thread.currentThread();
        if (currentThread instanceof PartitionThread) {
            if (partitionId > -1) {
                int threadId = ((BasicOperationScheduler.PartitionThread) currentThread).threadId;
                return toPartitionThreadIndex(partitionId) == threadId;
            }
            return true;
        }

        // IO threads are not allowed to run any operation
        if (currentThread instanceof NIOThread) {
            return false;
        }
        return true;
    }

    public int getOperationExecutorQueueSize() {
        int size = 0;
        for (PartitionThread t : partitionThreads) {
            size += t.workQueue.size();
        }

        //todo: we don't include the globalExecutor?
        return size;
    }


    public int getPriorityOperationExecutorQueueSize() {
        int size = 0;
        for (PartitionThread t : partitionThreads) {
            size += t.priorityQueue.size();
        }

       return size;
    }

    public void execute(final Object task, int partitionId, boolean priority) {
        if (task == null) {
            throw new NullPointerException();
        }

        if (partitionId > -1) {
            PartitionThread partitionThread = partitionThreads[toPartitionThreadIndex(partitionId)];

            if (priority) {
                offerWork(partitionThread.priorityQueue, task);
                offerWork(partitionThread.workQueue, triggerTask);
            } else {
                offerWork(partitionThread.workQueue, task);
            }
        } else {
            if (priority) {
                offerWork(globalExecutorPriorityQueue, task);
                globalExecutor.execute(new ProcessTask(null));
            } else {
                globalExecutor.execute(new ProcessTask(task));
            }
        }
    }

    private void offerWork(Queue queue, Object task) {
        //in 3.3 we are going to apply backpressure on overload and then we are going to do something
        //with the return values of the offer methods.
        //Currently the queues are all unbound, so this can't happen anyway.

        boolean offer = queue.offer(task);
        if (!offer) {
            logger.severe("Failed to offer " + task + " to BasicOperationScheduler due to overload");
        }
    }

    private class ProcessTask implements Runnable {
        private final Object task;

        public ProcessTask(Object task) {
            this.task = task;
        }

        @Override
        public void run() {
            try {
                for (; ; ) {
                    Object task = globalExecutorPriorityQueue.poll();
                    if (task == null) {
                        break;
                    }

                    processor.process(task);
                }

                if (task != null) {
                    processor.process(task);
                }
            } catch (Throwable t) {
                inspectOutputMemoryError(t);
                logger.severe(t);
            }
        }
    }

    private int toPartitionThreadIndex(int partitionId) {
        return partitionId % operationThreadCount;
    }

    public void shutdown() {
        for (PartitionThread thread : partitionThreads) {
            thread.shutdown();
        }

        for (PartitionThread thread : partitionThreads) {
            try {
                thread.awaitTermination(3, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
            }
        }
    }

    private class PartitionThreadFactory extends AbstractExecutorThreadFactory {

        private final String threadName;
        private final int threadId;

        public PartitionThreadFactory(int threadId) {
            super(node.threadGroup, node.getConfigClassLoader());
            String poolNamePrefix = node.getThreadPoolNamePrefix("operation");
            this.threadName = poolNamePrefix + threadId;
            this.threadId = threadId;
        }

        @Override
        protected PartitionThread createThread(Runnable r) {
            return new PartitionThread(threadName, threadId);
        }
    }

    public final class PartitionThread extends HazelcastManagedThread {

        final int threadId;
        private final BlockingQueue workQueue = new LinkedBlockingQueue();
        private final Queue priorityQueue = new ConcurrentLinkedQueue();
        private volatile boolean shutdown;

        public PartitionThread(String name, int threadId) {
            super(node.threadGroup, name);
            this.threadId = threadId;
        }

        @Override
        public void run() {
            try {
                doRun();
            } catch (OutOfMemoryError e) {
                onOutOfMemory(e);
            } catch (Throwable t) {
                logger.severe(t);
            }
        }

        private void doRun() {
            for (; ; ) {
                Object task;
                try {
                    task = workQueue.take();
                } catch (InterruptedException e) {
                    if (shutdown) {
                        return;
                    }
                    continue;
                }

                if (shutdown) {
                    return;
                }

                processPriorityMessages();
                process(task);
            }
        }

        private void process(Object task) {
            try {
                processor.process(task);
            } catch (Exception e) {
                logger.severe("Failed tp process task: " + task + " on partitionThread:" + getName());
            }
        }

        private void processPriorityMessages() {
            for (; ; ) {
                Object task = priorityQueue.poll();
                if (task == null) {
                    return;
                }

                process(task);
            }
        }

        private void shutdown() {
            shutdown = true;
            workQueue.add(new PoisonPill());
        }

        public void awaitTermination(int timeout, TimeUnit unit) throws InterruptedException {
            join(unit.toMillis(timeout));
        }

        private class PoisonPill {
        }
    }
}
