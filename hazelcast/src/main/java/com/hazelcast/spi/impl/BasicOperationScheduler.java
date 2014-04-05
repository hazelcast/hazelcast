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
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.util.executor.AbstractExecutorThreadFactory;

import java.util.Queue;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

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
    private final BasicOperationProcessor processor;

    //all operations for specific partitions will be executed on these threads, .e.g map.put(key,value).
    private final OperationThread[] partitionOperationThreads;

    //all operations that are not specific for a partition will be executed here, e.g heartbeat or map.size
    private final OperationThread[] genericOperationThreads;

    //The genericOperationRandom is used when a generic operation is scheduled, and a generic OperationThread
    //needs to be selected.
    //todo:
    //We could have a look at the ThreadLocalRandom, but it requires java 7. So some kind of reflection
    //could to the trick to use something less painful.
    private final Random genericOperationRandom = new Random();

    //The trigger is used when a priority message is send and offered to the operation-thread priority queue.
    //To wakeup the thread, a priorityTaskTrigger is send to the regular blocking queue to wake up the operation
    //thread.
    private final Runnable priorityTaskTrigger = new Runnable() {
        @Override
        public void run() {
        }

        @Override
        public String toString() {
            return "TriggerTask";
        }
    };

    public BasicOperationScheduler(Node node,
                                   ExecutionService executionService,
                                   BasicOperationProcessor processor) {
        this.logger = node.getLogger(BasicOperationScheduler.class);
        this.node = node;
        this.processor = processor;

        this.genericOperationThreads = new OperationThread[getGenericOperationThreadCount()];
        for (int threadId = 0; threadId < genericOperationThreads.length; threadId++) {
            OperationThread operationThread = createGenericOperationThread(threadId);
            genericOperationThreads[threadId] = operationThread;
            operationThread.start();
        }

        this.partitionOperationThreads = new OperationThread[getPartitionOperationThreadCount()];
        for (int threadId = 0; threadId < partitionOperationThreads.length; threadId++) {
            OperationThread operationThread = createPartitionOperationThread(threadId);
            partitionOperationThreads[threadId] = operationThread;
            operationThread.start();
        }
    }

    private int getGenericOperationThreadCount() {
        int threadCount = node.getGroupProperties().GENERIC_OPERATION_THREAD_COUNT.getInteger();
        if (threadCount <= 0) {
            int coreSize = Runtime.getRuntime().availableProcessors();
            threadCount = coreSize * 2;
        }
        return threadCount;
    }

    private int getPartitionOperationThreadCount() {
        int threadCount = node.getGroupProperties().PARTITION_OPERATION_THREAD_COUNT.getInteger();
        if (threadCount <= 0) {
            int coreSize = Runtime.getRuntime().availableProcessors();
            threadCount = coreSize * 2;
        }
        return threadCount;
    }

    private OperationThread createPartitionOperationThread(int operationThreadId) {
        String poolNamePrefix = node.getThreadPoolNamePrefix("partition-operation");
        OperationThreadFactory threadFactory = new OperationThreadFactory(poolNamePrefix, operationThreadId);
        return threadFactory.createThread(null);
    }

    private OperationThread createGenericOperationThread(int operationThreadId) {
        String poolNamePrefix = node.getThreadPoolNamePrefix("generic-operation");
        OperationThreadFactory threadFactory = new OperationThreadFactory(poolNamePrefix, operationThreadId);
        return threadFactory.createThread(null);
    }

    boolean isAllowedToRunInCurrentThread(int partitionId) {
        if (partitionId < 0) {
            return true;
        }

        Thread currentThread = Thread.currentThread();
        if (currentThread instanceof OperationThread) {
            int threadId = ((OperationThread) currentThread).threadId;
            return toPartitionThreadIndex(partitionId) == threadId;
        }
        return false;
    }

    boolean isInvocationAllowedFromCurrentThread(int partitionId) {
        Thread currentThread = Thread.currentThread();
        if (currentThread instanceof OperationThread) {
            if (partitionId > -1) {
                int threadId = ((OperationThread) currentThread).threadId;
                return toPartitionThreadIndex(partitionId) == threadId;
            }
            return true;
        }
        return true;
    }

    public int getOperationExecutorQueueSize() {
        int size = 0;

        for (OperationThread t : partitionOperationThreads) {
            size += t.workQueue.size();
        }

        for (OperationThread t : genericOperationThreads) {
            size += t.workQueue.size();
        }

        return size;
    }

    public int getPriorityOperationExecutorQueueSize() {
        int size = 0;

        for (OperationThread t : partitionOperationThreads) {
            size += t.priorityQueue.size();
        }

        for (OperationThread t : genericOperationThreads) {
            size += t.priorityQueue.size();
        }

        return size;
    }

    public void execute(final Object task, int partitionId, boolean priority) {
        if (task == null) {
            throw new NullPointerException();
        }

        OperationThread operationThread = getOperationThread(partitionId);
        if (priority) {
            offerWork(operationThread.priorityQueue, task);
            offerWork(operationThread.workQueue, priorityTaskTrigger);
        } else {
            offerWork(operationThread.workQueue, task);
        }
    }

    private OperationThread getOperationThread(int partitionId) {
        if (partitionId < 0) {
            //the task can be executed on a generic operation thread
            int genericThreadIndex = genericOperationRandom.nextInt(genericOperationThreads.length);
            return genericOperationThreads[genericThreadIndex];
        } else {
            //the task needs to be executed on a partition operation thread.
            int partitionThreadIndex = toPartitionThreadIndex(partitionId);
            return partitionOperationThreads[partitionThreadIndex];
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

    private int toPartitionThreadIndex(int partitionId) {
        return partitionId % partitionOperationThreads.length;
    }

    public void shutdown() {
        shutdown(partitionOperationThreads);
        shutdown(genericOperationThreads);
        awaitTermination(partitionOperationThreads);
        awaitTermination(genericOperationThreads);
    }

    private static void shutdown(OperationThread[] operationThreads) {
        for (OperationThread thread : operationThreads) {
            thread.shutdown();
        }
    }

    private static void awaitTermination(OperationThread[] operationThreads) {
        for (OperationThread thread : operationThreads) {
            try {
                thread.awaitTermination(3, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
            }
        }
    }

    @Override
    public String toString() {
        return "BasicOperationScheduler{" +
                "node=" + node.getThisAddress() +
                '}';
    }

    private class OperationThreadFactory extends AbstractExecutorThreadFactory {

        private final String threadName;
        private final int threadId;

        public OperationThreadFactory(String poolNamePrefix, int threadId) {
            super(node.threadGroup, node.getConfigClassLoader());
            this.threadName = poolNamePrefix + threadId;
            this.threadId = threadId;
        }

        @Override
        protected OperationThread createThread(Runnable r) {
            return new OperationThread(threadName, threadId);
        }
    }

    public final class OperationThread extends Thread {

        final int threadId;
        private final BlockingQueue workQueue = new LinkedBlockingQueue();
        private final Queue priorityQueue = new ConcurrentLinkedQueue();
        private volatile boolean shutdown;
        public volatile Object current;

        public OperationThread(String name, int threadId) {
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
            current = task;
            try {
                processor.process(task);
            } catch (Exception e) {
                logger.severe("Failed tp process task: " + task + " on partitionThread:" + getName());
            }
            current = null;
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
    }

    private static class PoisonPill {
        @Override
        public String toString() {
            return "PoisonPill";
        }
    }
}
