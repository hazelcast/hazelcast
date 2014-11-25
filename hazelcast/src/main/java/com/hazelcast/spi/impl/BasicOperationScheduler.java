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

import com.hazelcast.core.PartitionAware;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.NIOThread;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.UrgentSystemOperation;
import com.hazelcast.util.executor.HazelcastManagedThread;

import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.instance.OutOfMemoryErrorDispatcher.inspectOutputMemoryError;

/**
 * The BasicOperationProcessor belongs to the BasicOperationService and is responsible for scheduling
 * operations/packets to the correct threads.
 * <p/>
 * The actual processing of the 'task' that is scheduled, is forwarded to the {@link BasicDispatcher}. So
 * this class is purely responsible for assigning a 'task' to a particular thread.
 * <p/>
 * The {@link #execute(Object, int, boolean)} accepts an Object instead of a runnable to prevent needing to
 * create wrapper runnables around tasks. This is done to reduce the amount of object litter and therefor
 * reduce pressure on the gc.
 * <p/>
 * There are 2 category of operation threads:
 * <ol>
 * <li>partition specific operation threads: these threads are responsible for executing e.g. a map.put.
 * Operations for the same partition, always end up in the same thread.
 * </li>
 * <li>
 * generic operation threads: these threads are responsible for executing operations that are not
 * specific to a partition. E.g. a heart beat.
 * </li>
 * </ol>
 */
public final class BasicOperationScheduler {

    public static final int TERMINATION_TIMEOUT_SECONDS = 3;

    //all operations for specific partitions will be executed on these threads, .e.g map.put(key,value).
    final OperationThread[] partitionOperationThreads;

    //all operations that are not specific for a partition will be executed here, e.g heartbeat or map.size
    final OperationThread[] genericOperationThreads;
    private final ILogger logger;
    private final Node node;
    private final ExecutionService executionService;
    private final BasicDispatcher dispatcher;

    //the generic workqueues are shared between all generic operation threads, so that work can be stolen
    //and a task gets processed as quickly as possible.
    private final BlockingQueue genericWorkQueue = new LinkedBlockingQueue();
    private final ConcurrentLinkedQueue genericPriorityWorkQueue = new ConcurrentLinkedQueue();

    private final ResponseThread responseThread;

    private volatile boolean shutdown;

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
                                   BasicDispatcher dispatcher) {
        this.executionService = executionService;
        this.logger = node.getLogger(BasicOperationScheduler.class);
        this.node = node;
        this.dispatcher = dispatcher;

        this.genericOperationThreads = new OperationThread[getGenericOperationThreadCount()];
        initOperationThreads(genericOperationThreads, new GenericOperationThreadFactory());

        this.partitionOperationThreads = new OperationThread[getPartitionOperationThreadCount()];
        initOperationThreads(partitionOperationThreads, new PartitionOperationThreadFactory());

        this.responseThread = new ResponseThread();
        responseThread.start();

        logger.info("Starting with " + genericOperationThreads.length + " generic operation threads and "
                + partitionOperationThreads.length + " partition operation threads.");
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings({"NP_NONNULL_PARAM_VIOLATION" })
    private static void initOperationThreads(OperationThread[] operationThreads, ThreadFactory threadFactory) {
        for (int threadId = 0; threadId < operationThreads.length; threadId++) {
            OperationThread operationThread = (OperationThread) threadFactory.newThread(null);
            operationThreads[threadId] = operationThread;
            operationThread.start();
        }
    }

    private int getGenericOperationThreadCount() {
        int threadCount = node.getGroupProperties().GENERIC_OPERATION_THREAD_COUNT.getInteger();
        if (threadCount <= 0) {
            // default generic operation thread count
            int coreSize = Runtime.getRuntime().availableProcessors();
            threadCount = Math.max(2, coreSize / 2);
        }
        return threadCount;
    }

    private int getPartitionOperationThreadCount() {
        int threadCount = node.getGroupProperties().PARTITION_OPERATION_THREAD_COUNT.getInteger();
        if (threadCount <= 0) {
            // default partition operation thread count
            int coreSize = Runtime.getRuntime().availableProcessors();
            threadCount = Math.max(2, coreSize);
        }
        return threadCount;
    }

    int getPartitionIdForExecution(Operation op) {
        return op instanceof PartitionAwareOperation ? op.getPartitionId() : -1;
    }

    boolean isAllowedToRunInCurrentThread(Operation op) {
        return isAllowedToRunInCurrentThread(getPartitionIdForExecution(op));
    }

    boolean isInvocationAllowedFromCurrentThread(Operation op) {
        return isInvocationAllowedFromCurrentThread(getPartitionIdForExecution(op));
    }

    private boolean isAllowedToRunInCurrentThread(int partitionId) {
        Thread currentThread = Thread.currentThread();

        // IO threads are not allowed to run any operation
        if (currentThread instanceof NIOThread) {
            return false;
        }

        //todo: do we want to allow non partition specific tasks to be run on a partitionSpecific operation thread?
        if (partitionId < 0) {
            return true;
        }

        //we are only allowed to execute partition aware actions on an OperationThread.
        if (!(currentThread instanceof OperationThread)) {
            return false;
        }

        OperationThread operationThread = (OperationThread) currentThread;
        //if the operationThread is a not a partition specific operation thread, then we are not allowed to execute
        //partition specific operations on it.
        if (!operationThread.isPartitionSpecific) {
            return false;
        }

        //so it is an partition operation thread, now we need to make sure that this operation thread is allowed
        //to execute operations for this particular partitionId.
        int threadId = operationThread.threadId;
        return toPartitionThreadIndex(partitionId) == threadId;
    }

    private boolean isInvocationAllowedFromCurrentThread(int partitionId) {
        Thread currentThread = Thread.currentThread();

        if (currentThread instanceof OperationThread) {
            if (partitionId > -1) {
                OperationThread operationThread = (OperationThread) currentThread;
                if (operationThread.isPartitionSpecific) {
                    int threadId = operationThread.threadId;
                    return toPartitionThreadIndex(partitionId) == threadId;
                }
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

        for (OperationThread t : partitionOperationThreads) {
            size += t.workQueue.size();
        }

        size += genericWorkQueue.size();

        return size;
    }

    public int getPriorityOperationExecutorQueueSize() {
        int size = 0;

        for (OperationThread t : partitionOperationThreads) {
            size += t.priorityWorkQueue.size();
        }

        size += genericPriorityWorkQueue.size();
        return size;
    }

    public int getResponseQueueSize() {
        return responseThread.workQueue.size();
    }

    public void execute(Operation op) {
        String executorName = op.getExecutorName();
        if (executorName == null) {
            int partitionId = getPartitionIdForExecution(op);
            boolean hasPriority = op.isUrgent();
            execute(op, partitionId, hasPriority);
        } else {
            executeOnExternalExecutor(op, executorName);
        }
    }

    public void execute(Runnable task, int partitionId) {
        execute(task, partitionId, false);
    }

    private void executeOnExternalExecutor(Operation op, String executorName) {
        ExecutorService executor = executionService.getExecutor(executorName);
        if (executor == null) {
            throw new IllegalStateException("Could not found executor with name: " + executorName);
        }
        if (op instanceof PartitionAware) {
            throw new IllegalStateException("PartitionAwareOperation " + op + " can't be executed on a "
                    + "custom executor with name: " + executorName);
        }
        if (op instanceof UrgentSystemOperation) {
            throw new IllegalStateException("UrgentSystemOperation " + op + " can't be executed on a custom "
                    + "executor with name: " + executorName);
        }
        executor.execute(new LocalOperationProcessor(op));
    }

    public void execute(Packet packet) {
        try {
            if (packet.isHeaderSet(Packet.HEADER_RESPONSE)) {
                //it is an response packet.
                responseThread.process(packet);
            } else {
                //it is an must be an operation packet
                int partitionId = packet.getPartitionId();
                boolean hasPriority = packet.isUrgent();
                execute(packet, partitionId, hasPriority);
            }
        } catch (RejectedExecutionException e) {
            if (node.nodeEngine.isActive()) {
                throw e;
            }
        }
    }

    private void execute(Object task, int partitionId, boolean priority) {
        if (task == null) {
            throw new NullPointerException();
        }

        BlockingQueue workQueue;
        Queue priorityWorkQueue;
        if (partitionId < 0) {
            workQueue = genericWorkQueue;
            priorityWorkQueue = genericPriorityWorkQueue;
        } else {
            OperationThread partitionOperationThread = partitionOperationThreads[toPartitionThreadIndex(partitionId)];
            workQueue = partitionOperationThread.workQueue;
            priorityWorkQueue = partitionOperationThread.priorityWorkQueue;
        }

        if (priority) {
            offerWork(priorityWorkQueue, task);
            offerWork(workQueue, priorityTaskTrigger);
        } else {
            offerWork(workQueue, task);
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
        shutdown = true;
        interruptAll(partitionOperationThreads);
        interruptAll(genericOperationThreads);
        awaitTermination(partitionOperationThreads);
        awaitTermination(genericOperationThreads);
    }

    private static void interruptAll(OperationThread[] operationThreads) {
        for (OperationThread thread : operationThreads) {
            thread.interrupt();
        }
    }

    private static void awaitTermination(OperationThread[] operationThreads) {
        for (OperationThread thread : operationThreads) {
            try {
                thread.awaitTermination(TERMINATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public String toString() {
        return "BasicOperationScheduler{"
                + "node=" + node.getThisAddress()
                + '}';
    }

    public void dumpPerformanceMetrics(StringBuffer sb) {
        for (int k = 0; k < partitionOperationThreads.length; k++) {
            OperationThread operationThread = partitionOperationThreads[k];
            sb.append(operationThread.getName())
                    .append(" processedCount=").append(operationThread.processedCount)
                    .append(" pendingCount=").append(operationThread.workQueue.size())
                    .append('\n');
        }
        sb.append("pending generic operations ").append(genericWorkQueue.size()).append('\n');
        for (int k = 0; k < genericOperationThreads.length; k++) {
            OperationThread operationThread = genericOperationThreads[k];
            sb.append(operationThread.getName())
                    .append(" processedCount=").append(operationThread.processedCount).append('\n');
        }
        sb.append(responseThread.getName())
                .append(" processedCount: ").append(responseThread.processedResponses)
                .append(" pendingCount: ").append(responseThread.workQueue.size()).append('\n');
    }

    private class GenericOperationThreadFactory implements ThreadFactory {
        private int threadId;

        @Override
        public OperationThread newThread(Runnable ignore) {
            String threadName = node.getThreadPoolNamePrefix("generic-operation") + threadId;
            OperationThread thread = new OperationThread(threadName, false, threadId, genericWorkQueue,
                    genericPriorityWorkQueue);
            threadId++;
            return thread;
        }
    }

    private class PartitionOperationThreadFactory implements ThreadFactory {
        private int threadId;

        @Override
        public Thread newThread(Runnable ignore) {
            String threadName = node.getThreadPoolNamePrefix("partition-operation") + threadId;
            //each partition operation thread, has its own workqueues because operations are partition specific and can't
            //be executed by other threads.
            LinkedBlockingQueue workQueue = new LinkedBlockingQueue();
            ConcurrentLinkedQueue priorityWorkQueue = new ConcurrentLinkedQueue();
            OperationThread thread = new OperationThread(threadName, true, threadId, workQueue, priorityWorkQueue);
            threadId++;
            return thread;
        }
    }

    final class OperationThread extends HazelcastManagedThread {

        private final int threadId;
        private final boolean isPartitionSpecific;
        private final BlockingQueue workQueue;
        private final Queue priorityWorkQueue;
        // This field is updated by this OperationThread (so a single writer) and can be read by other threads.
        private volatile long processedCount;

        public OperationThread(String name, boolean isPartitionSpecific,
                               int threadId, BlockingQueue workQueue, Queue priorityWorkQueue) {
            super(node.threadGroup, name);
            setContextClassLoader(node.getConfigClassLoader());
            this.isPartitionSpecific = isPartitionSpecific;
            this.workQueue = workQueue;
            this.priorityWorkQueue = priorityWorkQueue;
            this.threadId = threadId;
        }

        @Override
        public void run() {
            node.getNodeExtension().onThreadStart(this);
            try {
                doRun();
            } catch (Throwable t) {
                inspectOutputMemoryError(t);
                logger.severe(t);
            } finally {
                node.getNodeExtension().onThreadStop(this);
            }
        }

        private void doRun() {
            for (;;) {
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

        @edu.umd.cs.findbugs.annotations.SuppressWarnings({"VO_VOLATILE_INCREMENT" })
        private void process(Object task) {
            processedCount++;
            try {
                dispatcher.dispatch(task);
            } catch (Throwable e) {
                inspectOutputMemoryError(e);
                logger.severe("Failed to process task: " + task + " on partitionThread:" + getName());
            }
        }

        private void processPriorityMessages() {
            for (;;) {
                Object task = priorityWorkQueue.poll();
                if (task == null) {
                    return;
                }

                process(task);
            }
        }

        public void awaitTermination(int timeout, TimeUnit unit) throws InterruptedException {
            join(unit.toMillis(timeout));
        }
    }

    private class ResponseThread extends Thread {
        private final BlockingQueue<Packet> workQueue = new LinkedBlockingQueue<Packet>();
        // field is only written by the response-thread itself, but can be read by other threads.
        private volatile long processedResponses;

        public ResponseThread() {
            super(node.threadGroup, node.getThreadNamePrefix("response"));
            setContextClassLoader(node.getConfigClassLoader());
        }

        public void run() {
            try {
                doRun();
            } catch (Throwable t) {
                inspectOutputMemoryError(t);
                logger.severe(t);
            }
        }

        private void doRun() {
            for (;;) {
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

                process(task);
            }
        }

        @edu.umd.cs.findbugs.annotations.SuppressWarnings({"VO_VOLATILE_INCREMENT" })
        private void process(Object response) {
            processedResponses++;
            try {
                dispatcher.dispatch(response);
            } catch (Throwable e) {
                inspectOutputMemoryError(e);
                logger.severe("Failed to process response: " + response + " on response thread:" + getName());
            }
        }
    }

    /**
     * Process the operation that has been send locally to this OperationService.
     */
    private final class LocalOperationProcessor implements Runnable {
        private final Operation op;

        private LocalOperationProcessor(Operation op) {
            this.op = op;
        }

        @Override
        public void run() {
            dispatcher.dispatch(op);
        }
    }
}
