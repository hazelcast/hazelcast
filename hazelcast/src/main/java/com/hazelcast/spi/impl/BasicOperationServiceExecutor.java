package com.hazelcast.spi.impl;

import com.hazelcast.instance.Node;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.util.executor.AbstractExecutorThreadFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.instance.OutOfMemoryErrorDispatcher.onOutOfMemory;

public class BasicOperationServiceExecutor {

    private final ConcurrentLinkedQueue[] partitionExecutorPriorityQueues;
    private final ExecutorService[] partitionExecutors;
    private final BlockingQueue[] partitionExecutorQueues;
    private final Node node;
    private final Executor globalExecutor;
    private final ConcurrentLinkedQueue<Runnable> globalExecutorPriorityQueue;
    private final int operationThreadCount;
    private final Runnable runSystemOperations;

    public BasicOperationServiceExecutor(Node node, ExecutionService executionService,
                                         int operationThreadCount, Runnable runSystemOperations) {
        this.node = node;
        this.runSystemOperations = runSystemOperations;
        this.operationThreadCount = operationThreadCount;
        this.partitionExecutors = new ExecutorService[operationThreadCount];
        this.partitionExecutorQueues = new BlockingQueue[operationThreadCount];
        this.partitionExecutorPriorityQueues = new ConcurrentLinkedQueue[operationThreadCount];
        for (int partitionId = 0; partitionId < partitionExecutors.length; partitionId++) {
            BlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>();
            partitionExecutorQueues[partitionId] = queue;

            ConcurrentLinkedQueue<Runnable> priorityQueue = new ConcurrentLinkedQueue<Runnable>();
            partitionExecutorPriorityQueues[partitionId] = priorityQueue;

            partitionExecutors[partitionId] = new ThreadPoolExecutor(1, 1,
                    0L, TimeUnit.MILLISECONDS,
                    queue,
                    new PartitionThreadFactory(partitionId));
        }

        int coreSize = Runtime.getRuntime().availableProcessors();
        this.globalExecutorPriorityQueue = new ConcurrentLinkedQueue<Runnable>();
        this.globalExecutor = executionService.register(ExecutionService.OPERATION_EXECUTOR,
                coreSize * 2, coreSize * 100000);
    }

    public int getOperationExecutorQueueSize() {
        int size = 0;
        for (BlockingQueue q : partitionExecutorQueues) {
            size += q.size();
        }
        return size;
    }

    public void execute(Runnable task, int partitionId, boolean priority) {
        Executor executor = getExecutor(partitionId);
        if (priority) {
            ConcurrentLinkedQueue<Runnable> priorityQueue = getPriorityQueue(partitionId);
            priorityQueue.offer(task);
            executor.execute(runSystemOperations);
        } else {
            executor.execute(task);
        }
    }

    private Executor getExecutor(int partitionId) {
        if (partitionId > -1) {
            return partitionExecutors[partitionId % operationThreadCount];
        } else {
            return globalExecutor;
        }
    }

    private ConcurrentLinkedQueue<Runnable> getPriorityQueue(int partitionId) {
        if (partitionId > -1) {
            return partitionExecutorPriorityQueues[partitionId % operationThreadCount];
        } else {
            return globalExecutorPriorityQueue;
        }
    }

    public void shutdown() {
        for (ExecutorService executor : partitionExecutors) {
            executor.shutdown();
        }

        for (ExecutorService executor : partitionExecutors) {
            try {
                executor.awaitTermination(3, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
            }
        }
    }

    private class PartitionThreadFactory extends AbstractExecutorThreadFactory {

        final String threadName;
        final int threadId;

        public PartitionThreadFactory(int threadId) {
            super(node.threadGroup, node.getConfigClassLoader());
            String poolNamePrefix = node.getThreadPoolNamePrefix("operation");
            this.threadName = poolNamePrefix + threadId;
            this.threadId = threadId;
        }

        @Override
        protected Thread createThread(Runnable r) {
            return new PartitionThread(threadGroup, r, threadName, threadId);
        }
    }

    public static class PartitionThread extends Thread {

        final int threadId;

        public PartitionThread(ThreadGroup threadGroup, Runnable target, String name, int threadId) {
            super(threadGroup, target, name);
            this.threadId = threadId;
        }

        @Override
        public void run() {
            try {
                super.run();
            } catch (OutOfMemoryError e) {
                onOutOfMemory(e);
            }
        }
    }
}
