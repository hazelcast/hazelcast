package com.hazelcast.spi.impl;

import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.OperationService;
import com.hazelcast.util.executor.AbstractExecutorThreadFactory;

import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.instance.OutOfMemoryErrorDispatcher.onOutOfMemory;

public class BasicOperationScheduler {

    private final ILogger logger;

    private final Node node;
    private final Executor globalExecutor;
    private final ConcurrentLinkedQueue<Runnable> globalExecutorPriorityQueue;
    private final int operationThreadCount;
    private final BasicOperationProcessor processor;
    private final PartitionThread[] partitionThreads;

    public BasicOperationScheduler(Node node, ExecutionService executionService,
                                   int operationThreadCount, BasicOperationProcessor processor) {
        this.logger = node.getLogger(OperationService.class);
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
        this.globalExecutorPriorityQueue = new ConcurrentLinkedQueue<Runnable>();
        this.globalExecutor = executionService.register(ExecutionService.OPERATION_EXECUTOR,
                coreSize * 2, coreSize * 100000);
    }

    private PartitionThread createPartitionThread(int operationThreadId) {
        BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<Runnable>();
        ConcurrentLinkedQueue<Runnable> priorityQueue = new ConcurrentLinkedQueue<Runnable>();
        PartitionThreadFactory threadFactory = new PartitionThreadFactory(operationThreadId, workQueue, priorityQueue);
        return threadFactory.createThread(null);
    }

    boolean isAllowedToRunInCurrentThread(int partitionId) {
        if (partitionId > -1) {
            final Thread currentThread = Thread.currentThread();
            if (currentThread instanceof PartitionThread) {
                int threadId = ((BasicOperationScheduler.PartitionThread) currentThread).threadId;
                return toPartitionThreadIndex(partitionId) == threadId;
            }
            return false;
        }
        return true;
    }

    boolean isInvocationAllowedFromCurrentThread(int partitionId) {
        final Thread currentThread = Thread.currentThread();
        if (currentThread instanceof PartitionThread) {
            if (partitionId > -1) {
                int threadId = ((BasicOperationScheduler.PartitionThread) currentThread).threadId;
                return toPartitionThreadIndex(partitionId) == threadId;
            }
            return true;
        }
        return true;
    }

    public int getOperationExecutorQueueSize() {
        int size = 0;
        for (PartitionThread t : partitionThreads) {
            size += t.workQueue.size();
            size += t.priorityQueue.size();
        }

        //todo: we don't include the globalExecutor?
        return size;
    }

    public void execute(final Object task, int partitionId, boolean priority) {
        if (task == null) {
            throw new NullPointerException();
        }

        if (partitionId > -1) {
            PartitionThread partitionThread = partitionThreads[toPartitionThreadIndex(partitionId)];
            if (priority) {
                partitionThread.priorityQueue.offer(task);
                partitionThread.workQueue.offer(triggerTask);
            } else {
                partitionThread.workQueue.offer(task);
            }
        } else {
            Runnable runnable;
            if (task instanceof Runnable) {
                runnable = (Runnable) task;
            } else {
                runnable = new Runnable() {
                    @Override
                    public void run() {
                        processor.process(task);
                    }
                };
            }

            if (priority) {

            } else {

            }
            globalExecutor.execute(runnable);
        }
    }

    private final Runnable triggerTask = new Runnable() {
        @Override
        public void run() {
        }
    };

    public class ProcessPriorityQueueTask implements Runnable {
        private final ConcurrentLinkedQueue<Runnable> priorityQueue;
        private final Runnable task;

        public ProcessPriorityQueueTask(ConcurrentLinkedQueue<Runnable> priorityQueue, Runnable task) {
            this.priorityQueue = priorityQueue;
            this.task = task;
        }

        @Override
        public void run() {
            for (; ; ) {
                Runnable task = priorityQueue.poll();
                if (task == null) {
                    break;
                }

                task.run();
            }

            if (task != null) {
                task.run();
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
        private final BlockingQueue workQueue;
        private final Queue priorityQueue;

        public PartitionThreadFactory(int threadId, BlockingQueue workQueue, Queue priorityQueue) {
            super(node.threadGroup, node.getConfigClassLoader());
            String poolNamePrefix = node.getThreadPoolNamePrefix("operation");
            this.threadName = poolNamePrefix + threadId;
            this.threadId = threadId;
            this.workQueue = workQueue;
            this.priorityQueue = priorityQueue;
        }

        @Override
        protected PartitionThread createThread(Runnable r) {
            return new PartitionThread(threadName, threadId, workQueue, priorityQueue);
        }
    }

    public final class PartitionThread extends Thread {

        final int threadId;
        private final BlockingQueue workQueue;
        private final Queue priorityQueue;

        public PartitionThread(String name, int threadId,
                               BlockingQueue workQueue, Queue priorityQueue) {
            super(node.threadGroup, name);
            this.threadId = threadId;
            this.workQueue = workQueue;
            this.priorityQueue = priorityQueue;
        }

        @Override
        public void run() {
            try {
                doRun();
            } catch (OutOfMemoryError e) {
                onOutOfMemory(e);
            }
        }

        private void doRun() {
            for (; ; ) {
                Object task;
                try {
                    task = workQueue.take();
                } catch (InterruptedException e) {
                    continue;
                }

                if (task instanceof PoisonPill) {
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
            workQueue.add(new PoisonPill());
        }

        public void awaitTermination(int timeout, TimeUnit unit) throws InterruptedException {
            join(unit.toMillis(timeout));
        }

        private class PoisonPill {
        }
    }
}
