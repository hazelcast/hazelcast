package com.hazelcast.spi.impl;

import com.hazelcast.executor.ExecutorThreadFactory;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.OutOfMemoryErrorDispatcher;
import com.hazelcast.instance.ThreadContext;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
* @mdogan 11/20/12
*/
final class ExecutorServiceManager {

    private final NodeServiceImpl nodeService;
    private final ExecutorService cachedExecutorService;
    private final ExecutorService eventExecutorService;
    private final ScheduledExecutorService scheduledExecutorService;
//    private final ExecutorService[] partitionExecutors;
//    private final int partitionThreadCount; // TODO: should be configurable

    ExecutorServiceManager(final NodeServiceImpl nodeService) {
        this.nodeService = nodeService;
        final Node node = nodeService.getNode();
        final ClassLoader classLoader = node.getConfig().getClassLoader();

        cachedExecutorService = new ThreadPoolExecutor(
                3, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>(),
                new ExecutorThreadFactory(node.threadGroup, node.hazelcastInstance,
                        node.getThreadPoolNamePrefix("cached"), classLoader));
        eventExecutorService = Executors.newSingleThreadExecutor(
                new ExecutorThreadFactory(node.threadGroup, node.hazelcastInstance,
                        node.getThreadPoolNamePrefix("event"), node.getConfig().getClassLoader()));
        scheduledExecutorService = Executors.newScheduledThreadPool(2,
                new ExecutorThreadFactory(node.threadGroup,
                        node.hazelcastInstance,
                        node.getThreadPoolNamePrefix("scheduled"), classLoader));

//        final int count = node.groupProperties.PARTITION_THREAD_COUNT.getInteger();
//        partitionThreadCount = count > 0 ? count : Runtime.getRuntime().availableProcessors();
//        partitionExecutors = new ExecutorService[partitionThreadCount];
//        final ThreadFactory partitionThreadFactory = new PartitionThreadFactory("partition");
//        for (int i = 0; i < partitionThreadCount; i++) {
//            partitionExecutors[i] = new ThreadPoolExecutor (
//                    1, 1, 0L, TimeUnit.MILLISECONDS,
//                    new LinkedBlockingQueue<Runnable>(),
//                    partitionThreadFactory,
//                    new RejectedExecutionHandler() {
//                        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
//                        }
//                    }
//            );
//        }
    }

//    ExecutorService getExecutor(int partitionId) {
//        if (partitionId >= 0) {
//            return partitionExecutors[partitionId % partitionThreadCount];
//        } else if (partitionId == NodeService.EXECUTOR_THREAD_ID) {
//            return cachedExecutorService;
//        } else if (partitionId == NodeService.EVENT_THREAD_ID) {
//            return eventExecutorService;
//        } else {
//            throw new IllegalArgumentException("Illegal partition/thread id: " + partitionId);
//        }
//    }
//
    ExecutorService getEventExecutor() {
        return eventExecutorService;
    }

    ExecutorService getCachedExecutor() {
        return cachedExecutorService;
    }

    ScheduledExecutorService getScheduledExecutor() {
        return scheduledExecutorService;
    }

    boolean isPartitionThread() {
        return Thread.currentThread().getClass() == PartitionThread.class;
    }

    void shutdownNow() {
//        for (ExecutorService worker : partitionExecutors) {
//            worker.shutdownNow();
//        }
        cachedExecutorService.shutdownNow();
        scheduledExecutorService.shutdownNow();
        eventExecutorService.shutdownNow();
        try {
            scheduledExecutorService.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
        }
        try {
            cachedExecutorService.awaitTermination(3, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
        }
//        for (ExecutorService worker : partitionExecutors) {
//            try {
//                worker.awaitTermination(3, TimeUnit.SECONDS);
//            } catch (InterruptedException ignored) {
//            }
//        }
    }

    private final class PartitionThreadFactory implements ThreadFactory {
        final String threadName;
        final ThreadGroup partitionThreadGroup ;
        final AtomicInteger threadNumber = new AtomicInteger();

        PartitionThreadFactory(final String threadName) {
            this.threadName = threadName;
            partitionThreadGroup = new ThreadGroup(nodeService.getNode().threadGroup, threadName);
        }

        public Thread newThread(final Runnable r) {
            final String name = threadName + "-" + threadNumber.getAndIncrement();
            final Thread t = new PartitionThread(partitionThreadGroup, r, name);
            t.setContextClassLoader(nodeService.getNode().getConfig().getClassLoader());
            if (t.isDaemon()) {
                t.setDaemon(false);
            }
            if (t.getPriority() != Thread.NORM_PRIORITY) {
                t.setPriority(Thread.NORM_PRIORITY);
            }
            return t;
        }
    }

    private final class PartitionThread extends Thread {

        private PartitionThread(final ThreadGroup group, final Runnable target, final String name) {
            super(group, target, name);
        }

        public void run() {
            ThreadContext.get().setCurrentInstance(nodeService.getNode().hazelcastInstance);
            try {
                super.run();
            } catch (OutOfMemoryError e) {
                OutOfMemoryErrorDispatcher.onOutOfMemory(e);
            } finally {
                try {
                    ThreadContext.shutdown(this);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
