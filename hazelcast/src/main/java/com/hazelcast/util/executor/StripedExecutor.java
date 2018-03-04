/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.util.executor;

import com.hazelcast.instance.OutOfMemoryErrorDispatcher;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.logging.ILogger;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.util.HashUtil.hashToIndex;
import static com.hazelcast.util.Preconditions.checkPositive;
import static java.lang.Math.ceil;

/**
 * The StripedExecutor internally uses a stripe of queues and each queue has its own private worker-thread.
 * When a task is 'executed' on the StripedExecutor, the task is checked if it is a StripedRunnable. If it
 * is, the right worker is looked up and the task put in the queue of that worker. If the task is not a
 * StripedRunnable, a random worker is looked up.
 *
 * If the queue is full and the runnable implements TimeoutRunnable, then a configurable amount of blocking is
 * done on the queue. If the runnable doesn't implement TimeoutRunnable or when the blocking times out,
 * then the task is rejected and a RejectedExecutionException is thrown.
 */
public final class StripedExecutor implements Executor {

    public static final AtomicLong THREAD_ID_GENERATOR = new AtomicLong();

    private final int size;
    private final ILogger logger;
    private final Worker[] workers;
    private final Random rand = new Random();

    private volatile boolean live = true;

    public StripedExecutor(ILogger logger, String threadNamePrefix, int threadCount, int maximumQueueCapacity) {
        checkPositive(threadCount, "threadCount should be positive but found " + threadCount);
        checkPositive(maximumQueueCapacity, "maximumQueueCapacity should be positive but found " + maximumQueueCapacity);

        this.logger = logger;
        this.size = threadCount;
        this.workers = new Worker[threadCount];

        // `maximumQueueCapacity` is the given max capacity for this executor. Each worker in this executor should consume
        // only a portion of that capacity. Otherwise we will have `threadCount * maximumQueueCapacity` instead of
        // `maximumQueueCapacity`.
        final int perThreadMaxQueueCapacity = (int) ceil(1D * maximumQueueCapacity / threadCount);
        for (int i = 0; i < threadCount; i++) {
            Worker worker = new Worker(threadNamePrefix, perThreadMaxQueueCapacity);
            worker.start();
            workers[i] = worker;
        }
    }

    /**
     * Returns the total number of tasks pending to be executed.
     *
     * @return total work queue size.
     */
    public int getWorkQueueSize() {
        int size = 0;
        for (Worker worker : workers) {
            size += worker.workQueue.size();
        }
        return size;
    }

    /**
     * Returns the total number of processed events.
     */
    public long processedCount() {
        long size = 0;
        for (Worker worker : workers) {
            size += worker.processed.inc();
        }
        return size;
    }

    /**
     * Shuts down this StripedExecutor.
     * <p>
     * No checking is done to see if the StripedExecutor already is shut down, so it should be called only once.
     * <p>
     * If there is any pending work, it will be thrown away.
     */
    public void shutdown() {
        live = false;

        for (Worker worker : workers) {
            worker.workQueue.clear();
            worker.interrupt();
        }
    }

    /**
     * Checks if this StripedExecutor is alive (so not shut down).
     *
     * @return live (true)
     */
    public boolean isLive() {
        return live;
    }

    @Override
    public void execute(Runnable command) {
        if (command == null) {
            throw new NullPointerException("command can't be null");
        }

        if (!live) {
            throw new RejectedExecutionException("Executor is terminated!");
        }

        Worker worker = getWorker(command);
        worker.schedule(command);
    }

    private Worker getWorker(Runnable command) {
        final int key;
        if (command instanceof StripedRunnable) {
            key = ((StripedRunnable) command).getKey();
        } else {
            key = rand.nextInt();
        }

        int index = hashToIndex(key, size);
        return workers[index];
    }

    public List<BlockingQueue<Runnable>> getWorkQueues() {
        List<BlockingQueue<Runnable>> workQueues = new ArrayList<BlockingQueue<Runnable>>(workers.length);
        for (Worker worker : workers) {
            workQueues.add(worker.workQueue);
        }
        return workQueues;
    }

    // used in tests
    Worker[] getWorkers() {
        return workers;
    }

    final class Worker extends Thread {

        private final BlockingQueue<Runnable> workQueue;
        private final SwCounter processed = SwCounter.newSwCounter();
        private final int queueCapacity;

        private Worker(String threadNamePrefix, int queueCapacity) {
            super(threadNamePrefix + "-" + THREAD_ID_GENERATOR.incrementAndGet());
            this.workQueue = new LinkedBlockingQueue<Runnable>(queueCapacity);
            this.queueCapacity = queueCapacity;
        }

        private void schedule(Runnable command) {
            long timeout = 0;
            TimeUnit timeUnit = TimeUnit.SECONDS;
            if (command instanceof TimeoutRunnable) {
                TimeoutRunnable timeoutRunnable = ((TimeoutRunnable) command);
                timeout = timeoutRunnable.getTimeout();
                timeUnit = timeoutRunnable.getTimeUnit();
            }

            boolean offered;
            try {
                if (timeout == 0) {
                    offered = workQueue.offer(command);
                } else {
                    offered = workQueue.offer(command, timeout, timeUnit);
                }
            } catch (InterruptedException e) {
                currentThread().interrupt();
                throw new RejectedExecutionException("Thread is interrupted while offering work");
            }

            if (!offered) {
                throw new RejectedExecutionException("Task: " + command + " is rejected, the worker queue is full!");
            }
        }

        @Override
        public void run() {
            for (; ; ) {
                try {
                    try {
                        Runnable task = workQueue.take();
                        process(task);
                    } catch (InterruptedException e) {
                        if (!live) {
                            return;
                        }
                    }
                } catch (Throwable t) {
                    //This should not happen because the process method is protected against failure.
                    //So if this happens, something very seriously is going wrong.
                    logger.severe(getName() + " caught an exception", t);
                }
            }
        }

        private void process(Runnable task) {
            processed.inc();
            try {
                task.run();
            } catch (Throwable e) {
                OutOfMemoryErrorDispatcher.inspectOutOfMemoryError(e);
                logger.severe(getName() + " caught an exception while processing task:" + task, e);
            }
        }

        // used in tests.
        int getQueueCapacity() {
            return queueCapacity;
        }
    }
}
