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

package com.hazelcast.internal.util.executor;

import com.hazelcast.instance.impl.OutOfMemoryErrorDispatcher;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.internal.util.EmptyStatement.ignore;
import static com.hazelcast.internal.util.HashUtil.hashToIndex;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkPositive;
import static java.lang.Math.ceil;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * The StripedExecutor internally uses a stripe of queues and each queue has its own private worker-thread.
 * When a task is 'executed' on the StripedExecutor, the task is checked if it is a StripedRunnable. If it
 * is, the right worker is looked up and the task put in the queue of that worker. If the task is not a
 * StripedRunnable, a random worker is looked up.
 * <p>
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

    public StripedExecutor(ILogger logger,
                           String threadNamePrefix,
                           int threadCount,
                           int queueCapacity) {
        this(logger, threadNamePrefix, threadCount, queueCapacity, false);
    }

    public StripedExecutor(ILogger logger,
                           String threadNamePrefix,
                           int threadCount,
                           int queueCapacity,
                           boolean lazyThreads) {
        checkPositive("threadCount", threadCount);
        checkPositive("queueCapacity", queueCapacity);

        this.logger = logger;
        this.size = threadCount;
        this.workers = new Worker[threadCount];

        // `queueCapacity` is the given max capacity for this executor. Each worker in this executor should consume
        // only a portion of that capacity. Otherwise we will have `threadCount * queueCapacity` instead of
        // `queueCapacity`.
        int perThreadMaxQueueCapacity = (int) ceil(1D * queueCapacity / threadCount);
        for (int i = 0; i < threadCount; i++) {
            Worker worker = new Worker(threadNamePrefix, perThreadMaxQueueCapacity);
            if (!lazyThreads) {
                worker.started.set(true);
                worker.start();
            }
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
            size += worker.taskQueue.size();
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
            worker.shutdown();
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
    public void execute(@Nonnull Runnable task) {
        checkNotNull(task, "task can't be null");

        if (!live) {
            throw new RejectedExecutionException("Executor is terminated!");
        }

        Worker worker = getWorker(task);
        worker.schedule(task);
    }

    private Worker getWorker(Runnable task) {
        int key;
        if (task instanceof StripedRunnable) {
            key = ((StripedRunnable) task).getKey();
        } else {
            key = rand.nextInt();
        }

        int index = hashToIndex(key, size);
        return workers[index];
    }

    public List<BlockingQueue<Runnable>> getTaskQueues() {
        List<BlockingQueue<Runnable>> taskQueues = new ArrayList<BlockingQueue<Runnable>>(workers.length);
        for (Worker worker : workers) {
            taskQueues.add(worker.taskQueue);
        }
        return taskQueues;
    }

    // used in tests
    Worker[] getWorkers() {
        return workers;
    }

    final class Worker extends Thread {

        private final BlockingQueue<Runnable> taskQueue;
        private final SwCounter processed = SwCounter.newSwCounter();
        private final int queueCapacity;
        private final AtomicBoolean started = new AtomicBoolean();

        private Worker(String threadNamePrefix, int queueCapacity) {
            super(threadNamePrefix + "-" + THREAD_ID_GENERATOR.incrementAndGet());
            this.taskQueue = new LinkedBlockingQueue<Runnable>(queueCapacity);
            this.queueCapacity = queueCapacity;
        }

        private void schedule(Runnable task) {
            if (!started.get() && started.compareAndSet(false, true)) {
                start();
            }

            long timeoutNanos = timeoutNanos(task);
            try {
                boolean offered = timeoutNanos == 0
                        ? taskQueue.offer(task)
                        : taskQueue.offer(task, timeoutNanos, NANOSECONDS);

                if (!offered) {
                    throw new RejectedExecutionException("Task: " + task + " is rejected, "
                            + "the taskqueue of " + getName() + " is full!");
                }
            } catch (InterruptedException e) {
                currentThread().interrupt();
                throw new RejectedExecutionException("Thread is interrupted while offering work");
            }
        }

        private long timeoutNanos(Runnable task) {
            if (task instanceof TimeoutRunnable) {
                TimeoutRunnable r = ((TimeoutRunnable) task);
                return r.getTimeUnit().toNanos(r.getTimeout());
            } else {
                return 0;
            }
        }

        @Override
        public void run() {
            try {
                while (live) {
                    try {
                        Runnable task = taskQueue.take();
                        process(task);
                    } catch (InterruptedException ignore) {
                        // we can safely ignore this exception since we'll check if the
                        // striped executor is still alive in the next iteration of the loop.
                        ignore(ignore);
                    }
                }
            } catch (Throwable t) {
                //This should not happen because the process method is protected against failure.
                //So if this happens, something very seriously is going wrong.
                logger.severe(getName() + " caught an exception", t);
            }
        }

        private void process(Runnable task) {
            processed.inc();
            try {
                task.run();
            } catch (Throwable e) {
                OutOfMemoryErrorDispatcher.inspectOutOfMemoryError(e);
                logger.severe(getName() + " caught an exception while processing:" + task, e);
            }
        }

        // used in tests.
        int getQueueCapacity() {
            return queueCapacity;
        }

        private void shutdown() {
            taskQueue.clear();
            interrupt();
        }
    }
}
