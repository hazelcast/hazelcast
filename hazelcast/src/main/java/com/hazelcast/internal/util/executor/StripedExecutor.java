/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.internal.util.HashUtil.hashToIndex;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkPositive;
import static java.lang.Math.ceil;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * The StripedExecutor internally uses a stripe of queues and each queue has its own private
 * worker-thread. When a task is 'executed' on the StripedExecutor, the task is checked if it
 * is a StripedRunnable. If it is, the right worker is looked up and the task put in the queue
 * of that worker. If the task is not a StripedRunnable, a random worker is looked up.
 * <p>
 * If the queue is full and the runnable implements TimeoutRunnable, then a configurable amount
 * of blocking is done on the queue. If the runnable doesn't implement TimeoutRunnable or when
 * the blocking times out, then the task is rejected and a RejectedExecutionException is thrown.
 */
public final class StripedExecutor implements ManagedExecutorService {
    public static final AtomicLong THREAD_ID_GENERATOR = new AtomicLong();

    private final String name;
    private final int size;
    private final ILogger logger;
    private final Worker[] workers;
    private final ManagedExecutorService[] specificWorkers;
    private final Random rand = new Random();
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    public StripedExecutor(ILogger logger,
                           String threadNamePrefix,
                           int threadCount,
                           int queueCapacity) {
        this(null, logger, threadNamePrefix, threadCount, queueCapacity, false);
    }

    public StripedExecutor(ILogger logger,
                           String threadNamePrefix,
                           int threadCount,
                           int queueCapacity,
                           boolean lazyThreads) {
        this(null, logger, threadNamePrefix, threadCount, queueCapacity, lazyThreads);
    }

    public StripedExecutor(String name,
                           ILogger logger,
                           String threadNamePrefix,
                           int threadCount,
                           int queueCapacity,
                           boolean lazyThreads) {
        checkPositive("threadCount", threadCount);
        checkPositive("queueCapacity", queueCapacity);

        this.name = name;
        this.logger = logger;
        size = threadCount;
        workers = new Worker[size];
        specificWorkers = new SpecificWorker[size];

        // `queueCapacity` is the given max capacity for this executor. Each worker in this
        // executor should consume only a portion of that capacity. Otherwise, we will have
        // `size * queueCapacity` instead of `queueCapacity`.
        int perThreadMaxQueueCapacity = (int) ceil((double) queueCapacity / size);
        for (int i = 0; i < size; i++) {
            Worker worker = new Worker(threadNamePrefix, perThreadMaxQueueCapacity);
            if (!lazyThreads) {
                worker.started.set(true);
                worker.start();
            }
            workers[i] = worker;
            specificWorkers[i] = new SpecificWorker(worker);
        }
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public int getPoolSize() {
        return size;
    }

    @Override
    public int getMaximumPoolSize() {
        return size;
    }

    @Override
    public long getCompletedTaskCount() {
        long size = 0;
        for (Worker worker : workers) {
            size += worker.processed.get();
        }
        return size;
    }

    @Override
    public int getQueueSize() {
        int size = 0;
        for (Worker worker : workers) {
            size += worker.taskQueue.size();
        }
        return size;
    }

    @Override
    public int getRemainingQueueCapacity() {
        int remaining = 0;
        for (Worker worker : workers) {
            remaining += worker.taskQueue.remainingCapacity();
        }
        return remaining;
    }

    public List<BlockingQueue<Runnable>> getTaskQueues() {
        List<BlockingQueue<Runnable>> taskQueues = new ArrayList<>(workers.length);
        for (Worker worker : workers) {
            taskQueues.add(worker.taskQueue);
        }
        return taskQueues;
    }

    // used in tests
    Worker[] getWorkers() {
        return workers;
    }

    @Override
    public void shutdown() {
        shutdown.set(true);
    }

    @Nonnull
    @Override
    public List<Runnable> shutdownNow() {
        if (!shutdown.compareAndSet(false, true)) {
            return Collections.emptyList();
        }
        List<Runnable> tasks = new ArrayList<>();
        for (Worker worker : workers) {
            worker.taskQueue.drainTo(tasks);
            worker.interrupt();
        }
        for (Runnable task : tasks) {
            if (task instanceof RunnableFuture) {
                ((RunnableFuture<?>) task).cancel(false);
            }
        }
        return tasks;
    }

    @Override
    public boolean isShutdown() {
        return shutdown.get();
    }

    @Override
    public boolean isTerminated() {
        return shutdown.get() && Arrays.stream(workers).allMatch(worker -> worker.taskQueue.isEmpty());
    }

    /**
     * Equivalent to {@code !}{@link #isShutdown()}.
     */
    public boolean isLive() {
        return !shutdown.get();
    }

    @Nonnull
    @Override
    public <T> Future<T> submit(@Nonnull Callable<T> task) {
        RunnableFuture<T> rf = new CompletableFutureTask<>(task);
        execute(rf);
        return rf;
    }

    @Nonnull
    @Override
    public <T> Future<T> submit(@Nonnull Runnable task, T result) {
        RunnableFuture<T> rf = new CompletableFutureTask<>(task, result);
        execute(rf);
        return rf;
    }

    @Nonnull
    @Override
    public Future<?> submit(@Nonnull Runnable task) {
        return submit(task, null);
    }

    @Override
    public void execute(@Nonnull Runnable task) {
        checkNotNull(task, "task can't be null");

        if (shutdown.get()) {
            throw new RejectedExecutionException("Executor is shut down!");
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

    public ManagedExecutorService useSpecificWorker(int key) {
        return specificWorkers[hashToIndex(key, size)];
    }

    @Override
    public boolean awaitTermination(long timeout, @Nonnull TimeUnit unit) {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public <T> List<Future<T>> invokeAll(@Nonnull Collection<? extends Callable<T>> tasks) {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public <T> List<Future<T>> invokeAll(@Nonnull Collection<? extends Callable<T>> tasks, long timeout,
                                         @Nonnull TimeUnit unit) {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public <T> T invokeAny(@Nonnull Collection<? extends Callable<T>> tasks) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T invokeAny(@Nonnull Collection<? extends Callable<T>> tasks, long timeout, @Nonnull TimeUnit unit) {
        throw new UnsupportedOperationException();
    }

    final class Worker extends Thread {
        private final BlockingQueue<Runnable> taskQueue;
        private final SwCounter processed = SwCounter.newSwCounter();
        private final int queueCapacity;
        private final AtomicBoolean started = new AtomicBoolean();

        private Worker(String threadNamePrefix, int queueCapacity) {
            super(threadNamePrefix + "-" + THREAD_ID_GENERATOR.incrementAndGet());
            this.taskQueue = new LinkedBlockingQueue<>(queueCapacity);
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
                TimeoutRunnable r = (TimeoutRunnable) task;
                return r.getTimeUnit().toNanos(r.getTimeout());
            } else {
                return 0;
            }
        }

        @Override
        public void run() {
            try {
                while (!(shutdown.get() && Thread.interrupted())) {
                    try {
                        process(taskQueue.take());
                    } catch (InterruptedException ignored) {
                        // We can safely ignore this exception since we'll check if the
                        // executor is still alive in the next iteration of the loop.
                    }
                }
            } catch (Throwable t) {
                // This should not happen because the process method is protected against
                // failure. So if this happens, something very seriously is going wrong.
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
    }

    private class SpecificWorker implements ManagedExecutorService {
        final Worker worker;

        SpecificWorker(Worker worker) {
            this.worker = worker;
        }

        @Override
        public void execute(@Nonnull Runnable task) {
            checkNotNull(task, "task can't be null");

            if (shutdown.get()) {
                throw new RejectedExecutionException("Executor is shut down!");
            }

            worker.schedule(task);
        }

        @Override
        public String getName() {
            return StripedExecutor.this.name;
        }

        @Override
        public int getPoolSize() {
            return StripedExecutor.this.getPoolSize();
        }

        @Override
        public int getMaximumPoolSize() {
            return StripedExecutor.this.getMaximumPoolSize();
        }

        @Override
        public long getCompletedTaskCount() {
            return StripedExecutor.this.getCompletedTaskCount();
        }

        @Override
        public int getQueueSize() {
            return StripedExecutor.this.getQueueSize();
        }

        @Override
        public int getRemainingQueueCapacity() {
            return StripedExecutor.this.getRemainingQueueCapacity();
        }

        @Override
        public void shutdown() {
            StripedExecutor.this.shutdown();
        }

        @Nonnull
        @Override
        public List<Runnable> shutdownNow() {
            return StripedExecutor.this.shutdownNow();
        }

        @Override
        public boolean isShutdown() {
            return StripedExecutor.this.isShutdown();
        }

        @Override
        public boolean isTerminated() {
            return StripedExecutor.this.isTerminated();
        }

        @Override
        public boolean awaitTermination(long timeout, @Nonnull TimeUnit unit) {
            return StripedExecutor.this.awaitTermination(timeout, unit);
        }

        @Nonnull
        @Override
        public <T> Future<T> submit(@Nonnull Callable<T> task) {
            return StripedExecutor.this.submit(task);
        }

        @Nonnull
        @Override
        public <T> Future<T> submit(@Nonnull Runnable task, T result) {
            return StripedExecutor.this.submit(task, result);
        }

        @Nonnull
        @Override
        public Future<?> submit(@Nonnull Runnable task) {
            return StripedExecutor.this.submit(task);
        }

        @Nonnull
        @Override
        public <T> List<Future<T>> invokeAll(@Nonnull Collection<? extends Callable<T>> tasks) {
            return StripedExecutor.this.invokeAll(tasks);
        }

        @Nonnull
        @Override
        public <T> List<Future<T>> invokeAll(@Nonnull Collection<? extends Callable<T>> tasks, long timeout,
                                             @Nonnull TimeUnit unit) {
            return StripedExecutor.this.invokeAll(tasks, timeout, unit);
        }

        @Nonnull
        @Override
        public <T> T invokeAny(@Nonnull Collection<? extends Callable<T>> tasks) {
            return StripedExecutor.this.invokeAny(tasks);
        }

        @Override
        public <T> T invokeAny(@Nonnull Collection<? extends Callable<T>> tasks, long timeout, @Nonnull TimeUnit unit) {
            return StripedExecutor.this.invokeAny(tasks, timeout, unit);
        }
    }
}
