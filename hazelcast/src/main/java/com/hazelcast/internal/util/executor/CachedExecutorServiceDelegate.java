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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.hazelcast.internal.util.EmptyStatement.ignore;
import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;

public final class CachedExecutorServiceDelegate implements ExecutorService, ManagedExecutorService {

    private static final AtomicLongFieldUpdater<CachedExecutorServiceDelegate> EXECUTED_COUNT =
            newUpdater(CachedExecutorServiceDelegate.class, "executedCount");

    private volatile long executedCount;
    private final String name;
    private final int maxPoolSize;
    private final ExecutorService cachedExecutor;
    private final BlockingQueue<Runnable> taskQ;
    private final Lock lock = new ReentrantLock();
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private volatile int size;

    public CachedExecutorServiceDelegate(String name, ExecutorService cachedExecutor,
                                         int maxPoolSize, int queueCapacity) {
        if (maxPoolSize <= 0) {
            throw new IllegalArgumentException("Max pool size must be positive!");
        }
        if (queueCapacity <= 0) {
            throw new IllegalArgumentException("Queue capacity must be positive!");
        }
        this.name = name;
        this.maxPoolSize = maxPoolSize;
        this.cachedExecutor = cachedExecutor;
        this.taskQ = new LinkedBlockingQueue<Runnable>(queueCapacity);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public long getCompletedTaskCount() {
        return executedCount;
    }

    @Override
    public int getMaximumPoolSize() {
        return maxPoolSize;
    }

    @Override
    public int getPoolSize() {
        return size;
    }

    @Override
    public int getQueueSize() {
        // LBQ size handled by an atomic int
        return taskQ.size();
    }

    @Override
    public int getRemainingQueueCapacity() {
        return taskQ.remainingCapacity();
    }

    @Override
    public void execute(Runnable command) {
        if (shutdown.get()) {
            throw new RejectedExecutionException("Executor[" + name + "] was shut down.");
        }
        if (!taskQ.offer(command)) {
            throw new RejectedExecutionException("Executor[" + name + "] is overloaded!");
        }
        addNewWorkerIfRequired();
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        final RunnableFuture<T> rf = new CompletableFutureTask<T>(task);
        execute(rf);
        return rf;
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        final RunnableFuture<T> rf = new CompletableFutureTask<>(task, result);
        execute(rf);
        return rf;
    }

    @Override
    public Future<?> submit(Runnable task) {
        return submit(task, null);
    }

    @SuppressFBWarnings("VO_VOLATILE_INCREMENT")
    private void addNewWorkerIfRequired() {
        if (size < maxPoolSize) {
            try {
                lock.lockInterruptibly();
                try {
                    if (size < maxPoolSize && getQueueSize() > 0) {
                        size++;
                        cachedExecutor.execute(new Worker());
                    }
                } finally {
                    lock.unlock();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public void shutdown() {
        shutdown.set(true);
    }

    @Override
    public List<Runnable> shutdownNow() {
        if (!shutdown.compareAndSet(false, true)) {
            return Collections.emptyList();
        }
        List<Runnable> tasks = new LinkedList<Runnable>();
        taskQ.drainTo(tasks);
        for (Runnable task : tasks) {
            if (task instanceof RunnableFuture) {
                ((RunnableFuture) task).cancel(false);
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
        return shutdown.get() && taskQ.isEmpty();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks,
                                         long timeout, TimeUnit unit) throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        throw new UnsupportedOperationException();
    }

    private class Worker implements Runnable {

        @Override
        public void run() {
            try {
                Runnable r;
                do {
                    r = taskQ.poll(1, TimeUnit.MILLISECONDS);
                    if (r != null) {
                        r.run();
                        EXECUTED_COUNT.incrementAndGet(CachedExecutorServiceDelegate.this);
                    }
                }
                while (r != null);
            } catch (InterruptedException ignored) {
                ignore(ignored);
            } finally {
                exit();
            }
        }

        void exit() {
            lock.lock();
            try {
                size--;
                if (taskQ.peek() != null) {
                    // may cause underlying cached executor to create some extra threads!
                    addNewWorkerIfRequired();
                }
            } finally {
                lock.unlock();
            }
        }
    }
}
