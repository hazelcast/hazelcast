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

package com.hazelcast.util.executor;

import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;

import java.util.Collection;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author mdogan 2/18/13
 */
public final class CachedExecutorServiceDelegate implements ExecutorService, ManagedExecutorService {

    private final AtomicLong executedCount = new AtomicLong();
    private final String name;
    private final int maxPoolSize;
    private final ExecutorService cachedExecutor;
    private final NodeEngine nodeEngine;
    private final BlockingQueue<Runnable> taskQ;
    private final Lock lock = new ReentrantLock();
    private volatile int size;

    public CachedExecutorServiceDelegate(NodeEngine nodeEngine, String name, ExecutorService cachedExecutor,
            int maxPoolSize) {
        this(nodeEngine, name, cachedExecutor, maxPoolSize, Integer.MAX_VALUE);
    }

    public CachedExecutorServiceDelegate(NodeEngine nodeEngine, String name, ExecutorService cachedExecutor,
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
        this.nodeEngine = nodeEngine;
    }

    public String getName() {
        return name;
    }

    public long getCompletedTaskCount() {
        return executedCount.get();
    }

    public int getMaximumPoolSize() {
        return maxPoolSize;
    }

    public int getPoolSize() {
        return size;
    }

    public int getQueueSize() {
        return taskQ.size();  // LBQ size handled by an atomic int
    }

    public int getRemainingQueueCapacity() {
        return taskQ.remainingCapacity();
    }

    public void execute(Runnable command) {
        if (!taskQ.offer(command)) {
            throw new RejectedExecutionException("Executor[" + name + "] is overloaded!");
        }
        addNewWorkerIfRequired();
    }

    public <T> Future<T> submit(Callable<T> task) {
        final RunnableFuture<T> rf = new CompletableFutureTask<T>(task, getAsyncExecutor());
        execute(rf);
        return rf;
    }

    public <T> Future<T> submit(Runnable task, T result) {
        final RunnableFuture<T> rf = new CompletableFutureTask<T>(task, result, getAsyncExecutor());
        execute(rf);
        return rf;
    }

    public Future<?> submit(Runnable task) {
        return submit(task, null);
    }

    private void addNewWorkerIfRequired() {
        if (size < maxPoolSize) {
            try {
                if (lock.tryLock(250, TimeUnit.MILLISECONDS)) {
                    try {
                        if (size < maxPoolSize && getQueueSize() > 0) {
                            size++;
                            cachedExecutor.execute(new Worker());
                        }
                    } finally {
                        lock.unlock();
                    }
                }
            } catch (InterruptedException ignored) {
            }
        }
    }

       public void shutdown() {
        taskQ.clear();
    }

    public List<Runnable> shutdownNow() {
        shutdown();
        return null;
    }

    public boolean isShutdown() {
        return false;
    }

    public boolean isTerminated() {
        return false;
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return false;
    }

    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        throw new UnsupportedOperationException();
    }

    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        throw new UnsupportedOperationException();
    }

    private ExecutorService getAsyncExecutor() {
        return nodeEngine.getExecutionService().getExecutor(ExecutionService.ASYNC_EXECUTOR);
    }

    private class Worker implements Runnable {

        public void run() {
            try {
                Runnable r;
                do {
                    r = taskQ.poll(1, TimeUnit.MILLISECONDS);
                    if (r != null) {
                        r.run();
                        executedCount.incrementAndGet();
                    }
                }
                while (r != null);
            } catch (InterruptedException ignored) {
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
