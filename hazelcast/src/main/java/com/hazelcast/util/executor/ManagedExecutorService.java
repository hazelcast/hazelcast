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

import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @mdogan 2/18/13
 */
public final class ManagedExecutorService implements ExecutorService {

    private final String name;
    private final int maxPoolSize;
    private final ExecutorService cachedExecutor;
    private final BlockingQueue<Runnable> taskQ;
    private final Lock lock = new ReentrantLock();
    private volatile int size;

    public ManagedExecutorService(String name, ExecutorService cachedExecutor, int maxPoolSize) {
        this(name, cachedExecutor, maxPoolSize, Integer.MAX_VALUE);
    }

    public ManagedExecutorService(String name, ExecutorService cachedExecutor, int maxPoolSize, int queueCapacity) {
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

    public void execute(Runnable command) {
        if (!taskQ.offer(command)) {
            throw new RejectedExecutionException("Executor[" + name + "] is overloaded!");
        }
        addNewWorkerIfRequired();
    }

    public <T> Future<T> submit(Callable<T> task) {
        final RunnableFuture<T> rf = new FutureTask<T>(task);
        execute(rf);
        return rf;
    }

    public <T> Future<T> submit(Runnable task, T result) {
        final RunnableFuture<T> rf = new FutureTask<T>(task, result);
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
                        if (size < maxPoolSize && queueSize() > 0) {
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

    public int poolSize() {
        return size;
    }

    public int queueSize() {
        return taskQ.size();  // LBQ size handled by an atomic int
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

    private class Worker implements Runnable {

        public void run() {
            try {
                Runnable r;
                do {
                    r = taskQ.poll(1, TimeUnit.MILLISECONDS);
                    if (r != null) {
                        r.run();
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
