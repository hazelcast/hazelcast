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

import com.hazelcast.core.CompletableFuture;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.hazelcast.util.ValidationUtil.isNotNull;

/**
 * @author mdogan 2/18/13
 */
public final class ManagedExecutorService implements ExecutorService {

    private final AtomicLong executedCount = new AtomicLong();
    private final String name;
    private final int maxPoolSize;
    private final ExecutorService cachedExecutor;
    private final BlockingQueue<Runnable> taskQ;
    private final Lock lock = new ReentrantLock();
    private final NodeEngine nodeEngine;
    private final ILogger logger;
    private volatile int size;

    public ManagedExecutorService(NodeEngine nodeEngine, String name, ExecutorService cachedExecutor, int maxPoolSize) {
        this(nodeEngine, name, cachedExecutor, maxPoolSize, Integer.MAX_VALUE);
    }

    public ManagedExecutorService(NodeEngine nodeEngine, String name, ExecutorService cachedExecutor, int maxPoolSize, int queueCapacity) {
        if (maxPoolSize <= 0) {
            throw new IllegalArgumentException("Max pool size must be positive!");
        }
        if (queueCapacity <= 0) {
            throw new IllegalArgumentException("Queue capacity must be positive!");
        }
        this.name = name;
        this.nodeEngine = nodeEngine;
        this.maxPoolSize = maxPoolSize;
        this.cachedExecutor = cachedExecutor;
        this.logger = nodeEngine.getLogger(ManagedExecutorService.class);
        this.taskQ = new LinkedBlockingQueue<Runnable>(queueCapacity);
    }

    public String getName() {
        return name;
    }

    public long getExecutedCount() {
        return executedCount.get();
    }

    public int maxPoolSize() {
        return maxPoolSize;
    }

    public int poolSize() {
        return size;
    }

    public int queueSize() {
        return taskQ.size();  // LBQ size handled by an atomic int
    }

    public int queueRemainingCapacity() {
        return taskQ.remainingCapacity();
    }

    public void execute(Runnable command) {
        if (!taskQ.offer(command)) {
            throw new RejectedExecutionException("Executor[" + name + "] is overloaded!");
        }
        addNewWorkerIfRequired();
    }

    public <T> Future<T> submit(Callable<T> task) {
        final RunnableFuture<T> rf = new CompletableFutureTask<T>(task);
        execute(rf);
        return rf;
    }

    public <T> Future<T> submit(Runnable task, T result) {
        final RunnableFuture<T> rf = new CompletableFutureTask<T>(task, result);
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

    private class CompletableFutureTask<V> extends FutureTask<V> implements CompletableFuture<V> {

        private volatile ExecutionCallbackNode<V> callbackHead;

        public CompletableFutureTask(Callable<V> callable) {
            super(callable);
        }

        public CompletableFutureTask(Runnable runnable, V result) {
            super(runnable, result);
        }

        @Override
        public void run() {
            try {
                super.run();
            } finally {
                fireCallbacks();
            }
        }

        @Override
        public void andThen(ExecutionCallback<V> callback) {
            andThen(callback, getAsyncExecutor());
        }

        @Override
        public void andThen(ExecutionCallback<V> callback, Executor executor) {
            isNotNull(callback, "callback");
            isNotNull(executor, "executor");

            if (isDone()) {
                runAsynchronous(callback, executor);
                return;
            }

            synchronized (this) {
                this.callbackHead = new ExecutionCallbackNode<V>(callback, executor, callbackHead);
            }
        }

        private Object readResult() {
            try {
                return get();
            } catch (Throwable t) {
                return t;
            }
        }

        private void fireCallbacks() {
            ExecutionCallbackNode<V> callbackChain;
            synchronized (this) {
                callbackChain = callbackHead;
                callbackHead = null;
            }

            while (callbackChain != null) {
                runAsynchronous(callbackChain.callback, callbackChain.executor);
                callbackChain = callbackChain.next;
            }
        }

        private void runAsynchronous(final ExecutionCallback<V> callback, final Executor executor) {
            final Object result = readResult();
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        if (result instanceof Throwable) {
                            callback.onFailure((Throwable) result);
                        } else {
                            callback.onResponse((V) result);
                        }
                    } catch (Throwable t) {
                        //todo: improved error message
                        logger.severe("Failed to async for " + CompletableFutureTask.this, t);
                    }
                }
            });
        }

        private ExecutorService getAsyncExecutor() {
            return nodeEngine.getExecutionService().getExecutor(ExecutionService.ASYNC_EXECUTOR);
        }
    }

    private static class ExecutionCallbackNode<E> {
        private final ExecutionCallback<E> callback;
        private final Executor executor;
        private final ExecutionCallbackNode<E> next;

        private ExecutionCallbackNode(ExecutionCallback<E> callback, Executor executor, ExecutionCallbackNode<E> next) {
            this.callback = callback;
            this.executor = executor;
            this.next = next;
        }
    }

}
