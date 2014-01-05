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

package com.hazelcast.spi.impl;

import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.core.CompletableFuture;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.annotation.PrivateApi;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.executor.*;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;

import static com.hazelcast.util.ValidationUtil.isNotNull;

/**
 * @author mdogan 12/14/12
 */
public final class ExecutionServiceImpl implements ExecutionService {

    private final NodeEngineImpl nodeEngine;
    private final ExecutorService cachedExecutorService;
    private final ScheduledExecutorService scheduledExecutorService;
    private final ExecutorService scheduledManagedExecutor;
    private final ILogger logger;
    private final CompletableFutureTask completableFutureTask;

    private final ConcurrentMap<String, ManagedExecutorService> executors = new ConcurrentHashMap<String, ManagedExecutorService>();

    public ExecutionServiceImpl(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        final Node node = nodeEngine.getNode();
        logger = node.getLogger(ExecutionService.class.getName());
        final ClassLoader classLoader = node.getConfigClassLoader();
        final ThreadFactory threadFactory = new PoolExecutorThreadFactory(node.threadGroup,
                node.getThreadPoolNamePrefix("cached"), classLoader);

        cachedExecutorService = new ThreadPoolExecutor(
                3, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>(), threadFactory, new RejectedExecutionHandler() {
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                if (logger.isFinestEnabled()) {
                    logger.finest( "Node is shutting down; discarding the task: " + r);
                }
            }
        });

        final String scheduledThreadName = node.getThreadNamePrefix("scheduled");
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
                new SingleExecutorThreadFactory(node.threadGroup, classLoader, scheduledThreadName));
        enableRemoveOnCancelIfAvailable();

        final int coreSize = Runtime.getRuntime().availableProcessors();
        // default executors
        register(SYSTEM_EXECUTOR, coreSize, Integer.MAX_VALUE);
        scheduledManagedExecutor = register(SCHEDULED_EXECUTOR, coreSize * 5, coreSize * 100000);

        // Register CompletableFuture task
        completableFutureTask = new CompletableFutureTask();
        scheduleWithFixedDelay(completableFutureTask, 100, 100, TimeUnit.MILLISECONDS);
    }

    public Set<String> getExecutorNames(){
        return new HashSet<String>(executors.keySet());
    }

    private void enableRemoveOnCancelIfAvailable() {
        try {
            final Method m = scheduledExecutorService.getClass().getMethod("setRemoveOnCancelPolicy", boolean.class);
            m.invoke(scheduledExecutorService, true);
        } catch (NoSuchMethodException ignored) {
        } catch (InvocationTargetException ignored) {
        } catch (IllegalAccessException ignored) {
        }
    }

    public ExecutorService register(String name, int poolSize, int queueCapacity) {
        ExecutorConfig cfg = nodeEngine.getConfig().getExecutorConfigs().get(name);
        if (cfg != null) {
            poolSize = cfg.getPoolSize();
            queueCapacity = cfg.getQueueCapacity() <= 0 ? Integer.MAX_VALUE : cfg.getQueueCapacity();
            if (logger.isLoggable(Level.INFO)) {
                logger.info("Overriding ExecutorService['" + name + "'] pool-size and queue-capacity using " + cfg);
            }
        }
        final ManagedExecutorService executor = new ManagedExecutorService(nodeEngine, name, cachedExecutorService,
                poolSize, queueCapacity);
        if (executors.putIfAbsent(name, executor) != null) {
            throw new IllegalArgumentException("ExecutorService['" + name + "'] already exists!");
        }
        return executor;
    }

    private final ConstructorFunction<String, ManagedExecutorService> constructor =
            new ConstructorFunction<String, ManagedExecutorService>() {
                public ManagedExecutorService createNew(String name) {
                    final ExecutorConfig cfg = nodeEngine.getConfig().findExecutorConfig(name);
                    final int queueCapacity = cfg.getQueueCapacity() <= 0 ? Integer.MAX_VALUE : cfg.getQueueCapacity();
                    return new ManagedExecutorService(nodeEngine, name, cachedExecutorService, cfg.getPoolSize(), queueCapacity);
                }
            };

    public ManagedExecutorService getExecutor(String name) {
        return ConcurrencyUtil.getOrPutIfAbsent(executors, name, constructor);
    }

    @Override
    public <V> CompletableFuture<V> asCompletableFuture(Future<V> future) {
        if (future == null) {
            throw new IllegalArgumentException("future must not be null");
        }
        if (future instanceof CompletableFuture) {
            return (CompletableFuture<V>) future;
        }
        return registerCompletableFuture(future);
    }

    public void execute(String name, Runnable command) {
        getExecutor(name).execute(command);
    }

    public Future<?> submit(String name, Runnable task) {
        return getExecutor(name).submit(task);
    }

    public <T> Future<T> submit(String name, Callable<T> task) {
        return getExecutor(name).submit(task);
    }

    public ScheduledFuture<?> schedule(final Runnable command, long delay, TimeUnit unit) {
        return scheduledExecutorService.schedule(createScheduledRunner(command, scheduledManagedExecutor), delay, unit);
    }

    public ScheduledFuture<?> scheduleAtFixedRate(final Runnable command, long initialDelay, long period, TimeUnit unit) {
        return scheduledExecutorService.scheduleAtFixedRate(createScheduledRunner(command, scheduledManagedExecutor), initialDelay, period, unit);
    }

    public ScheduledFuture<?> scheduleWithFixedDelay(final Runnable command, long initialDelay, long period, TimeUnit unit) {
        return scheduledExecutorService.scheduleWithFixedDelay(createScheduledRunner(command, scheduledManagedExecutor), initialDelay, period, unit);
    }

    private static ScheduledTaskRunner createScheduledRunner(Runnable command, Executor executor) {
        if (command instanceof ScheduledTaskRunner) {
            return (ScheduledTaskRunner) command;
        }
        return new ScheduledTaskRunner(executor, command);
    }

    @PrivateApi
    public Executor getCachedExecutor() {
        return new ExecutorDelegate(cachedExecutorService);
    }

    public ScheduledExecutorService getScheduledExecutor() {
        return new ScheduledExecutorServiceDelegate(scheduledExecutorService, scheduledManagedExecutor);
    }

    @PrivateApi
    void shutdown() {
        logger.finest( "Stopping executors...");
        cachedExecutorService.shutdown();
        scheduledExecutorService.shutdownNow();
        try {
            cachedExecutorService.awaitTermination(3, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.finest(e);
        }
        for (ExecutorService executorService : executors.values()) {
            executorService.shutdown();
        }
        executors.clear();
    }

    public void shutdownExecutor(String name) {
        final ExecutorService ex = executors.remove(name);
        if (ex != null) {
            ex.shutdown();
        }
    }

    private <V> CompletableFuture<V> registerCompletableFuture(Future<V> future) {
        CompletableFutureEntry<V> entry = new CompletableFutureEntry<V>(future, nodeEngine, completableFutureTask);
        completableFutureTask.registerCompletableFutureEntry(entry);
        return entry.completableFuture;
    }

    private static class CompletableFutureTask implements Runnable {
        private final List<CompletableFutureEntry> entries = new ArrayList<CompletableFutureEntry>();
        private final Lock entriesLock = new ReentrantLock();

        private <V> void registerCompletableFutureEntry(CompletableFutureEntry<V> entry) {
            entriesLock.lock();
            try {
                entries.add(entry);
            } finally {
                entriesLock.unlock();
            }
        }

        private <V> boolean cancelCompletableFutureEntry(CompletableFutureEntry<V> entry) {
            entriesLock.lock();
            try {
                return entries.remove(entry);
            } finally {
                entriesLock.unlock();
            }
        }

        @Override
        public void run() {
            if (entries.size() > 0) {
                CompletableFutureEntry[] copy;
                entriesLock.lock();
                try {
                    copy = new CompletableFutureEntry[entries.size()];
                    copy = this.entries.toArray(copy);
                } finally {
                    entriesLock.unlock();
                }
                List<CompletableFutureEntry> removes = null;
                for (CompletableFutureEntry entry : copy) {
                    if (entry.processState()) {
                        if (removes == null) {
                            removes = new ArrayList<CompletableFutureEntry>(copy.length / 2);
                        }
                        removes.add(entry);
                    }
                }
                // Remove processed elements
                if (removes != null && !removes.isEmpty()) {
                    entriesLock.lock();
                    try {
                        for (int i = 0; i < removes.size(); i++) {
                            entries.remove(removes.get(i));
                        }
                    } finally {
                        entriesLock.unlock();
                    }
                }
            }
        }
    }

    static class CompletableFutureEntry<V> {
        private final BasicCompletableFuture<V> completableFuture;
        private final CompletableFutureTask completableFutureTask;

        private CompletableFutureEntry(Future<V> future, NodeEngine nodeEngine,
                                       CompletableFutureTask completableFutureTask) {
            this.completableFutureTask = completableFutureTask;
            this.completableFuture  = new BasicCompletableFuture<V>(future, nodeEngine, this);
        }

        private boolean processState() {
            if (completableFuture.isDone()) {
                completableFuture.fireCallbacks();
                return true;
            }
            return false;
        }

        public void cancel() {
            completableFutureTask.cancelCompletableFutureEntry(this);
        }
    }

    private static class ExecutorDelegate implements Executor {
        private final Executor executor;

        private ExecutorDelegate(Executor executor) {
            this.executor = executor;
        }

        public void execute(Runnable command) {
            executor.execute(command);
        }
    }

    private static class ScheduledExecutorServiceDelegate implements ScheduledExecutorService {

        private final ScheduledExecutorService scheduledExecutorService;
        private final ExecutorService executor;

        private ScheduledExecutorServiceDelegate(ScheduledExecutorService scheduledExecutorService, ExecutorService executor) {
            this.scheduledExecutorService = scheduledExecutorService;
            this.executor = executor;
        }

        public void execute(Runnable command) {
            executor.execute(command);
        }

        public <T> Future<T> submit(Callable<T> task) {
            return executor.submit(task);
        }

        public <T> Future<T> submit(Runnable task, T result) {
            return executor.submit(task, result);
        }

        public Future<?> submit(Runnable task) {
            return executor.submit(task);
        }

        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
            return scheduledExecutorService.schedule(createScheduledRunner(command, executor), delay, unit);
        }

        public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
            return scheduledExecutorService.scheduleAtFixedRate(createScheduledRunner(command, executor), initialDelay, period, unit);
        }

        public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
            return scheduledExecutorService.scheduleWithFixedDelay(createScheduledRunner(command, executor), initialDelay, delay, unit);
        }

        public void shutdown() {
            throw new UnsupportedOperationException();
        }

        public List<Runnable> shutdownNow() {
            throw new UnsupportedOperationException();
        }

        public boolean isShutdown() {
            return false;
        }

        public boolean isTerminated() {
            return false;
        }

        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            throw new UnsupportedOperationException();
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

        public <V> ScheduledFuture<V> schedule(final Callable<V> callable, long delay, TimeUnit unit) {
            throw new UnsupportedOperationException();
        }
    }


    static class BasicCompletableFuture<V> implements CompletableFuture<V> {

        private static final Object NULL_VALUE = new Object();

        private final AtomicReferenceFieldUpdater<BasicCompletableFuture, ExecutionCallbackNode> callbackUpdater;

        private final ILogger logger;
        private final Future<V> future;
        private final NodeEngine nodeEngine;
        private final CompletableFutureEntry<V> completableFutureEntry;
        private volatile ExecutionCallbackNode<V> callbackHead;
        private volatile Object result = NULL_VALUE;

        BasicCompletableFuture(Future<V> future, NodeEngine nodeEngine,
                                      CompletableFutureEntry<V> completableFutureEntry) {
            this.future = future;
            this.nodeEngine = nodeEngine;
            this.completableFutureEntry = completableFutureEntry;
            this.logger = nodeEngine.getLogger(BasicCompletableFuture.class);
            this.callbackUpdater = AtomicReferenceFieldUpdater.newUpdater(
                    BasicCompletableFuture.class, ExecutionCallbackNode.class, "callbackHead");
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
            for (;;) {
                ExecutionCallbackNode oldCallbackHead = callbackHead;
                ExecutionCallbackNode newCallbackHead = new ExecutionCallbackNode<V>(callback, executor, oldCallbackHead);
                if (callbackUpdater.compareAndSet(this, oldCallbackHead, newCallbackHead)) {
                    break;
                }
            }
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return future.cancel(mayInterruptIfRunning);
        }

        @Override
        public boolean isCancelled() {
            return future.isCancelled();
        }

        @Override
        public boolean isDone() {
            return result != NULL_VALUE || future.isDone();
        }

        @Override
        public V get() throws InterruptedException, ExecutionException {
            try {
                return get(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                logger.severe("Unexpected timeout while processing " + this, e);
                return null;
            }
        }

        @Override
        public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            // Cancel the async waiting since we now have a waiting thread to use
            completableFutureEntry.cancel();

            // Waiting for the result and fire callbacks afterwards
            future.get(timeout, unit);
            fireCallbacks();
            return (V) result;
        }

        public void fireCallbacks() {
            ExecutionCallbackNode<V> callbackChain;
            for (;;) {
                callbackChain = callbackHead;
                if (callbackUpdater.compareAndSet(this, callbackChain, null)) {
                    break;
                }
            }
            while (callbackChain != null) {
                runAsynchronous(callbackChain.callback, callbackChain.executor);
                callbackChain = callbackChain.next;
            }
        }

        private Object readResult() {
            if (result == NULL_VALUE) {
                try {
                    result = future.get();
                } catch (Throwable t) {
                    result = t;
                }
            }
            return result;
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
                        logger.severe("Failed to async for " + BasicCompletableFuture.this, t);
                    }
                }
            });
        }

        private ExecutorService getAsyncExecutor() {
            return nodeEngine.getExecutionService().getExecutor(ASYNC_EXECUTOR);
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
}
