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
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.annotation.PrivateApi;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.executor.CachedExecutorServiceDelegate;
import com.hazelcast.util.executor.ExecutorType;
import com.hazelcast.util.executor.ManagedExecutorService;
import com.hazelcast.util.executor.NamedThreadPoolExecutor;
import com.hazelcast.util.executor.PoolExecutorThreadFactory;
import com.hazelcast.util.executor.SingleExecutorThreadFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.hazelcast.util.EmptyStatement.ignore;

public final class ExecutionServiceImpl implements ExecutionService {

    private static final int CORE_POOL_SIZE = 3;
    private static final long KEEP_ALIVE_TIME = 60L;
    private static final long INITIAL_DELAY = 1000;
    private static final long PERIOD = 100;
    private static final int BEGIN_INDEX = 3;
    private static final long AWAIT_TIME = 3;
    private static final int POOL_MULTIPLIER = 2;
    private static final int QUEUE_MULTIPLIER = 100000;

    private final NodeEngineImpl nodeEngine;
    private final ExecutorService cachedExecutorService;
    private final ScheduledExecutorService scheduledExecutorService;
    private final ScheduledExecutorService defaultScheduledExecutorServiceDelegate;
    private final ILogger logger;
    private final CompletableFutureTask completableFutureTask;

    private final ConcurrentMap<String, ManagedExecutorService> executors
            = new ConcurrentHashMap<String, ManagedExecutorService>();

    private final ConstructorFunction<String, ManagedExecutorService> constructor =
            new ConstructorFunction<String, ManagedExecutorService>() {
                @Override
                public ManagedExecutorService createNew(String name) {
                    final ExecutorConfig cfg = nodeEngine.getConfig().findExecutorConfig(name);
                    final int queueCapacity = cfg.getQueueCapacity() <= 0 ? Integer.MAX_VALUE : cfg.getQueueCapacity();
                    return createExecutor(name, cfg.getPoolSize(), queueCapacity, ExecutorType.CACHED);
                }
            };

    public ExecutionServiceImpl(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        final Node node = nodeEngine.getNode();
        logger = node.getLogger(ExecutionService.class.getName());
        final ClassLoader classLoader = node.getConfigClassLoader();
        final ThreadFactory threadFactory = new PoolExecutorThreadFactory(node.threadGroup,
                node.getThreadPoolNamePrefix("cached"), classLoader);

        cachedExecutorService = new ThreadPoolExecutor(
                CORE_POOL_SIZE, Integer.MAX_VALUE, KEEP_ALIVE_TIME, TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>(), threadFactory, new RejectedExecutionHandler() {
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                if (logger.isFinestEnabled()) {
                    logger.finest("Node is shutting down; discarding the task: " + r);
                }
            }
        }
        );

        final String scheduledThreadName = node.getThreadNamePrefix("scheduled");
        scheduledExecutorService = new ScheduledThreadPoolExecutor(1,
                new SingleExecutorThreadFactory(node.threadGroup, classLoader, scheduledThreadName));
        enableRemoveOnCancelIfAvailable();

        final int coreSize = Runtime.getRuntime().availableProcessors();
        // default executors
        register(SYSTEM_EXECUTOR, coreSize, Integer.MAX_VALUE, ExecutorType.CACHED);
        register(SCHEDULED_EXECUTOR, coreSize * POOL_MULTIPLIER, coreSize * QUEUE_MULTIPLIER, ExecutorType.CACHED);
        defaultScheduledExecutorServiceDelegate = getScheduledExecutor(SCHEDULED_EXECUTOR);

        // Register CompletableFuture task
        completableFutureTask = new CompletableFutureTask();
        scheduleWithFixedDelay(completableFutureTask, INITIAL_DELAY, PERIOD, TimeUnit.MILLISECONDS);
    }

    private void enableRemoveOnCancelIfAvailable() {
        try {
            final Method m = scheduledExecutorService.getClass().getMethod("setRemoveOnCancelPolicy", boolean.class);
            m.invoke(scheduledExecutorService, true);
        } catch (NoSuchMethodException ignored) {
            ignore(ignored);
        } catch (InvocationTargetException ignored) {
            ignore(ignored);
        } catch (IllegalAccessException ignored) {
            ignore(ignored);
        }
    }

    @Override
    public ManagedExecutorService register(String name, int defaultPoolSize, int defaultQueueCapacity,
                                           ExecutorType type) {
        ExecutorConfig cfg = nodeEngine.getConfig().getExecutorConfigs().get(name);

        int poolSize = defaultPoolSize;
        int queueCapacity = defaultQueueCapacity;
        if (cfg != null) {
            poolSize = cfg.getPoolSize();
            if (cfg.getQueueCapacity() <= 0) {
                queueCapacity = Integer.MAX_VALUE;
            } else {
                queueCapacity = cfg.getQueueCapacity();
            }
        }

        ManagedExecutorService executor = createExecutor(name, poolSize, queueCapacity, type);
        if (executors.putIfAbsent(name, executor) != null) {
            throw new IllegalArgumentException("ExecutorService['" + name + "'] already exists!");
        }
        return executor;
    }

    private ManagedExecutorService createExecutor(String name, int poolSize, int queueCapacity, ExecutorType type) {
        ManagedExecutorService executor;
        if (type == ExecutorType.CACHED) {
            executor = new CachedExecutorServiceDelegate(nodeEngine, name, cachedExecutorService, poolSize, queueCapacity);
        } else if (type == ExecutorType.CONCRETE) {
            Node node = nodeEngine.getNode();
            String internalName = name.startsWith("hz:") ? name.substring(BEGIN_INDEX) : name;
            NamedThreadPoolExecutor pool = new NamedThreadPoolExecutor(name, poolSize, poolSize,
                    KEEP_ALIVE_TIME, TimeUnit.SECONDS,
                    new LinkedBlockingQueue<Runnable>(queueCapacity),
                    new PoolExecutorThreadFactory(node.threadGroup,
                            node.getThreadPoolNamePrefix(internalName), node.getConfigClassLoader())
            );
            pool.allowCoreThreadTimeOut(true);
            executor = pool;
        } else {
            throw new IllegalArgumentException("Unknown executor type: " + type);
        }
        return executor;
    }

    @Override
    public ManagedExecutorService getExecutor(String name) {
        return ConcurrencyUtil.getOrPutIfAbsent(executors, name, constructor);
    }

    @Override
    public <V> ICompletableFuture<V> asCompletableFuture(Future<V> future) {
        if (future == null) {
            throw new IllegalArgumentException("future must not be null");
        }
        if (future instanceof ICompletableFuture) {
            return (ICompletableFuture<V>) future;
        }
        return registerCompletableFuture(future);
    }

    @Override
    public void execute(String name, Runnable command) {
        getExecutor(name).execute(command);
    }

    @Override
    public Future<?> submit(String name, Runnable task) {
        return getExecutor(name).submit(task);
    }

    @Override
    public <T> Future<T> submit(String name, Callable<T> task) {
        return getExecutor(name).submit(task);
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        return defaultScheduledExecutorServiceDelegate.schedule(command, delay, unit);
    }

    @Override
    public ScheduledFuture<?> schedule(String name, Runnable command, long delay, TimeUnit unit) {
        return getScheduledExecutor(name).schedule(command, delay, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        return defaultScheduledExecutorServiceDelegate.scheduleAtFixedRate(command, initialDelay, period, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(String name, Runnable command, long initialDelay,
                                                  long period, TimeUnit unit) {
        return getScheduledExecutor(name).scheduleAtFixedRate(command, initialDelay, period, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long period, TimeUnit unit) {
        return defaultScheduledExecutorServiceDelegate.scheduleWithFixedDelay(command, initialDelay, period, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(String name, Runnable command, long initialDelay,
                                                     long period, TimeUnit unit) {
        return getScheduledExecutor(name).scheduleWithFixedDelay(command, initialDelay, period, unit);
    }

    @Override
    public ScheduledExecutorService getDefaultScheduledExecutor() {
        return defaultScheduledExecutorServiceDelegate;
    }

    @Override
    public ScheduledExecutorService getScheduledExecutor(String name) {
        return new ScheduledExecutorServiceDelegate(scheduledExecutorService, getExecutor(name));
    }

    @PrivateApi
    void shutdown() {
        logger.finest("Stopping executors...");
        for (ExecutorService executorService : executors.values()) {
            executorService.shutdown();
        }
        scheduledExecutorService.shutdownNow();
        cachedExecutorService.shutdown();
        try {
            scheduledExecutorService.awaitTermination(AWAIT_TIME, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.finest(e);
        }
        try {
            cachedExecutorService.awaitTermination(AWAIT_TIME, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.finest(e);
        }
        executors.clear();
    }

    @Override
    public void shutdownExecutor(String name) {
        final ExecutorService ex = executors.remove(name);
        if (ex != null) {
            ex.shutdown();
        }
    }

    private <V> ICompletableFuture<V> registerCompletableFuture(Future<V> future) {
        CompletableFutureEntry<V> entry = new CompletableFutureEntry<V>(future, nodeEngine);
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

        @Override
        public void run() {
            List<CompletableFutureEntry> removableEntries = removableEntries();
            removeEntries(removableEntries);
        }

        private void removeEntries(List<CompletableFutureEntry> removableEntries) {
            if (removableEntries.isEmpty()) {
                return;
            }

            entriesLock.lock();
            try {
                entries.removeAll(removableEntries);
            } finally {
                entriesLock.unlock();
            }
        }

        private List<CompletableFutureEntry> removableEntries() {
            CompletableFutureEntry[] entries = copyEntries();

            List<CompletableFutureEntry> removableEntries = Collections.EMPTY_LIST;
            for (CompletableFutureEntry entry : entries) {
                if (entry.processState()) {
                    if (removableEntries.isEmpty()) {
                        removableEntries = new ArrayList<CompletableFutureEntry>(entries.length / 2);
                    }

                    removableEntries.add(entry);
                }
            }
            return removableEntries;
        }

        private CompletableFutureEntry[] copyEntries() {
            if (entries.isEmpty()) {
                return new CompletableFutureEntry[]{};
            }

            CompletableFutureEntry[] copy;
            entriesLock.lock();
            try {
                copy = new CompletableFutureEntry[entries.size()];
                copy = entries.toArray(copy);
            } finally {
                entriesLock.unlock();
            }
            return copy;
        }
    }

    static final class CompletableFutureEntry<V> {
        private final BasicCompletableFuture<V> completableFuture;

        private CompletableFutureEntry(Future<V> future, NodeEngine nodeEngine) {
            this.completableFuture = new BasicCompletableFuture<V>(future, nodeEngine);
        }

        private boolean processState() {
            if (completableFuture.isDone()) {
                Object result;
                try {
                    result = completableFuture.future.get();
                } catch (Throwable t) {
                    result = t;
                }
                completableFuture.setResult(result);
                return true;
            }
            return false;
        }
    }

    private static final class ScheduledExecutorServiceDelegate implements ScheduledExecutorService {

        private final ScheduledExecutorService scheduledExecutorService;
        private final ExecutorService executor;

        private ScheduledExecutorServiceDelegate(ScheduledExecutorService scheduledExecutorService, ExecutorService executor) {
            this.scheduledExecutorService = scheduledExecutorService;
            this.executor = executor;
        }

        @Override
        public void execute(Runnable command) {
            executor.execute(command);
        }

        @Override
        public <T> Future<T> submit(Callable<T> task) {
            return executor.submit(task);
        }

        @Override
        public <T> Future<T> submit(Runnable task, T result) {
            return executor.submit(task, result);
        }

        @Override
        public Future<?> submit(Runnable task) {
            return executor.submit(task);
        }

        @Override
        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
            return scheduledExecutorService.schedule(
                    new ScheduledTaskRunner(command, executor), delay, unit);
        }

        @Override
        public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
            return scheduledExecutorService.scheduleAtFixedRate(
                    new ScheduledTaskRunner(command, executor), initialDelay, period, unit);
        }

        @Override
        public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
            return scheduledExecutorService.scheduleWithFixedDelay(
                    new ScheduledTaskRunner(command, executor), initialDelay, delay, unit);
        }

        @Override
        public void shutdown() {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<Runnable> shutdownNow() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isShutdown() {
            return false;
        }

        @Override
        public boolean isTerminated() {
            return false;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
                throws InterruptedException {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
                throws InterruptedException {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
                throws InterruptedException, ExecutionException {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
                throws InterruptedException, ExecutionException, TimeoutException {
            throw new UnsupportedOperationException();
        }

        @Override
        public <V> ScheduledFuture<V> schedule(final Callable<V> callable, long delay, TimeUnit unit) {
            throw new UnsupportedOperationException();
        }
    }

    static class BasicCompletableFuture<V> extends AbstractCompletableFuture<V> {

        private final Future<V> future;

        BasicCompletableFuture(Future<V> future, NodeEngine nodeEngine) {
            super(nodeEngine, nodeEngine.getLogger(BasicCompletableFuture.class));
            this.future = future;
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
            boolean done = future.isDone();
            if (done && !super.isDone()) {
                forceSetResult();
                return true;
            }
            return done || super.isDone();
        }

        @Override
        public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            V result = future.get(timeout, unit);
            // If not yet set by CompletableFuture task runner, we can go for it!
            if (!super.isDone()) {
                setResult(result);
            }
            return result;
        }

        private void forceSetResult() {
            Object result;
            try {
                result = future.get();
            } catch (Throwable t) {
                result = t;
            }
            setResult(result);
        }
    }

    private static class ScheduledTaskRunner implements Runnable {

        private final Executor executor;
        private final Runnable runnable;

        public ScheduledTaskRunner(Runnable runnable, Executor executor) {
            this.executor = executor;
            this.runnable = runnable;
        }

        @Override
        public void run() {
            try {
                executor.execute(runnable);
            } catch (Throwable t) {
                ExceptionUtil.sneakyThrow(t);
            }
        }
    }
}
