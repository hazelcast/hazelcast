/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl.executionservice.impl;

import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.instance.HazelcastThreadGroup;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.executionservice.InternalExecutionService;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.executor.CachedExecutorServiceDelegate;
import com.hazelcast.util.executor.ExecutorType;
import com.hazelcast.util.executor.ManagedExecutorService;
import com.hazelcast.util.executor.NamedThreadPoolExecutor;
import com.hazelcast.util.executor.PoolExecutorThreadFactory;
import com.hazelcast.util.executor.SingleExecutorThreadFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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

import static com.hazelcast.util.EmptyStatement.ignore;

public final class ExecutionServiceImpl implements InternalExecutionService {

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

    private final MetricsRegistry metricsRegistry;

    public ExecutionServiceImpl(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.metricsRegistry = nodeEngine.getMetricsRegistry();
        final Node node = nodeEngine.getNode();
        logger = node.getLogger(ExecutionService.class.getName());
        HazelcastThreadGroup threadGroup = node.getHazelcastThreadGroup();
        final ThreadFactory threadFactory = new PoolExecutorThreadFactory(threadGroup, "cached");

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

        scheduledExecutorService = new ScheduledThreadPoolExecutor(1, new SingleExecutorThreadFactory(threadGroup, "scheduled"));
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

        final ManagedExecutorService executor = createExecutor(name, poolSize, queueCapacity, type);
        if (executors.putIfAbsent(name, executor) != null) {
            throw new IllegalArgumentException("ExecutorService['" + name + "'] already exists!");
        }

        metricsRegistry.scanAndRegister(executor, "executor." + name);

        return executor;
    }

    private ManagedExecutorService createExecutor(String name, int poolSize, int queueCapacity, ExecutorType type) {
        ManagedExecutorService executor;
        if (type == ExecutorType.CACHED) {
            executor = new CachedExecutorServiceDelegate(nodeEngine, name, cachedExecutorService, poolSize, queueCapacity);
        } else if (type == ExecutorType.CONCRETE) {
            Node node = nodeEngine.getNode();
            String internalName = name.startsWith("hz:") ? name.substring(BEGIN_INDEX) : name;
            HazelcastThreadGroup hazelcastThreadGroup = node.getHazelcastThreadGroup();
            PoolExecutorThreadFactory threadFactory = new PoolExecutorThreadFactory(hazelcastThreadGroup,
                    hazelcastThreadGroup.getThreadPoolNamePrefix(internalName));
            NamedThreadPoolExecutor pool = new NamedThreadPoolExecutor(name, poolSize, poolSize,
                    KEEP_ALIVE_TIME, TimeUnit.SECONDS,
                    new LinkedBlockingQueue<Runnable>(queueCapacity),
                    threadFactory
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

    public void shutdown() {
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
}
