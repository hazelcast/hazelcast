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

package com.hazelcast.spi.impl.executionservice.impl;

import com.hazelcast.config.DurableExecutorConfig;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.config.ScheduledExecutorConfig;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.metrics.DynamicMetricsProvider;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.internal.util.ConcurrencyUtil;
import com.hazelcast.internal.util.ConstructorFunction;
import com.hazelcast.internal.util.RuntimeAvailableProcessors;
import com.hazelcast.internal.util.executor.CachedExecutorServiceDelegate;
import com.hazelcast.internal.util.executor.ExecutorType;
import com.hazelcast.internal.util.executor.LoggingScheduledExecutor;
import com.hazelcast.internal.util.executor.ManagedExecutorService;
import com.hazelcast.internal.util.executor.NamedThreadPoolExecutor;
import com.hazelcast.internal.util.executor.PoolExecutorThreadFactory;
import com.hazelcast.internal.util.executor.SingleExecutorThreadFactory;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.impl.executionservice.TaskScheduler;

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.EXECUTOR_DISCRIMINATOR_NAME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.EXECUTOR_PREFIX_DURABLE_INTERNAL;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.EXECUTOR_PREFIX_INTERNAL;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.EXECUTOR_PREFIX_SCHEDULED_INTERNAL;
import static com.hazelcast.internal.metrics.MetricTarget.MANAGEMENT_CENTER;
import static com.hazelcast.internal.util.ThreadUtil.createThreadPoolName;
import static java.lang.Thread.currentThread;

@SuppressWarnings({"checkstyle:classfanoutcomplexity", "checkstyle:methodcount"})
public final class ExecutionServiceImpl implements ExecutionService {

    private static final int CORE_POOL_SIZE = 3;
    private static final long KEEP_ALIVE_TIME = 60L;
    private static final long INITIAL_DELAY = 1000;
    private static final long PERIOD = 100;
    private static final int BEGIN_INDEX = 3;
    private static final long AWAIT_TIME = 3;
    private static final int POOL_MULTIPLIER = 2;
    private static final int QUEUE_MULTIPLIER = 100000;
    private static final int ASYNC_QUEUE_CAPACITY = 100000;
    private static final int OFFLOADABLE_QUEUE_CAPACITY = 100000;

    private final ILogger logger;
    private final NodeEngineImpl nodeEngine;
    private final TaskScheduler globalTaskScheduler;
    private final ExecutorService cachedExecutorService;
    private final LoggingScheduledExecutor scheduledExecutorService;
    private final CompletableFutureTask completableFutureTask;
    private final ConcurrentMap<String, ManagedExecutorService> executors = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ManagedExecutorService> durableExecutors = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ManagedExecutorService> scheduleDurableExecutors = new ConcurrentHashMap<>();
    private final ConstructorFunction<String, ManagedExecutorService> constructor =
            new ConstructorFunction<String, ManagedExecutorService>() {
                @Override
                public ManagedExecutorService createNew(String name) {
                    ExecutorConfig config = nodeEngine.getConfig().findExecutorConfig(name);
                    int queueCapacity = config.getQueueCapacity() <= 0 ? Integer.MAX_VALUE : config.getQueueCapacity();
                    return createExecutor(name, config.getPoolSize(), queueCapacity, ExecutorType.CACHED, null);
                }
            };
    private final ConstructorFunction<String, ManagedExecutorService> durableConstructor =
            new ConstructorFunction<String, ManagedExecutorService>() {
                @Override
                public ManagedExecutorService createNew(String name) {
                    DurableExecutorConfig cfg = nodeEngine.getConfig().findDurableExecutorConfig(name);
                    return createExecutor(name, cfg.getPoolSize(), Integer.MAX_VALUE, ExecutorType.CACHED, null);
                }
            };
    private final ConstructorFunction<String, ManagedExecutorService> scheduledDurableConstructor =
            new ConstructorFunction<String, ManagedExecutorService>() {
                @Override
                public ManagedExecutorService createNew(String name) {
                    ScheduledExecutorConfig cfg = nodeEngine.getConfig().findScheduledExecutorConfig(name);
                    return createExecutor(name, cfg.getPoolSize(), Integer.MAX_VALUE, ExecutorType.CACHED, null);
                }
            };

    public ExecutionServiceImpl(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;

        Node node = nodeEngine.getNode();
        this.logger = node.getLogger(ExecutionService.class.getName());

        String hzName = nodeEngine.getHazelcastInstance().getName();
        ClassLoader configClassLoader = node.getConfigClassLoader();
        ThreadFactory threadFactory = new PoolExecutorThreadFactory(createThreadPoolName(hzName, "cached"),
                configClassLoader);
        this.cachedExecutorService = new ThreadPoolExecutor(
                CORE_POOL_SIZE, Integer.MAX_VALUE, KEEP_ALIVE_TIME, TimeUnit.SECONDS, new SynchronousQueue<>(),
                threadFactory, (r, executor) -> {
            if (logger.isFinestEnabled()) {
                logger.finest("Node is shutting down; discarding the task: " + r);
            }
        });

        ThreadFactory singleExecutorThreadFactory = new SingleExecutorThreadFactory(configClassLoader,
                createThreadPoolName(hzName, "scheduled"));
        this.scheduledExecutorService = new LoggingScheduledExecutor(logger, 1, singleExecutorThreadFactory);

        int coreSize = Math.max(RuntimeAvailableProcessors.get(), 2);
        // default executors
        register(SYSTEM_EXECUTOR, coreSize, Integer.MAX_VALUE, ExecutorType.CACHED);
        register(SCHEDULED_EXECUTOR, coreSize * POOL_MULTIPLIER, coreSize * QUEUE_MULTIPLIER, ExecutorType.CACHED);
        register(ASYNC_EXECUTOR, coreSize, ASYNC_QUEUE_CAPACITY, ExecutorType.CONCRETE);
        register(OFFLOADABLE_EXECUTOR, coreSize, OFFLOADABLE_QUEUE_CAPACITY, ExecutorType.CACHED);
        this.globalTaskScheduler = getTaskScheduler(SCHEDULED_EXECUTOR);

        // register CompletableFuture task
        this.completableFutureTask = new CompletableFutureTask();
        scheduleWithRepetition(completableFutureTask, INITIAL_DELAY, PERIOD, TimeUnit.MILLISECONDS);

        // register in metricsRegistry
        nodeEngine.getMetricsRegistry().registerDynamicMetricsProvider(new MetricsProvider(executors, durableExecutors,
                scheduleDurableExecutors));
    }

    // only used in tests
    public LoggingScheduledExecutor getScheduledExecutorService() {
        return scheduledExecutorService;
    }

    @Override
    public ManagedExecutorService register(String name, int defaultPoolSize, int defaultQueueCapacity, ExecutorType type) {
        return register(name, defaultPoolSize, defaultQueueCapacity, type, null);
    }

    @Override
    public ManagedExecutorService register(String name, int defaultPoolSize,
                                           int defaultQueueCapacity, ThreadFactory threadFactory) {
        return register(name, defaultPoolSize, defaultQueueCapacity, ExecutorType.CONCRETE, threadFactory);
    }

    private ManagedExecutorService register(String name, int defaultPoolSize,
                                            int defaultQueueCapacity, ExecutorType type, ThreadFactory threadFactory) {

        ExecutorConfig config = nodeEngine.getConfig().getExecutorConfigs().get(name);

        int poolSize = defaultPoolSize;
        int queueCapacity = defaultQueueCapacity;
        if (config != null) {
            poolSize = config.getPoolSize();
            if (config.getQueueCapacity() <= 0) {
                queueCapacity = Integer.MAX_VALUE;
            } else {
                queueCapacity = config.getQueueCapacity();
            }
        }

        ManagedExecutorService executor = createExecutor(name, poolSize, queueCapacity, type, threadFactory);
        if (executors.putIfAbsent(name, executor) != null) {
            throw new IllegalArgumentException("ExecutorService['" + name + "'] already exists!");
        }

        return executor;
    }

    private ManagedExecutorService createExecutor(String name, int poolSize, int queueCapacity,
                                                  ExecutorType type, ThreadFactory threadFactory) {
        ManagedExecutorService executor;
        if (type == ExecutorType.CACHED) {
            if (threadFactory != null) {
                throw new IllegalArgumentException("Cached executor can not be used with external thread factory");
            }
            executor = new CachedExecutorServiceDelegate(name, cachedExecutorService, poolSize, queueCapacity);
        } else if (type == ExecutorType.CONCRETE) {
            if (threadFactory == null) {
                ClassLoader classLoader = nodeEngine.getConfigClassLoader();
                String hzName = nodeEngine.getHazelcastInstance().getName();
                String internalName = name.startsWith("hz:") ? name.substring(BEGIN_INDEX) : name;
                String threadNamePrefix = createThreadPoolName(hzName, internalName);
                threadFactory = new PoolExecutorThreadFactory(threadNamePrefix, classLoader);
            }

            NamedThreadPoolExecutor pool = new NamedThreadPoolExecutor(name, poolSize, poolSize,
                    KEEP_ALIVE_TIME, TimeUnit.SECONDS, new LinkedBlockingQueue<>(queueCapacity), threadFactory);
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
    public ManagedExecutorService getDurable(String name) {
        return ConcurrencyUtil.getOrPutIfAbsent(durableExecutors, name, durableConstructor);
    }

    @Override
    public ExecutorService getScheduledDurable(String name) {
        return ConcurrencyUtil.getOrPutIfAbsent(scheduleDurableExecutors, name, scheduledDurableConstructor);
    }

    @Override
    public <V> InternalCompletableFuture<V> asCompletableFuture(Future<V> future) {
        if (future == null) {
            throw new IllegalArgumentException("future must not be null");
        }
        if (future instanceof InternalCompletableFuture) {
            return (InternalCompletableFuture<V>) future;
        }
        return registerCompletableFuture(future);
    }

    @Override
    public void execute(String name, Runnable command) {
        getExecutor(name).execute(command);
    }

    @Override
    public void executeDurable(String name, Runnable command) {
        getDurable(name).execute(command);
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
        return globalTaskScheduler.schedule(command, delay, unit);
    }

    @Override
    public ScheduledFuture<?> schedule(String name, Runnable command, long delay, TimeUnit unit) {
        return getTaskScheduler(name).schedule(command, delay, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleDurable(String name, Runnable command, long delay, TimeUnit unit) {
        return getDurableTaskScheduler(name).schedule(command, delay, unit);
    }

    @Override
    public <V> ScheduledFuture<Future<V>> scheduleDurable(String name, Callable<V> command, long delay, TimeUnit unit) {
        return getDurableTaskScheduler(name).schedule(command, delay, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleWithRepetition(Runnable command, long initialDelay, long period, TimeUnit unit) {
        return globalTaskScheduler.scheduleWithRepetition(command, initialDelay, period, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleWithRepetition(String name, Runnable command, long initialDelay,
                                                     long period, TimeUnit unit) {
        return getTaskScheduler(name).scheduleWithRepetition(command, initialDelay, period, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleDurableWithRepetition(String name, Runnable command, long initialDelay,
                                                            long period, TimeUnit unit) {
        return getDurableTaskScheduler(name).scheduleWithRepetition(command, initialDelay, period, unit);
    }

    @Override
    public TaskScheduler getGlobalTaskScheduler() {
        return globalTaskScheduler;
    }

    @Override
    public TaskScheduler getTaskScheduler(String name) {
        return new DelegatingTaskScheduler(scheduledExecutorService, getExecutor(name));
    }

    public void shutdown() {
        logger.finest("Stopping executors...");
        scheduledExecutorService.notifyShutdownInitiated();
        for (ExecutorService executorService : executors.values()) {
            executorService.shutdown();
        }
        for (ExecutorService executorService : durableExecutors.values()) {
            executorService.shutdown();
        }
        for (ExecutorService executorService : scheduleDurableExecutors.values()) {
            executorService.shutdown();
        }
        scheduledExecutorService.shutdownNow();
        cachedExecutorService.shutdown();
        try {
            scheduledExecutorService.awaitTermination(AWAIT_TIME, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            currentThread().interrupt();
            logger.finest(e);
        }
        try {
            if (!cachedExecutorService.awaitTermination(AWAIT_TIME, TimeUnit.SECONDS)) {
                cachedExecutorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            currentThread().interrupt();
            logger.finest(e);
        }
        executors.clear();
        durableExecutors.clear();
        scheduleDurableExecutors.clear();
    }

    @Override
    public void shutdownExecutor(String name) {
        ExecutorService executorService = executors.remove(name);
        if (executorService != null) {
            executorService.shutdown();
        }
    }

    @Override
    public void shutdownDurableExecutor(String name) {
        ExecutorService executorService = durableExecutors.remove(name);
        if (executorService != null) {
            executorService.shutdown();
        }
    }

    @Override
    public void shutdownScheduledDurableExecutor(String name) {
        ExecutorService executorService = scheduleDurableExecutors.remove(name);
        if (executorService != null) {
            executorService.shutdown();
        }
    }

    private <V> InternalCompletableFuture<V> registerCompletableFuture(Future<V> future) {
        CompletableFutureEntry<V> entry = new CompletableFutureEntry<>(future);
        completableFutureTask.registerCompletableFutureEntry(entry);
        return entry.completableFuture;
    }

    private TaskScheduler getDurableTaskScheduler(String name) {
        return new DelegatingTaskScheduler(scheduledExecutorService, getScheduledDurable(name));
    }

    private static final class MetricsProvider implements DynamicMetricsProvider {

        private final ConcurrentMap<String, ManagedExecutorService> executors;
        private final ConcurrentMap<String, ManagedExecutorService> durableExecutors;
        private final ConcurrentMap<String, ManagedExecutorService> scheduleDurableExecutors;

        private MetricsProvider(ConcurrentMap<String, ManagedExecutorService> executors,
                                ConcurrentMap<String, ManagedExecutorService> durableExecutors,
                                ConcurrentMap<String, ManagedExecutorService> scheduleDurableExecutors) {

            this.executors = executors;
            this.durableExecutors = durableExecutors;
            this.scheduleDurableExecutors = scheduleDurableExecutors;
        }

        @Override
        public void provideDynamicMetrics(MetricDescriptor descriptor,
                                          MetricsCollectionContext context) {
            for (ManagedExecutorService executorService : executors.values()) {
                MetricDescriptor executorDescriptor = descriptor
                        .copy()
                        .withPrefix(EXECUTOR_PREFIX_INTERNAL)
                        .withDiscriminator(EXECUTOR_DISCRIMINATOR_NAME, executorService.getName())
                        .withExcludedTarget(MANAGEMENT_CENTER);
                context.collect(executorDescriptor, executorService);
            }

            for (ManagedExecutorService executorService : durableExecutors.values()) {
                MetricDescriptor executorDescriptor = descriptor
                        .copy()
                        .withPrefix(EXECUTOR_PREFIX_DURABLE_INTERNAL)
                        .withDiscriminator(EXECUTOR_DISCRIMINATOR_NAME, executorService.getName())
                        .withExcludedTarget(MANAGEMENT_CENTER);
                context.collect(executorDescriptor, executorService);
            }

            for (ManagedExecutorService executorService : scheduleDurableExecutors.values()) {
                MetricDescriptor executorDescriptor = descriptor
                        .copy()
                        .withPrefix(EXECUTOR_PREFIX_SCHEDULED_INTERNAL)
                        .withDiscriminator(EXECUTOR_DISCRIMINATOR_NAME, executorService.getName())
                        .withExcludedTarget(MANAGEMENT_CENTER);
                context.collect(executorDescriptor, executorService);
            }
        }
    }
}
