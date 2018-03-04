/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.spi.impl;

import com.hazelcast.client.spi.ClientExecutionService;
import com.hazelcast.internal.metrics.MetricsProvider;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.util.RuntimeAvailableProcessors;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;
import com.hazelcast.util.executor.LoggingScheduledExecutor;
import com.hazelcast.util.executor.PoolExecutorThreadFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static com.hazelcast.spi.properties.GroupProperty.TASK_SCHEDULER_REMOVE_ON_CANCEL;
import static java.lang.Thread.currentThread;

public final class ClientExecutionServiceImpl implements ClientExecutionService, MetricsProvider {

    public static final HazelcastProperty INTERNAL_EXECUTOR_POOL_SIZE
            = new HazelcastProperty("hazelcast.client.internal.executor.pool.size", 3);

    public static final long TERMINATE_TIMEOUT_SECONDS = 30;

    private final ILogger logger;
    private final ExecutorService userExecutor;
    private final ScheduledExecutorService internalExecutor;

    public ClientExecutionServiceImpl(String name, ClassLoader classLoader,
                                      HazelcastProperties properties, int poolSize, LoggingService loggingService) {
        int internalPoolSize = properties.getInteger(INTERNAL_EXECUTOR_POOL_SIZE);
        if (internalPoolSize <= 0) {
            internalPoolSize = Integer.parseInt(INTERNAL_EXECUTOR_POOL_SIZE.getDefaultValue());
        }
        int executorPoolSize = poolSize;
        if (executorPoolSize <= 0) {
            executorPoolSize = RuntimeAvailableProcessors.get();
        }
        logger = loggingService.getLogger(ClientExecutionService.class);
        internalExecutor = new LoggingScheduledExecutor(logger, internalPoolSize,
                new PoolExecutorThreadFactory(name + ".internal-", classLoader),
                properties.getBoolean(TASK_SCHEDULER_REMOVE_ON_CANCEL),
                new RejectedExecutionHandler() {
                    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                        String message = "Internal executor rejected task: " + r + ", because client is shutting down...";
                        logger.finest(message);
                        throw new RejectedExecutionException(message);
                    }
                });
        userExecutor = new ThreadPoolExecutor(executorPoolSize, executorPoolSize, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new PoolExecutorThreadFactory(name + ".user-", classLoader),
                new RejectedExecutionHandler() {
                    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                        String message = "User executor rejected task: " + r + ", because client is shutting down...";
                        logger.finest(message);
                        throw new RejectedExecutionException(message);
                    }
                });
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        return internalExecutor.schedule(command, delay, unit);
    }

    @Override
    public <V> ScheduledFuture<Future<V>> schedule(Callable<V> command, long delay, TimeUnit unit) {
        return (ScheduledFuture<Future<V>>) internalExecutor.schedule(command, delay, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleWithRepetition(Runnable command, long initialDelay, long period, TimeUnit unit) {
        return internalExecutor.scheduleAtFixedRate(command, initialDelay, period, unit);
    }

    @Override
    public void execute(Runnable command) {
        internalExecutor.execute(command);
    }

    @Override
    public ExecutorService getUserExecutor() {
        return userExecutor;
    }

    @Probe (level = MANDATORY)
    public int getUserExecutorQueueSize() {
        return ((ThreadPoolExecutor) userExecutor).getQueue().size();
    }

    public void shutdown() {
        shutdownExecutor("user", userExecutor, logger);
        shutdownExecutor("internal", internalExecutor, logger);
    }

    public static void shutdownExecutor(String name, ExecutorService executor, ILogger logger) {
        executor.shutdown();
        try {
            boolean success = executor.awaitTermination(TERMINATE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (!success) {
                logger.warning(name + " executor awaitTermination could not complete in " + TERMINATE_TIMEOUT_SECONDS
                        + " seconds");
            }
        } catch (InterruptedException e) {
            currentThread().interrupt();
            logger.warning(name + " executor await termination is interrupted", e);
        }
    }

    @Override
    public void provideMetrics(MetricsRegistry registry) {
        registry.scanAndRegister(this, "executionService");
    }
}
