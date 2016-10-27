/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.spi.TaskScheduler;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;
import com.hazelcast.util.executor.CompletableFutureTask;
import com.hazelcast.util.executor.ExecutorType;
import com.hazelcast.util.executor.LoggingScheduledExecutor;
import com.hazelcast.util.executor.ManagedExecutorService;
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

public final class ClientExecutionServiceImpl implements ClientExecutionService {

    public static final HazelcastProperty INTERNAL_EXECUTOR_POOL_SIZE
            = new HazelcastProperty("hazelcast.client.internal.executor.pool.size", 3);

    private static final long TERMINATE_TIMEOUT_SECONDS = 30;

    private final ILogger logger;
    private final ExecutorService userExecutor;
    private final ScheduledExecutorService internalExecutor;

    public ClientExecutionServiceImpl(String name, ThreadGroup threadGroup, ClassLoader classLoader,
                                      HazelcastProperties properties, int poolSize, LoggingService loggingService) {
        int internalPoolSize = properties.getInteger(INTERNAL_EXECUTOR_POOL_SIZE);
        if (internalPoolSize <= 0) {
            internalPoolSize = Integer.parseInt(INTERNAL_EXECUTOR_POOL_SIZE.getDefaultValue());
        }
        int executorPoolSize = poolSize;
        if (executorPoolSize <= 0) {
            executorPoolSize = Runtime.getRuntime().availableProcessors();
        }
        logger = loggingService.getLogger(ClientExecutionService.class);
        internalExecutor = new LoggingScheduledExecutor(logger, internalPoolSize,
                new PoolExecutorThreadFactory(threadGroup, name + ".internal-", classLoader),
                new RejectedExecutionHandler() {
                    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                        String message = "Internal executor rejected task: " + r + ", because client is shutting down...";
                        logger.finest(message);
                        throw new RejectedExecutionException(message);
                    }
                });
        userExecutor = new ThreadPoolExecutor(executorPoolSize, executorPoolSize, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new PoolExecutorThreadFactory(threadGroup, name + ".user-", classLoader),
                new RejectedExecutionHandler() {
                    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                        String message = "Internal executor rejected task: " + r + ", because client is shutting down...";
                        logger.finest(message);
                        throw new RejectedExecutionException(message);
                    }
                });
    }

    public void executeInternal(Runnable runnable) {
        internalExecutor.execute(runnable);
    }

    public <T> ICompletableFuture<T> submitInternal(Runnable runnable) {
        CompletableFutureTask<T> futureTask = new CompletableFutureTask<T>(runnable, null, internalExecutor);
        internalExecutor.submit(futureTask);
        return futureTask;
    }

    @Override
    public void execute(Runnable command) {
        userExecutor.execute(command);
    }

    @Override
    @SuppressWarnings("unchecked")
    public ICompletableFuture<?> submit(Runnable task) {
        CompletableFutureTask futureTask = new CompletableFutureTask(task, null, getAsyncExecutor());
        userExecutor.submit(futureTask);
        return futureTask;
    }

    @Override
    public <T> ICompletableFuture<T> submit(Callable<T> task) {
        CompletableFutureTask<T> futureTask = new CompletableFutureTask<T>(task, getAsyncExecutor());
        userExecutor.submit(futureTask);
        return futureTask;
    }

    @Override
    public ManagedExecutorService register(String name, int poolSize, int queueCapacity, ExecutorType type) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ManagedExecutorService getExecutor(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void shutdownExecutor(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void execute(String name, Runnable command) {
        execute(command);
    }

    @Override
    public Future<?> submit(String name, Runnable task) {
        return submit(task);
    }

    @Override
    public <T> Future<T> submit(String name, Callable<T> task) {
        return submit(task);
    }

    /**
     * Utilized when given command needs to make a remote call.
     * <p>
     * The response of the remote call is not handled in the {@link Runnable} itself, but rather
     * in the execution callback so that executor is not blocked because of a remote operation.
     *
     * @param command the {@link Runnable} to schedule
     * @param delay   the delay for the scheduled execution
     * @param unit    the {@link TimeUnit} of the delay
     * @return scheduledFuture
     */
    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        return internalExecutor.schedule(command, delay, unit);
    }

    @Override
    public ScheduledFuture<?> schedule(String name, Runnable command, long delay, TimeUnit unit) {
        return schedule(command, delay, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleWithRepetition(Runnable command, long initialDelay, long period, TimeUnit unit) {
        return internalExecutor.scheduleAtFixedRate(command, initialDelay, period, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleWithRepetition(String name, Runnable command, long initialDelay, long period,
                                                     TimeUnit unit) {
        return scheduleWithRepetition(command, initialDelay, period, unit);
    }

    @Override
    public TaskScheduler getGlobalTaskScheduler() {
        throw new UnsupportedOperationException();
    }

    @Override
    public TaskScheduler getTaskScheduler(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <V> ICompletableFuture<V> asCompletableFuture(Future<V> future) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ExecutorService getAsyncExecutor() {
        return userExecutor;
    }

    public ExecutorService getInternalExecutor() {
        return internalExecutor;
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
            logger.warning(name + " executor await termination is interrupted", e);
        }
    }
}
