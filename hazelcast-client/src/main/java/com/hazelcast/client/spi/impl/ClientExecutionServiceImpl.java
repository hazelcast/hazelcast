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

import com.hazelcast.client.config.ClientProperties;
import com.hazelcast.client.config.ClientProperty;
import com.hazelcast.client.spi.ClientExecutionService;
import com.hazelcast.core.HazelcastOverloadException;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.util.executor.CompletableFutureTask;
import com.hazelcast.util.executor.PoolExecutorThreadFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public final class ClientExecutionServiceImpl implements ClientExecutionService {

    private static final ILogger LOGGER = Logger.getLogger(ClientExecutionService.class);
    private static final long TERMINATE_TIMEOUT_SECONDS = 30;

    private final ExecutorService userExecutor;
    private final ScheduledExecutorService internalExecutor;
    private int userExecutorQueueCapacity;
    private final LinkedBlockingQueue<Runnable> userExecutorQueue;

    public ClientExecutionServiceImpl(String name, ThreadGroup threadGroup, ClassLoader classLoader,
                                      ClientProperties properties, int poolSize) {
        int internalPoolSize = properties.getInteger(ClientProperty.INTERNAL_EXECUTOR_POOL_SIZE);
        if (internalPoolSize <= 0) {
            internalPoolSize = Integer.parseInt(ClientProperty.INTERNAL_EXECUTOR_POOL_SIZE.getDefaultValue());
        }

        userExecutorQueueCapacity = properties.getInteger(ClientProperty.USER_EXECUTOR_QUEUE_CAPACITY);
        if (userExecutorQueueCapacity < 0) {
            userExecutorQueueCapacity = Integer.MAX_VALUE;
        }

        int executorPoolSize = poolSize;
        if (executorPoolSize <= 0) {
            executorPoolSize = Runtime.getRuntime().availableProcessors();
        }
        internalExecutor = new ScheduledThreadPoolExecutor(internalPoolSize,
                new PoolExecutorThreadFactory(threadGroup, name + ".internal-", classLoader),
                new RejectedExecutionHandler() {
                    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                        String message = "Internal executor rejected task: " + r + ", because client is shutting down...";
                        LOGGER.finest(message);
                        throw new RejectedExecutionException(message);
                    }
                });

        userExecutorQueue = new LinkedBlockingQueue<Runnable>(userExecutorQueueCapacity);
        userExecutor = new ThreadPoolExecutor(executorPoolSize, executorPoolSize, 0L, TimeUnit.MILLISECONDS, userExecutorQueue,
                new PoolExecutorThreadFactory(threadGroup, name + ".user-", classLoader),
                new RejectedExecutionHandler() {
                    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                        RuntimeException e;
                        if (0 == userExecutorQueue.remainingCapacity()) {
                            e = new HazelcastOverloadException(
                                    "User executor rejected task: " + r + ", because executor current queue size limit of "
                                            + userExecutorQueueCapacity + " is reached!");
                        } else {
                            e = new RejectedExecutionException(
                                    "User executor rejected task: " + r + ", because client is shutting down...");
                        }

                        LOGGER.finest(e.getMessage());
                        throw e;
                    }
                });
    }

    public void executeInternal(Runnable runnable) {
        internalExecutor.execute(runnable);
    }

    public <T> ICompletableFuture<T> submitInternal(Runnable runnable) {
        CompletableFutureTask futureTask = new CompletableFutureTask(runnable, null, internalExecutor);
        internalExecutor.submit(futureTask);
        return futureTask;
    }

    @Override
    public void execute(Runnable command) {
        userExecutor.execute(command);
    }

    @Override
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

    /**
     * Utilized when given command needs to make a remote call. Response of remote call is not handled in runnable itself
     * but rather in  execution callback so that executor is not blocked because of a remote operation
     *
     * @param command
     * @param delay
     * @param unit
     * @return scheduledFuture
     */
    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        return internalExecutor.schedule(command, delay, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        return internalExecutor.scheduleAtFixedRate(command, initialDelay, period, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(final Runnable command, long initialDelay, long period, TimeUnit unit) {
        return internalExecutor.scheduleWithFixedDelay(command, initialDelay, period, unit);
    }

    @Override
    public ExecutorService getAsyncExecutor() {
        return userExecutor;
    }

    public void shutdown() {
        shutdownExecutor("user", userExecutor, LOGGER);
        shutdownExecutor("internal", internalExecutor, LOGGER);
    }

    public static void shutdownExecutor(String name, ExecutorService executor, ILogger logger) {
        executor.shutdown();
        try {
            boolean success = executor.awaitTermination(TERMINATE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (!success) {
                logger.warning(name + " executor awaitTermination could not completed in "
                        + TERMINATE_TIMEOUT_SECONDS + " seconds");
            }
        } catch (InterruptedException e) {
            logger.warning(name + " executor await termination is interrupted", e);
        }
    }

    public ExecutorService getInternalExecutor() {
        return internalExecutor;
    }
}
