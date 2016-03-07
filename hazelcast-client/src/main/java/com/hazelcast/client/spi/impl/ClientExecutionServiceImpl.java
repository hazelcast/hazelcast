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

package com.hazelcast.client.spi.impl;

import com.hazelcast.client.spi.ClientExecutionService;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.impl.executionservice.impl.SkipOnConcurrentExecutionDecorator;
import com.hazelcast.util.executor.CompletableFutureTask;
import com.hazelcast.util.executor.PoolExecutorThreadFactory;
import com.hazelcast.util.executor.SingleExecutorThreadFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public final class ClientExecutionServiceImpl implements ClientExecutionService {

    private static final ILogger LOGGER = Logger.getLogger(ClientExecutionService.class);
    private static final long TERMINATE_TIMEOUT_SECONDS = 30;
    private final ExecutorService userExecutor;
    private final ExecutorService internalExecutor;
    private final ScheduledExecutorService scheduledExecutor;

    public ClientExecutionServiceImpl(String name, ThreadGroup threadGroup, ClassLoader classLoader, int poolSize) {
        int executorPoolSize = poolSize;
        if (executorPoolSize <= 0) {
            executorPoolSize = Runtime.getRuntime().availableProcessors();
        }

        internalExecutor = new ThreadPoolExecutor(2, 2, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new PoolExecutorThreadFactory(threadGroup, name + ".internal-", classLoader),
                new RejectedExecutionHandler() {
                    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                        String message = "Internal executor rejected task: " + r + ", because client is shutting down...";
                        LOGGER.finest(message);
                        throw new RejectedExecutionException(message);
                    }
                });
        userExecutor = new ThreadPoolExecutor(executorPoolSize, executorPoolSize, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new PoolExecutorThreadFactory(threadGroup, name + ".user-", classLoader),
                new RejectedExecutionHandler() {
                    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                        String message = "Internal executor rejected task: " + r + ", because client is shutting down...";
                        LOGGER.finest(message);
                        throw new RejectedExecutionException(message);
                    }
                });

        scheduledExecutor = Executors.newSingleThreadScheduledExecutor(
                new SingleExecutorThreadFactory(threadGroup, classLoader, name + ".scheduled"));

    }

    public <T> ICompletableFuture<T> submitInternal(Runnable runnable) {
        CompletableFutureTask futureTask = new CompletableFutureTask(runnable, null, internalExecutor);
        internalExecutor.submit(futureTask);
        return futureTask;
    }

    public void executeInternal(Runnable runnable) {
        internalExecutor.execute(runnable);
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

    @Override
    public ScheduledFuture<?> schedule(final Runnable command, long delay, TimeUnit unit) {
        return scheduledExecutor.schedule(new Runnable() {
            public void run() {
                executeInternal(command);
            }
        }, delay, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleWithRepetition(final Runnable command, long initialDelay, long period, TimeUnit unit) {
        final Runnable decoratedCommand = new SkipOnConcurrentExecutionDecorator(command);
        return scheduledExecutor.scheduleAtFixedRate(new Runnable() {
            public void run() {
                executeInternal(decoratedCommand);
            }
        }, initialDelay, period, unit);
    }

    @Override
    public ExecutorService getAsyncExecutor() {
        return userExecutor;
    }

    public void shutdown() {
        shutdownExecutor("internal", internalExecutor);
        shutdownExecutor("scheduled", scheduledExecutor);
        shutdownExecutor("user", userExecutor);
    }

    private void shutdownExecutor(String name, ExecutorService executor) {
        executor.shutdown();
        try {
            boolean success = executor.awaitTermination(TERMINATE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (!success) {
                LOGGER.warning(name + " executor awaitTermination could not completed in "
                        + TERMINATE_TIMEOUT_SECONDS + " seconds");
            }
        } catch (InterruptedException e) {
            LOGGER.warning(name + " executor await termination is interrupted", e);
        }
    }
}
