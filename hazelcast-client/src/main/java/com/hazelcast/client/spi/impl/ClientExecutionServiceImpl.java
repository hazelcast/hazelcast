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
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
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
    private static final ExecutionCallback FAILURE_LOGGING_EXECUTION_CALLBACK = new ExecutionCallback() {
        @Override
        public void onResponse(Object response) {

        }

        @Override
        public void onFailure(Throwable t) {
            LOGGER.warning("Rejected internal execution on scheduledExecutor", t);
        }
    };

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
     * @param executionCallback
     * @return scheduledFuture
     */
    public ScheduledFuture<?> schedule(final Runnable command, long delay, TimeUnit unit,
                                       final ExecutionCallback executionCallback) {
        return scheduledExecutor.schedule(new Runnable() {
            public void run() {
                executeInternalSafely(command, executionCallback);
            }
        }, delay, unit);
    }

    @Override
    public ScheduledFuture<?> schedule(final Runnable command, long delay, TimeUnit unit) {
        return scheduledExecutor.schedule(new Runnable() {
            public void run() {
                executeInternalSafely(command, FAILURE_LOGGING_EXECUTION_CALLBACK);
            }
        }, delay, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(final Runnable command, long initialDelay, long period, TimeUnit unit) {
        return scheduledExecutor.scheduleAtFixedRate(new Runnable() {
            public void run() {
                executeInternalSafely(command, FAILURE_LOGGING_EXECUTION_CALLBACK);
            }
        }, initialDelay, period, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(final Runnable command, long initialDelay, long period, TimeUnit unit) {
        return scheduledExecutor.scheduleWithFixedDelay(new Runnable() {
            public void run() {
                executeInternalSafely(command, FAILURE_LOGGING_EXECUTION_CALLBACK);
            }
        }, initialDelay, period, unit);
    }

    @Override
    public ExecutorService getAsyncExecutor() {
        return userExecutor;
    }

    public void shutdown() {
        shutdownExecutor("scheduled", scheduledExecutor);
        shutdownExecutor("user", userExecutor);
        shutdownExecutor("internal", internalExecutor);
    }

    private void executeInternalSafely(Runnable command, ExecutionCallback executionCallback) {
        try {
            submitInternal(command).andThen(executionCallback);
        } catch (RejectedExecutionException e) {
            executionCallback.onFailure(e);
        }
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

    public ExecutorService getInternalExecutor() {
        return internalExecutor;
    }
}
