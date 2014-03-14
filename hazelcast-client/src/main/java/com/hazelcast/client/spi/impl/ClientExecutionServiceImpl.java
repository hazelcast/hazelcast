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

package com.hazelcast.client.spi.impl;

import com.hazelcast.client.spi.ClientExecutionService;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.util.executor.CompletableFutureTask;
import com.hazelcast.util.executor.PoolExecutorThreadFactory;
import com.hazelcast.util.executor.SingleExecutorThreadFactory;

import java.util.concurrent.*;

/**
 * @author mdogan 5/16/13
 */
public final class ClientExecutionServiceImpl implements ClientExecutionService {

    private static final ILogger logger = Logger.getLogger(ClientExecutionService.class);

    private final ExecutorService executor;
    private final ExecutorService internalExecutor;
    private final ScheduledExecutorService scheduledExecutor;

    public ClientExecutionServiceImpl(String name, ThreadGroup threadGroup, ClassLoader classLoader, int poolSize) {
        if (poolSize <= 0){
            final int cores = Runtime.getRuntime().availableProcessors();
            poolSize = cores * 5;
        }
        internalExecutor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new PoolExecutorThreadFactory(threadGroup, name + ".internal-", classLoader),
                new RejectedExecutionHandler() {
                    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                        String message = "Internal executor rejected task: " + r + ", because client is shutting down...";
                        logger.finest(message);
                        throw new RejectedExecutionException(message);
                    }
                });
        executor = new ThreadPoolExecutor(poolSize, poolSize, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new PoolExecutorThreadFactory(threadGroup, name + ".cached-", classLoader),
                new RejectedExecutionHandler() {
                    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                        String message = "Internal executor rejected task: " + r + ", because client is shutting down...";
                        logger.finest(message);
                        throw new RejectedExecutionException(message);
                    }
                });


        scheduledExecutor = Executors.newSingleThreadScheduledExecutor(
                new SingleExecutorThreadFactory(threadGroup, classLoader, name + ".scheduled"));
    }

    public void executeInternal(Runnable command) {
        internalExecutor.execute(command);
    }

    @Override
    public void execute(Runnable command) {
        executor.execute(command);
    }

    @Override
    public ICompletableFuture<?> submit(Runnable task) {
        CompletableFutureTask futureTask = new CompletableFutureTask(task, null, getAsyncExecutor());
        executor.submit(futureTask);
        return futureTask;
    }

    @Override
    public <T> ICompletableFuture<T> submit(Callable<T> task) {
        CompletableFutureTask<T> futureTask = new CompletableFutureTask<T>(task, getAsyncExecutor());
        executor.submit(futureTask);
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
    public ScheduledFuture<?> scheduleAtFixedRate(final Runnable command, long initialDelay, long period, TimeUnit unit) {
        return scheduledExecutor.scheduleAtFixedRate(new Runnable() {
            public void run() {
                executeInternal(command);
            }
        }, initialDelay, period, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(final Runnable command, long initialDelay, long period, TimeUnit unit) {
        return scheduledExecutor.scheduleWithFixedDelay(new Runnable() {
            public void run() {
                executeInternal(command);
            }
        }, initialDelay, period, unit);
    }

    @Override
    public ExecutorService getAsyncExecutor() {
        return executor;
    }

    public void shutdown() {
        scheduledExecutor.shutdownNow();
        executor.shutdownNow();
    }
}
