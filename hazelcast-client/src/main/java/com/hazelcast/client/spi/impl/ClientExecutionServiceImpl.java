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
import com.hazelcast.util.executor.CompletableFutureTask;
import com.hazelcast.util.executor.PoolExecutorThreadFactory;
import com.hazelcast.util.executor.SingleExecutorThreadFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author mdogan 5/16/13
 */
public final class ClientExecutionServiceImpl implements ClientExecutionService {

    private final ExecutorService executor;
    private final ScheduledExecutorService scheduledExecutor;

    public ClientExecutionServiceImpl(String name, ThreadGroup threadGroup, ClassLoader classLoader, int poolSize) {
        if (poolSize <= 0) {
            final int cores = Runtime.getRuntime().availableProcessors();
            poolSize = cores * 5;
        }
        executor = Executors.newFixedThreadPool(poolSize, new PoolExecutorThreadFactory(threadGroup, name + ".cached-", classLoader));
//        executor = Executors.newCachedThreadPool(new PoolExecutorThreadFactory(threadGroup, name + ".cached-", classLoader));

        scheduledExecutor = Executors.newSingleThreadScheduledExecutor(new SingleExecutorThreadFactory(threadGroup, classLoader, name + ".scheduled"));
    }

    public ExecutorService getExecutor() {
        return executor;
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
                execute(command);
            }
        }, delay, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(final Runnable command, long initialDelay, long period, TimeUnit unit) {
        return scheduledExecutor.scheduleAtFixedRate(new Runnable() {
            public void run() {
                execute(command);
            }
        }, initialDelay, period, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(final Runnable command, long initialDelay, long period, TimeUnit unit) {
        return scheduledExecutor.scheduleWithFixedDelay(new Runnable() {
            public void run() {
                execute(command);
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
