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

package com.hazelcast.spi;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.util.executor.ExecutorType;
import com.hazelcast.util.executor.ManagedExecutorService;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * A Service responsible for providing access to 'system' executors and customer executors.
 * <p/>
 * It also has functionality for scheduling tasks.
 *
 */
public interface ExecutionService {

    String SYSTEM_EXECUTOR = "hz:system";
    String ASYNC_EXECUTOR = "hz:async";
    String SCHEDULED_EXECUTOR = "hz:scheduled";
    String CLIENT_EXECUTOR = "hz:client";
    String QUERY_EXECUTOR = "hz:query";
    String IO_EXECUTOR = "hz:io";

    ManagedExecutorService register(String name, int poolSize, int queueCapacity, ExecutorType type);

    ManagedExecutorService getExecutor(String name);

    void shutdownExecutor(String name);

    void execute(String name, Runnable command);

    Future<?> submit(String name, Runnable task);

    <T> Future<T> submit(String name, Callable<T> task);

    ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit);

    ScheduledFuture<?> schedule(String name, Runnable command, long delay, TimeUnit unit);

    ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit);

    ScheduledFuture<?> scheduleAtFixedRate(String name, Runnable command, long initialDelay, long period, TimeUnit unit);

    ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long period, TimeUnit unit);

    ScheduledFuture<?> scheduleWithFixedDelay(String name, Runnable command, long initialDelay, long period, TimeUnit unit);

    ScheduledExecutorService getDefaultScheduledExecutor();

    ScheduledExecutorService getScheduledExecutor(String name);

    <V> ICompletableFuture<V> asCompletableFuture(Future<V> future);
}
