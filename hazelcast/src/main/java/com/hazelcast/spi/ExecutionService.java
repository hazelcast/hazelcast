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

package com.hazelcast.spi;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.util.executor.ExecutorType;
import com.hazelcast.util.executor.ManagedExecutorService;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * A Service responsible for providing access to 'system' executors and customer executors.
 *
 * It also has functionality for scheduling tasks.
 */
public interface ExecutionService {

    /**
     * Name of the system executor.
     */
    String SYSTEM_EXECUTOR = "hz:system";

    /**
     * Name of the async executor.
     */
    String ASYNC_EXECUTOR = "hz:async";

    /**
     * Name of the scheduled executor.
     */
    String SCHEDULED_EXECUTOR = "hz:scheduled";

    /**
     * Name of the client executor.
     */
    String CLIENT_EXECUTOR = "hz:client";

    /**
     * Name of the client executor.
     */
    String CLIENT_QUERY_EXECUTOR = "hz:client-query";

    /**
     * Name of the client management executor.
     */
    String CLIENT_MANAGEMENT_EXECUTOR = "hz:client-management";

    /**
     * Name of the query executor.
     */
    String QUERY_EXECUTOR = "hz:query";

    /**
     * Name of the io executor.
     */
    String IO_EXECUTOR = "hz:io";

    /**
     * Name of the offloadable executor.
     */
    String OFFLOADABLE_EXECUTOR = "hz:offloadable";

    /**
     * Name of the map-loader executor that loads the entry values
     * for a given key set locally on the member owning the partition
     * which contains the keys. This is the executor you want to
     * configure when you want to load more data from the database
     * in parallel.
     *
     * @see com.hazelcast.core.MapLoader#loadAll(java.util.Collection)
     */
    String MAP_LOADER_EXECUTOR = "hz:map-load";

    /**
     * The name of the executor that loads the entry keys and dispatches
     * the keys to the partition owners for value loading.
     *
     * @see com.hazelcast.core.MapLoader#loadAllKeys()
     */
    String MAP_LOAD_ALL_KEYS_EXECUTOR = "hz:map-loadAllKeys";

    ManagedExecutorService register(String name, int poolSize, int queueCapacity, ExecutorType type);

    ManagedExecutorService getExecutor(String name);

    void shutdownExecutor(String name);

    void execute(String name, Runnable command);

    Future<?> submit(String name, Runnable task);

    <T> Future<T> submit(String name, Callable<T> task);

    ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit);

    ScheduledFuture<?> schedule(String name, Runnable command, long delay, TimeUnit unit);

    ScheduledFuture<?> scheduleWithRepetition(Runnable command, long initialDelay, long period, TimeUnit unit);

    ScheduledFuture<?> scheduleWithRepetition(String name, Runnable command, long initialDelay, long period, TimeUnit unit);

    TaskScheduler getGlobalTaskScheduler();

    TaskScheduler getTaskScheduler(String name);

    <V> ICompletableFuture<V> asCompletableFuture(Future<V> future);
}
