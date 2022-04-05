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

package com.hazelcast.spi.impl.executionservice;

import com.hazelcast.map.MapLoader;
import com.hazelcast.internal.util.executor.ExecutorType;
import com.hazelcast.internal.util.executor.ManagedExecutorService;
import com.hazelcast.spi.impl.InternalCompletableFuture;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
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
     * Name of the client query executor.
     */
    String CLIENT_QUERY_EXECUTOR = "hz:client-query";

    /**
     * Name of the client transaction executor.
     */
    String CLIENT_BLOCKING_EXECUTOR = "hz:client-blocking-tasks";

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
     * @see MapLoader#loadAll(java.util.Collection)
     */
    String MAP_LOADER_EXECUTOR = "hz:map-load";

    /**
     * The name of the executor that loads the entry keys and dispatches
     * the keys to the partition owners for value loading.
     *
     * @see MapLoader#loadAllKeys()
     */
    String MAP_LOAD_ALL_KEYS_EXECUTOR = "hz:map-loadAllKeys";

    /**
     * Name of the Management Center executor. Used to execute blocking tasks
     * related with operations run by Management Center.
     */
    String MC_EXECUTOR = "hz:mc";

    /**
     * @param name          for the executor service
     * @param poolSize      the maximum number of threads to allow in the pool
     * @param queueCapacity the queue to use for holding tasks before they are executed.
     * @param type          {@link ExecutorType#CACHED} or {@link ExecutorType#CONCRETE}
     * @return the created managed executor service
     */
    ManagedExecutorService register(String name, int poolSize, int queueCapacity, ExecutorType type);

    /**
     * This register method creates the executor only on @{@link ExecutorType#CONCRETE} type.
     * The executors with @{@link ExecutorType#CACHED} types can not have custom thread factory since they will share the
     * threads with other @{@link ExecutorType#CACHED} executors.
     *
     * @param name          for the executor service
     * @param poolSize      the maximum number of threads to allow in the pool
     * @param queueCapacity the queue to use for holding tasks before they are executed.
     * @param threadFactory custom thread factory for the managed executor service.
     * @return managed executor service
     */
    ManagedExecutorService register(String name, int poolSize, int queueCapacity, ThreadFactory threadFactory);

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

    <V> InternalCompletableFuture<V> asCompletableFuture(Future<V> future);

    ExecutorService getDurable(String name);

    ExecutorService getScheduledDurable(String name);

    void executeDurable(String name, Runnable command);

    ScheduledFuture<?> scheduleDurable(String name, Runnable command, long delay, TimeUnit unit);

    <V> ScheduledFuture<Future<V>> scheduleDurable(String name, Callable<V> command, long delay, TimeUnit unit);

    ScheduledFuture<?> scheduleDurableWithRepetition(String name, Runnable command,
                                                     long initialDelay, long period, TimeUnit unit);

    void shutdownDurableExecutor(String name);

    void shutdownScheduledDurableExecutor(String name);
}
