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

import com.hazelcast.util.executor.ManagedExecutorService;

import java.util.concurrent.*;

/**
 * @author mdogan 12/14/12
 */
public interface ExecutionService {

    static final String SYSTEM_EXECUTOR = "hz:system";
    static final String OPERATION_EXECUTOR = "hz:operation";
    static final String ASYNC_EXECUTOR = "hz:async";
    static final String SCHEDULED_EXECUTOR = "hz:scheduled";
    static final String CLIENT_EXECUTOR = "hz:client";
    static final String QUERY_EXECUTOR = "hz:query";
    static final String IO_EXECUTOR = "hz:io";

    void execute(String name, Runnable command);

    Future<?> submit(String name, Runnable task);

    <T> Future<T> submit(String name, Callable<T> task);

    ManagedExecutorService getExecutor(String name);

    void shutdownExecutor(String name);

    ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit);

    ScheduledFuture<?> scheduleAtFixedRate(final Runnable command, long initialDelay, long period, TimeUnit unit);

    ScheduledFuture<?> scheduleWithFixedDelay(final Runnable command, long initialDelay, long period, TimeUnit unit);

    ScheduledExecutorService getScheduledExecutor();

}
