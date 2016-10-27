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

package com.hazelcast.client.spi;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.spi.ExecutionService;

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

/**
 * Executor service for Hazelcast clients.
 *
 * Allows asynchronous execution and scheduling of {@link Runnable} and {@link Callable} commands.
 */
public interface ClientExecutionService extends ExecutionService, Executor {

    /**
     * Execute alien (user code) on execution service
     *
     * @param command to run
     */
    @Override
    void execute(Runnable command);

    ICompletableFuture<?> submit(Runnable task);

    <T> ICompletableFuture<T> submit(Callable<T> task);

    /**
     * @return executorService that alien (user code) runs on
     */
    ExecutorService getAsyncExecutor();
}
