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

package com.hazelcast.spi.impl.executionservice;

import com.hazelcast.spi.ExecutionService;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * The internal {@link ExecutionService}. Currently this method doesn't contains any additional methods, but
 * in the future they can be added here.
 */
public interface InternalExecutionService extends ExecutionService {

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
