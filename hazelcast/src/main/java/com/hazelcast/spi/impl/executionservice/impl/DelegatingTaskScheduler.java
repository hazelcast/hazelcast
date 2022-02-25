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

package com.hazelcast.spi.impl.executionservice.impl;

import com.hazelcast.spi.impl.executionservice.TaskScheduler;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

public final class DelegatingTaskScheduler implements TaskScheduler {

    private final ScheduledExecutorService scheduledExecutorService;
    private final ExecutorService executor;

    public DelegatingTaskScheduler(ScheduledExecutorService scheduledExecutorService, ExecutorService executor) {
        this.scheduledExecutorService = scheduledExecutorService;
        this.executor = executor;
    }

    @Override
    public void execute(Runnable command) {
        executor.execute(command);
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        checkNotNull(command);
        Runnable decoratedTask = new DelegatingTaskDecorator(command, executor);
        return scheduledExecutorService.schedule(decoratedTask, delay, unit);
    }

    @Override
    public <V> ScheduledFuture<Future<V>> schedule(Callable<V> command, long delay, TimeUnit unit) {
        checkNotNull(command);
        Callable<Future<V>> decoratedTask = new DelegatingCallableTaskDecorator<V>(command, executor);
        return scheduledExecutorService.schedule(decoratedTask, delay, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleWithRepetition(Runnable command, long initialDelay, long period, TimeUnit unit) {
        checkNotNull(command);
        Runnable decoratedTask = new DelegateAndSkipOnConcurrentExecutionDecorator(command, executor);
        return scheduledExecutorService.scheduleAtFixedRate(decoratedTask, initialDelay, period, unit);
    }

}
