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

package com.hazelcast.client.map.impl.querycache.subscriber;

import com.hazelcast.map.impl.querycache.QueryCacheScheduler;
import com.hazelcast.spi.impl.executionservice.TaskScheduler;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Client side implementation of {@code QueryCacheScheduler}.
 *
 * @see QueryCacheScheduler
 */
public class ClientQueryCacheScheduler implements QueryCacheScheduler {

    private final TaskScheduler executionService;

    public ClientQueryCacheScheduler(TaskScheduler executionService) {
        this.executionService = executionService;
    }

    @Override
    public void execute(Runnable task) {
        executionService.execute(task);
    }

    @Override
    public ScheduledFuture<?> scheduleWithRepetition(Runnable task, long delaySeconds) {
        return executionService.scheduleWithRepetition(task, 1, delaySeconds, TimeUnit.SECONDS);
    }

    @Override
    public void shutdown() {
        // intentionally not implemented for client-side.
    }
}
