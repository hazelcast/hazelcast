/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.querycache.subscriber;

import com.hazelcast.client.spi.ClientExecutorConstants;
import com.hazelcast.map.impl.querycache.QueryCacheScheduler;
import com.hazelcast.spi.ExecutionService;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Client side implementation of {@code QueryCacheScheduler}.
 *
 * @see QueryCacheScheduler
 */
public class ClientQueryCacheScheduler implements QueryCacheScheduler {

    private final ExecutionService executionService;

    public ClientQueryCacheScheduler(ExecutionService executionService) {
        this.executionService = executionService;
    }

    @Override
    public void execute(Runnable task) {
        executionService.execute(ClientExecutorConstants.INTERNAL_EXECUTOR, task);
    }

    @Override
    public ScheduledFuture<?> scheduleWithRepetition(Runnable task, long delaySeconds) {
        return executionService.scheduleWithRepetition(ClientExecutorConstants.INTERNAL_EXECUTOR,
                task, 1, delaySeconds, TimeUnit.SECONDS);
    }

    @Override
    public void shutdown() {
        // intentionally not implemented for client-side.
    }
}
