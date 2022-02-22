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

package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.querycache.QueryCacheScheduler;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.executionservice.TaskScheduler;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.internal.util.executor.ExecutorType;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Node side implementation of {@code QueryCacheScheduler}.
 *
 * @see QueryCacheScheduler
 */
public class NodeQueryCacheScheduler implements QueryCacheScheduler {

    /**
     * Prefix for #executorName.
     */
    private static final String EXECUTOR_NAME_PREFIX = "hz:scheduled:cqc:";

    /**
     * Default work-queue capacity for this executor.
     */
    private static final int EXECUTOR_DEFAULT_QUEUE_CAPACITY = 10000;

    private final String executorName;
    private final TaskScheduler taskScheduler;
    private final ExecutionService executionService;

    public NodeQueryCacheScheduler(MapServiceContext mapServiceContext) {
        executionService = getExecutionService(mapServiceContext);
        executorName = EXECUTOR_NAME_PREFIX + UuidUtil.newUnsecureUuidString();
        executionService.register(executorName, 1, EXECUTOR_DEFAULT_QUEUE_CAPACITY, ExecutorType.CACHED);
        taskScheduler = executionService.getTaskScheduler(executorName);
    }

    private ExecutionService getExecutionService(MapServiceContext mapServiceContext) {
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        return nodeEngine.getExecutionService();
    }

    @Override
    public ScheduledFuture<?> scheduleWithRepetition(Runnable task, long delaySeconds) {
        return taskScheduler.scheduleWithRepetition(task, 1, delaySeconds, TimeUnit.SECONDS);
    }

    @Override
    public void execute(Runnable task) {
        taskScheduler.execute(task);
    }

    @Override
    public void shutdown() {
        executionService.shutdownExecutor(executorName);
    }
}
