/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.worker;

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.sql.impl.LocalMemberIdProvider;
import com.hazelcast.sql.impl.QueryUtils;
import com.hazelcast.sql.impl.operation.QueryOperationHandler;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Thread pool that executes query operations.
 */
// TODO: Rename
public class QueryOperationWorkerPool {

    private final LocalMemberIdProvider localMemberIdProvider;
    private final QueryOperationHandler operationHandler;
    private final SerializationService serializationService;
    private final ILogger logger;

    private final ExecutorService exec;

    // TODO: Remove context variables from here?
    public QueryOperationWorkerPool(
        String instanceName,
        String workerName,
        int threadCount,
        LocalMemberIdProvider localMemberIdProvider,
        QueryOperationHandler operationHandler,
        SerializationService serializationService,
        ILogger logger
    ) {
        this.localMemberIdProvider = localMemberIdProvider;
        this.operationHandler = operationHandler;
        this.serializationService = serializationService;
        this.logger = logger;

        exec = new ForkJoinPool(threadCount);
    }

    public void submit(QueryOperationExecutable task) {
        exec.submit(
            new QueryPoolTask(
                task,
                localMemberIdProvider,
                operationHandler,
                serializationService,
                logger
            )
        );
    }

    public void stop() {
        exec.shutdownNow();
    }

    private static final class PoolThreadFactory implements ForkJoinPool.ForkJoinWorkerThreadFactory {

        private final AtomicLong counter = new AtomicLong();
        private final String instanceName;
        private final String workerName;

        private PoolThreadFactory(String instanceName, String workerName) {
            this.instanceName = instanceName;
            this.workerName = workerName;
        }

        @Override
        public Thread newThread(@NotNull Runnable r) {
            String name = QueryUtils.workerName(instanceName, workerName, counter.incrementAndGet());

            Thread thread = new Thread(r);
            thread.setName(name);

            return thread;
        }
    }
}
