/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.instance.impl.OutOfMemoryErrorDispatcher.inspectOutOfMemoryError;

/**
 * Thread pool that executes query operations.
 */
public class QueryOperationWorkerPool {

    private final LocalMemberIdProvider localMemberIdProvider;
    private final QueryOperationHandler operationHandler;
    private final SerializationService serializationService;
    private final ILogger logger;

    private final ExecutorService exec;

    public QueryOperationWorkerPool(
        String instanceName,
        String workerName,
        int threadCount,
        LocalMemberIdProvider localMemberIdProvider,
        QueryOperationHandler operationHandler,
        SerializationService serializationService,
        ILogger logger,
        boolean system
    ) {
        this.localMemberIdProvider = localMemberIdProvider;
        this.operationHandler = operationHandler;
        this.serializationService = serializationService;
        this.logger = logger;

        WorkerThreadFactory threadFactory = new WorkerThreadFactory(instanceName, workerName, system);

        exec = new ForkJoinPool(threadCount, threadFactory, new ExceptionHandler(), true);
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

    public static boolean isSystemThread() {
        Thread thread = Thread.currentThread();

        return thread instanceof WorkerThread && ((WorkerThread) thread).isSystem();
    }

    private static final class WorkerThread extends ForkJoinWorkerThread {
        /** Whether this is a system thread. */
        private final boolean system;

        private WorkerThread(ForkJoinPool pool, boolean system) {
            super(pool);

            this.system = system;
        }

        private boolean isSystem() {
            return system;
        }
    }

    private static final class WorkerThreadFactory implements ForkJoinPool.ForkJoinWorkerThreadFactory {

        private final AtomicLong counter = new AtomicLong();
        private final String instanceName;
        private final String workerName;
        private final boolean system;

        private WorkerThreadFactory(String instanceName, String workerName, boolean system) {
            this.instanceName = instanceName;
            this.workerName = workerName;
            this.system = system;
        }

        @Override
        public ForkJoinWorkerThread newThread(ForkJoinPool pool) {
            String name = QueryUtils.workerName(instanceName, workerName, counter.incrementAndGet());

            WorkerThread thread = new WorkerThread(pool, system);
            thread.setName(name);

            return thread;
        }
    }

    private class ExceptionHandler implements Thread.UncaughtExceptionHandler {
        @Override
        public void uncaughtException(Thread thread, Throwable t) {
            inspectOutOfMemoryError(t);
            logger.severe(t);
        }
    }
}
