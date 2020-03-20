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

import com.hazelcast.sql.impl.QueryUtils;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.sql.impl.QueryUtils.WORKER_TYPE_FRAGMENT;

/**
 * Thread pool which executes query fragments
 */
public class QueryFragmentWorkerPool {

    private final ForkJoinPool pool;

    public QueryFragmentWorkerPool(String instanceName, int threadCount) {
        pool = new ForkJoinPool(threadCount, new WorkerThreadFactory(instanceName), null, false);
    }

    /**
     * Stop the pool.
     */
    public void stop() {
        pool.shutdown();
    }

    /**
     * Schedule query fragment in the pool.
     *
     * @param task Fragment.
     */
    public void submit(QueryFragmentExecutable task) {
        pool.submit(task::run);
    }

    private static final class WorkerThread extends ForkJoinWorkerThread {
        private WorkerThread(ForkJoinPool pool) {
            super(pool);
        }
    }

    private static final class WorkerThreadFactory implements ForkJoinPool.ForkJoinWorkerThreadFactory {

        private final String instanceName;
        private final AtomicLong indexGenerator = new AtomicLong();

        private WorkerThreadFactory(String instanceName) {
            this.instanceName = instanceName;
        }

        @Override
        public ForkJoinWorkerThread newThread(ForkJoinPool pool) {
            String name = QueryUtils.workerName(instanceName, WORKER_TYPE_FRAGMENT, indexGenerator.incrementAndGet());

            WorkerThread thread = new WorkerThread(pool);

            thread.setName(name);

            return thread;
        }
    }
}
