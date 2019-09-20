/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.worker.task.QueryTask;

/**
 * Query thread pool.
 */
public class QueryWorkerPool {
    /** Prefix for child threads. */
    private static final String THREAD_PREFIX = "query-thread-";

    /** Node engine. */
    private final NodeEngine nodeEngine;

    /** Number of threads. */
    private final int threadCnt;

    /** Workers. */
    protected final QueryWorker[] workers;

    public QueryWorkerPool(NodeEngine nodeEngine, int threadCnt) {
        this.nodeEngine = nodeEngine;
        this.threadCnt = threadCnt;

        workers = new QueryWorker[threadCnt];
    }

    /**
     * Start threads in the pool.
     */
    public void start() {
        for (int i = 0; i < threadCnt; i++) {
            QueryWorker worker = new QueryWorker(nodeEngine);

            Thread thread = new Thread(worker);

            thread.setName(THREAD_PREFIX + i);
            thread.start();

            workers[i] = worker;
        }

        for (QueryWorker worker : workers)
            worker.awaitStart();
    }

    /**
     * Shutdown threads.
     */
    public void shutdown() {
        for (QueryWorker worker : workers)
            worker.stop();
    }

    /**
     * Submit a task.
     *
     * @param task Task.
     */
    public void submit(int stripe, QueryTask task) {
        workers[stripe].offer(task);
    }

    public int getThreadCount() {
        return workers.length;
    }
}
