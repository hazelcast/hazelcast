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

import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.QueryFragmentExecutable;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Query thread pool.
 */
public class QueryWorkerPool {
    /** Prefix for child threads. */
    private static final String THREAD_PREFIX = "query-thread-";

    /** Workers. */
    private final QueryWorker[] workers;

    /** Node engine. */
    private final NodeEngine nodeEngine;

    /** Number of threads. */
    private final int threadCnt;

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
            QueryWorker worker = new QueryWorker(nodeEngine, this);

            Thread thread = new Thread(worker);

            thread.setName(THREAD_PREFIX + i);
            thread.start();

            workers[i] = worker;
        }

        for (QueryWorker worker : workers) {
            worker.awaitStart();
        }
    }

    /**
     * Shutdown threads.
     */
    public void shutdown() {
        for (QueryWorker worker : workers) {
            worker.stop();
        }
    }

    /**
     * Submit a fragment to be executed.
     *
     * @param fragmentExecutable Fragment.
     */
    public void submit(QueryFragmentExecutable fragmentExecutable) {
        workers[ThreadLocalRandom.current().nextInt(workers.length)].submit(fragmentExecutable);
    }
}
