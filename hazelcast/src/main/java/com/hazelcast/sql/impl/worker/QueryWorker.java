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

import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.util.EmptyStatement;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.QueryFragmentExecutable;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Query worker.
 */
public class QueryWorker implements Runnable {
    private static final Object POISON = new Object();

    /** Task queue. */
    private final LinkedBlockingDeque<Object> queue = new LinkedBlockingDeque<>();

    /** Start latch. */
    private final CountDownLatch startLatch = new CountDownLatch(1);

    /** Parent pool.. */
    private final QueryWorkerPool workerPool;

    /** Logger. */
    private final ILogger logger;

    /** Stop flag. */
    private boolean stop;

    protected QueryWorker(NodeEngine nodeEngine, QueryWorkerPool workerPool) {
        this.workerPool = workerPool;

        logger = nodeEngine.getLogger(this.getClass());
    }

    /**
     * Await worker start.
     */
    public void awaitStart() {
        try {
            startLatch.await();
        } catch (InterruptedException ignore) {
            Thread.currentThread().interrupt();

            throw new HazelcastException("Failed to wait for worker start (thread has been interrupted).");
        }
    }

    /**
     * Submit a new fragment for execution.
     *
     * @param fragmentExecutable Task.
     */
    public void submit(QueryFragmentExecutable fragmentExecutable) {
        queue.offer(fragmentExecutable);
    }

    /**
     * Stop the worker with a poison pill.
     */
    public void stop() {
        queue.offerFirst(POISON);
    }

    @Override
    public void run() {
        startLatch.countDown();

        while (!stop) {
            try {
                Object nextTask = queue.take();

                if (nextTask == POISON) {
                    try {
                        queue.clear();
                    } finally {
                        stop = true;
                    }
                } else {
                    QueryFragmentExecutable fragment = (QueryFragmentExecutable) nextTask;

                    fragment.run(workerPool);
                }
            } catch (InterruptedException e) {
                EmptyStatement.ignore(e);
            } catch (Exception e) {
                // Should never happen except of bugs. Write to log in case we missed some case (bug).
                logger.warning("Unexpected exception in SQL worker [threadName="
                    + Thread.currentThread().getName() + ']', e);
            }
        }
    }
}
