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

import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.util.EmptyStatement;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Abstract worker.
 *
 * @param <T> Type of the task.
 */
public abstract class AbstractWorker<T extends WorkerTask> implements Runnable {
    /** Task queue. */
    private final LinkedBlockingDeque<WorkerTask> queue = new LinkedBlockingDeque<>();

    /** Start latch. */
    private final CountDownLatch startLatch = new CountDownLatch(1);

    /** Node engine. */
    protected final NodeEngine nodeEngine;

    /** Logger. */
    protected final ILogger logger;

    /** Stop flag. */
    private boolean stop;

    protected AbstractWorker(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;

        logger = nodeEngine.getLogger(this.getClass());
    }

    /**
     * Await worker start.
     */
    public void awaitStart() {
        try {
            startLatch.await();
        }
        catch (InterruptedException ignore) {
            Thread.currentThread().interrupt();

            throw new HazelcastException("Failed to wait for worker start (thread has been interrupted).");
        }
    }

    /**
     * Offer a task.
     *
     * @param task Task.
     */
    public void offer(T task) {
        queue.offer(task);
    }

    /**
     * Stop the worker with a poison pill.
     */
    public void stop() {
        queue.offerFirst(StopWorkerTask.INSTANCE);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void run() {
        startLatch.countDown();

        while (!stop) {
            try {
                WorkerTask nextTask = queue.take();

                if (nextTask instanceof StopWorkerTask) {
                    try {
                        onStop();
                    }
                    finally {
                        stop = true;
                    }
                }
                else
                    executeTask((T)nextTask);
            }
            catch (InterruptedException e) {
                EmptyStatement.ignore(e);
            }
            catch (Exception e) {
                // Should never happen except of bugs. Write to log in case we missed some case (bug).
                logger.warning("Unexpected exception in SQL worker [threadName=" +
                    Thread.currentThread().getName() + ']', e);
            }
        }
    }

    /**
     * Execute the task.
     *
     * @param task Task.
     */
    protected abstract void executeTask(T task);

    /**
     * Handle stop.
     */
    protected abstract void onStop();
}
