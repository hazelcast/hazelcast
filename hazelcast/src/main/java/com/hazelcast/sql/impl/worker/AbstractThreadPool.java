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

/**
 * Abstract striped thread pool.
 *
 * @param <T> Type of worker.
 */
public abstract class AbstractThreadPool<T extends AbstractWorker> {
    /** Prefix assigned to newly created threads. */
    private final String threadPrefix;

    /** Number of threads. */
    private final int threadCnt;

    /** Workers. */
    protected final AbstractWorker[] workers;

    public AbstractThreadPool(String threadPrefix, int threadCnt) {
        this.threadPrefix = threadPrefix;
        this.threadCnt = threadCnt;

        workers = new AbstractWorker[threadCnt];
    }

    /**
     * Start threads in the pool.
     */
    public void start() {
        for (int i = 0; i < threadCnt; i++) {
            T worker = createWorker(i);

            Thread thread = new Thread(worker);

            thread.setName(threadPrefix + i);
            thread.start();

            workers[i] = worker;
        }

        for (AbstractWorker worker : workers)
            worker.awaitStart();
    }

    /**
     * Shutdown threads.
     */
    public void shutdown() {
        for (AbstractWorker worker : workers)
            worker.stop();

        // TODO TODO: Should we wait for completion here? Probably yes, otherwise clean instance stop is not possible.
    }

    protected int getThreadCount() {
        return workers.length;
    }

    @SuppressWarnings("unchecked")
    protected T getWorker(int idx) {
        return (T)workers[idx];
    }

    /**
     * Create a worker.
     *
     * @param idx Stripe index.
     * @return Worker.
     */
    protected abstract T createWorker(int idx);
}
