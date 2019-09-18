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

package com.hazelcast.sql.impl.worker.data;

import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.worker.AbstractThreadPool;

/**
 * Thread pool responsible for actual data processing.
 */
public class DataThreadPool extends AbstractThreadPool<DataWorker> {
    /** Prefix for child threads. */
    private static final String THREAD_PREFIX = "query-data-";

    /** Broadcast stripe marker. */
    private static final int BROADCAST_STRIPE = -1;

    public DataThreadPool(NodeEngine nodeEngine, int threadCnt) {
        super(nodeEngine, THREAD_PREFIX, threadCnt);
    }

    @Override
    protected DataWorker createWorker(NodeEngine nodeEngine, int idx) {
        return new DataWorker(nodeEngine, idx);
    }

    /**
     * Submit a task.
     *
     * @param task Task.
     */
    public void submit(DataTask task) {
        int stripe = resolveStripe(task);

        if (stripe == BROADCAST_STRIPE) {
            for (int i = 0; i < workers.length; i++)
                getWorker(i).offer(task);
        }
        else
            getWorker(stripe).offer(task);
    }

    /**
     * Resolve a stripe for the task.
     *
     * @param task Task.
     * @return Stripe.
     */
    private int resolveStripe(DataTask task) {
        int thread = task.getThread();

        if (thread == DataWorker.UNMAPPED_STRIPE)
            return BROADCAST_STRIPE;
        else
            return thread % getThreadCount();
    }

    /**
     * @return Number of stripes in the pool.
     */
    public int getStripeCount() {
        return workers.length;
    }
}