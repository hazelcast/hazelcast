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

package com.hazelcast.sql.impl.worker.control;

import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.SqlServiceImpl;
import com.hazelcast.sql.impl.worker.AbstractThreadPool;
import com.hazelcast.sql.impl.worker.data.DataThreadPool;

/**
 * Control thread pool. Responsible for query initiation and cancel as well as for reactions on asynchronous
 * events which may affect query execution: migration, member leave.
 */
public class ControlThreadPool extends AbstractThreadPool<ControlWorker> {
    /** Prefix for thread names in this pool. */
    private static final String THREAD_PREFIX = "query-control-";

    /** Query service. */
    private final SqlServiceImpl service;

    /** Data pool. */
    private final DataThreadPool dataPool;

    public ControlThreadPool(SqlServiceImpl service, NodeEngine nodeEngine, int threadCnt,
        DataThreadPool dataPool) {
        super(nodeEngine, THREAD_PREFIX, threadCnt);

        this.service = service;
        this.dataPool = dataPool;
    }

    @Override
    protected ControlWorker createWorker(NodeEngine nodeEngine, int idx) {
        return new ControlWorker(service, nodeEngine, dataPool);
    }

    /**
     * Submit a task.
     *
     * @param task Task.
     */
    public void submit(ControlTask task) {
        QueryId queryId = task.getQueryId();

        if (queryId == null) {
            // Tasks without a query ID should be broadcasted to all stripes.
            for (int i = 0; i < workers.length; i++)
                getWorker(i).offer(task);
        }
        else {
            int stripe = Math.abs(queryId.hashCode() % getThreadCount());

            getWorker(stripe).offer(task);
        }
    }
}
