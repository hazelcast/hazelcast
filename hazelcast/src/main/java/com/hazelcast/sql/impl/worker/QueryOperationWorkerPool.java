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
import com.hazelcast.sql.impl.operation.QueryOperation;
import com.hazelcast.sql.impl.operation.QueryOperationHandler;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Thread pool that executes query operations.
 */
public class QueryOperationWorkerPool {

    private final int threadCount;
    private final QueryOperationWorker[] workers;

    public QueryOperationWorkerPool(
        String instanceName,
        int threadCount,
        LocalMemberIdProvider localMemberIdProvider,
        QueryOperationHandler operationHandler,
        SerializationService serializationService,
        ILogger logger
    ) {
        this.threadCount = threadCount;

        workers = new QueryOperationWorker[threadCount];

        for (int i = 0; i < threadCount; i++) {
            QueryOperationWorker worker =
                new QueryOperationWorker(localMemberIdProvider, operationHandler, serializationService, instanceName, i, logger);

            workers[i] = worker;
        }
    }

    public void submit(int partition, QueryOperationExecutable task) {
        int index = getWorkerIndex(partition);

        QueryOperationWorker worker = getWorker(index);

        worker.submit(task);
    }

    public void stop() {
        for (QueryOperationWorker worker : workers) {
            worker.stop();
        }
    }

    QueryOperationWorker getWorker(int index) {
        return workers[index];
    }

    private int getWorkerIndex(int partition) {
        int index;

        if (partition == QueryOperation.PARTITION_ANY) {
            index = ThreadLocalRandom.current().nextInt(threadCount);
        } else {
            index = partition % threadCount;
        }

        assert index >= 0 && index < threadCount;

        return index;
    }
}
