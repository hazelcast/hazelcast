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

import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.concurrent.MPSCQueue;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.operation.QueryOperationHandler;
import com.hazelcast.sql.impl.QueryUtils;
import com.hazelcast.sql.impl.operation.QueryAbstractIdAwareOperation;
import com.hazelcast.sql.impl.operation.QueryCancelOperation;
import com.hazelcast.sql.impl.operation.QueryOperation;

import java.util.UUID;
import java.util.concurrent.BlockingQueue;

import static com.hazelcast.sql.impl.QueryUtils.WORKER_TYPE_OPERATION;

/**
 * Worker responsible for operation processing.
 */
public class QueryOperationWorker implements Runnable {

    private static final Object POISON = new Object();

    private final QueryOperationHandler operationHandler;
    private final SerializationService ss;
    private final Thread thread;
    private final BlockingQueue<Object> queue;

    public QueryOperationWorker(QueryOperationHandler operationHandler, SerializationService ss, String instanceName, int index) {
        this.operationHandler = operationHandler;
        this.ss = ss;

        thread = new Thread(this,  QueryUtils.workerName(instanceName, WORKER_TYPE_OPERATION, index));
        queue = new MPSCQueue<>(thread, null);

        thread.start();
    }

    public void submit(QueryOperationExecutable task) {
        queue.add(task);
    }

    public void stop() {
        queue.clear();
        queue.add(POISON);

        thread.interrupt();
    }

    @Override
    public void run() {
        try {
            while (true) {
                Object task = queue.take();

                if (task == POISON) {
                    break;
                } else {
                    assert task instanceof QueryOperationExecutable;

                    execute((QueryOperationExecutable) task);
                }
            }
        } catch (InterruptedException e) {
            // No-op.
        }
    }

    private void execute(QueryOperationExecutable task) {
        QueryOperation operation = task.getLocalOperation();

        if (operation == null) {
            operation = deserialize(task.getRemoteOperation());

            if (operation == null) {
                System.out.println(">>> SKIPPED: " + Thread.currentThread().getName() + " " + task);

                return;
            }
        }

        operationHandler.execute(operation);
    }

    /**
     * Deserializes the packet into operation. If an exception happens, the query is cancelled.
     *
     * @param packet Packet packet.
     * @return Query operation.
     */
    private QueryOperation deserialize(Packet packet) {
        try {
            return ss.toObject(packet);
        } catch (Exception e) {
            // We assume that only ID aware operations may hold user data. Other operations contain only HZ classes.
            QueryAbstractIdAwareOperation operation = QueryAbstractIdAwareOperation.getErrorOperation();

            if (operation != null) {
                sendDeserializationError(operation, e);
            }

            return null;
        }
    }

    private void sendDeserializationError(QueryAbstractIdAwareOperation operation, Exception e) {
        QueryId queryId = operation.getQueryId();
        UUID callerId = operation.getCallerId();

        HazelcastSqlException error = HazelcastSqlException.error("Failed to deserialize "
            + operation.getClass().getSimpleName() + " received from " + callerId + ": " + e.getMessage(), e);

        QueryCancelOperation cancelOperation = new QueryCancelOperation(
            queryId,
            error.getCode(),
            error.getMessage(),
            null
        );

        try {
            operationHandler.submit(queryId.getMemberId(), cancelOperation);
        } catch (Exception ignore) {
            // This should never happen, since we do not transmit user objects.
        }
    }
}
