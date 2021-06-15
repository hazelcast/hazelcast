/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.logging.ILogger;
import com.hazelcast.sql.impl.LocalMemberIdProvider;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.operation.coordinator.QueryCancelOperation;
import com.hazelcast.sql.impl.operation.coordinator.QueryOperation;
import com.hazelcast.sql.impl.operation.coordinator.QueryOperationDeserializationException;
import com.hazelcast.sql.impl.operation.coordinator.QueryOperationHandler;

import java.util.UUID;

import static com.hazelcast.instance.impl.OutOfMemoryErrorDispatcher.inspectOutOfMemoryError;

public class QueryPoolTask implements Runnable {

    private final QueryOperationExecutable task;
    private final LocalMemberIdProvider localMemberIdProvider;
    private final QueryOperationHandler operationHandler;
    private final SerializationService ss;
    private final ILogger logger;

    public QueryPoolTask(
        QueryOperationExecutable task,
        LocalMemberIdProvider localMemberIdProvider,
        QueryOperationHandler operationHandler,
        SerializationService ss,
        ILogger logger
    ) {
        this.task = task;
        this.localMemberIdProvider = localMemberIdProvider;
        this.operationHandler = operationHandler;
        this.ss = ss;
        this.logger = logger;
    }

    @Override
    public void run() {
        try {
            execute(task);
        } catch (Throwable t) {
            inspectOutOfMemoryError(t);
            logger.severe(t);
        }
    }

    private void execute(QueryOperationExecutable task) {
        QueryOperation operation;

        if (task.isLocal()) {
            operation = task.getLocalOperation();
        } else {
            operation = deserialize(task.getRemoteOperation());

            if (operation == null) {
                return;
            }
        }

        assert operation != null;

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
            if (e.getCause() instanceof QueryOperationDeserializationException) {
                QueryOperationDeserializationException error = (QueryOperationDeserializationException) e.getCause();

                // We assume that only ID aware operations may hold user data. Other operations contain only HZ classes and
                // we should never see deserialization errors for them.
                sendDeserializationError(error);
            } else {
                // It is not easy to decide how to handle an arbitrary exception. We do not have caller coordinates, so
                // we do not know how to notify it. We also cannot panic (i.e. kill local member), because it would be a
                // security threat. So the only sensible solution is to log the error.
                logger.severe("Failed to deserialize query operation received from " + packet.getConn().getRemoteAddress()
                    + " (will be ignored)", e);
            }
        }

        return null;
    }

    private void sendDeserializationError(QueryOperationDeserializationException e) {
        QueryId queryId = e.getQueryId();

        UUID localMemberId = localMemberIdProvider.getLocalMemberId();
        UUID targetMemberId = e.getCallerId();
        UUID initiatorMemberId = queryId.getMemberId();

        QueryCancelOperation cancelOperation = new QueryCancelOperation(queryId, SqlErrorCode.GENERIC,
                "Failed to deserialize " + e.getOperationClassName() + " received from " + targetMemberId + ": " + e.getMessage(),
                localMemberId);

        operationHandler.submit(localMemberId, initiatorMemberId, cancelOperation);
    }
}
