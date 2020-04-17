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

package com.hazelcast.sql.impl.operation;

import com.hazelcast.internal.nio.Connection;

import java.util.UUID;

/**
 * Default operation channel implementation.
 */
public class QueryOperationChannelImpl implements QueryOperationChannel {
    private final QueryOperationHandlerImpl operationHandler;
    private final UUID localMemberId;
    private final Connection connection;

    public QueryOperationChannelImpl(QueryOperationHandlerImpl operationHandler, UUID localMemberId, Connection connection) {
        this.operationHandler = operationHandler;
        this.localMemberId = localMemberId;
        this.connection = connection;
    }

    @Override
    public boolean submit(QueryOperation operation) {
        if (connection != null) {
            return operationHandler.submitRemote(localMemberId, connection, operation, true);
        } else {
            operationHandler.submitLocal(localMemberId, operation);

            return true;
        }
    }
}
