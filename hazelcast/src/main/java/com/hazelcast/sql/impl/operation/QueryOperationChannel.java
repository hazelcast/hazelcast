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

/**
 * A channel for sequential execution of multiple operations.
 */
public class QueryOperationChannel {

    private final QueryOperationHandler operationHandler;
    private final Connection connection;

    public QueryOperationChannel(QueryOperationHandler operationHandler, Connection connection) {
        this.operationHandler = operationHandler;
        this.connection = connection;
    }

    public boolean execute(QueryOperation operation) {
        if (connection != null) {
            return operationHandler.executeRemote(connection, operation, true);
        } else {
            operationHandler.executeLocal(operation);

            return true;
        }
    }
}
