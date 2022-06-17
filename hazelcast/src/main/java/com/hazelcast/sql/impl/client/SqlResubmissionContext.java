/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.client;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.sql.SqlStatement;

class SqlResubmissionContext {
    private final ClientMessage sqlExecuteMessage;
    private final boolean selectQuery;

    SqlResubmissionContext(ClientMessage sqlExecuteMessage, SqlStatement statement) {
        this.sqlExecuteMessage = sqlExecuteMessage;
        this.selectQuery = isSelectQuery(statement);
    }

    private boolean isSelectQuery(SqlStatement sqlStatement) {
        return sqlStatement.getSql().trim().toLowerCase().startsWith("select");
    }

    ClientMessage getSqlExecuteMessage() {
        return sqlExecuteMessage;
    }

    boolean isSelectQuery() {
        return selectQuery;
    }
}
