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

package com.hazelcast.sql.impl.state;

import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.impl.AbstractSqlResult;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.ResultIterator;

import java.util.UUID;

public class QueryClientState {

    private final UUID clientId;
    private final QueryId queryId;
    private final AbstractSqlResult sqlResult;
    private final boolean closed;
    private final long createdAt;

    private ResultIterator<SqlRow> iterator;

    public QueryClientState(UUID clientId, QueryId queryId, AbstractSqlResult sqlResult, boolean closed) {
        this.clientId = clientId;
        this.queryId = queryId;
        this.sqlResult = sqlResult;
        this.closed = closed;

        createdAt = System.nanoTime();
    }

    public UUID getClientId() {
        return clientId;
    }

    public QueryId getQueryId() {
        return queryId;
    }

    public AbstractSqlResult getSqlResult() {
        return sqlResult;
    }

    public boolean isClosed() {
        return closed;
    }

    public long getCreatedAtNano() {
        return createdAt;
    }

    public ResultIterator<SqlRow> getIterator() {
        assert sqlResult != null;

        if (iterator == null) {
            iterator = sqlResult.iterator();
        }

        return iterator;
    }
}
