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

package com.hazelcast.sql.impl.client;

import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.SqlCursorImpl;

import java.util.Iterator;
import java.util.UUID;

public class SqlClientState {
    /** Client ID. */
    private final UUID clientId;

    /** Query ID. */
    private final QueryId queryId;

    /** Cursor. */
    private final SqlCursorImpl cursor;

    /** Iterator. */
    private Iterator<SqlRow> iterator;

    public SqlClientState(UUID clientId, QueryId queryId, SqlCursorImpl cursor) {
        this.clientId = clientId;
        this.queryId = queryId;
        this.cursor = cursor;
    }

    public UUID getClientId() {
        return clientId;
    }

    public QueryId getQueryId() {
        return queryId;
    }

    public SqlCursorImpl getCursor() {
        return cursor;
    }

    public Iterator<SqlRow> getIterator() {
        if (iterator == null) {
            iterator = cursor.iterator();
        }

        return iterator;
    }
}
