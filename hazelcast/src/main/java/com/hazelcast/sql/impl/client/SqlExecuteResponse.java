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

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.Preconditions;
import com.hazelcast.sql.SqlColumnMetadata;
import com.hazelcast.sql.impl.QueryId;

import java.util.List;

public final class SqlExecuteResponse {

    private final QueryId queryId;
    private final List<SqlColumnMetadata> rowMetadata;
    private final List<List<Data>> rowPage;
    private final boolean rowPageLast;
    private final SqlError error;
    private final long updateCount;

    private SqlExecuteResponse(
        QueryId queryId,
        List<SqlColumnMetadata> rowMetadata,
        List<List<Data>> rowPage,
        boolean rowPageLast,
        long updateCount,
        SqlError error
    ) {
        this.queryId = queryId;
        this.rowMetadata = rowMetadata;
        this.rowPage = rowPage;
        this.rowPageLast = rowPageLast;
        this.updateCount = updateCount;
        this.error = error;
    }

    public static SqlExecuteResponse rowsResponse(
        QueryId queryId,
        List<SqlColumnMetadata> rowMetadata,
        List<List<Data>> rowPage,
        boolean rowPageLast
    ) {
        return new SqlExecuteResponse(queryId, rowMetadata, rowPage, rowPageLast, -1, null);
    }

    public static SqlExecuteResponse errorResponse(SqlError error) {
        return new SqlExecuteResponse(null, null, null, true, -1, error);
    }

    // used by Jet
    @SuppressWarnings("unused")
    public static SqlExecuteResponse updateCountResponse(long updateCount) {
        Preconditions.checkNotNegative(updateCount, "the updateCount must be >= 0");
        return new SqlExecuteResponse(null, null, null, true, updateCount, null);
    }

    public QueryId getQueryId() {
        return queryId;
    }

    public List<SqlColumnMetadata> getRowMetadata() {
        return rowMetadata;
    }

    public List<List<Data>> getRowPage() {
        return rowPage;
    }

    public boolean isRowPageLast() {
        return rowPageLast;
    }

    public SqlError getError() {
        return error;
    }

    public long getUpdateCount() {
        return updateCount;
    }
}
