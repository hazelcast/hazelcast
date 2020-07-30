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
import com.hazelcast.sql.SqlColumnMetadata;
import com.hazelcast.sql.impl.QueryId;

import java.util.List;

public final class SqlExecuteResponse {

    private final boolean isUpdateCount;
    private final QueryId queryId;
    private final List<SqlColumnMetadata> rowMetadata;
    private final List<List<Data>> rowPage;
    private final boolean rowPageLast;
    private final SqlError error;
    private final long updatedCount;

    private SqlExecuteResponse(
        boolean isUpdateCount,
        QueryId queryId,
        List<SqlColumnMetadata> rowMetadata,
        List<List<Data>> rowPage,
        boolean rowPageLast,
        long updatedCount,
        SqlError error
    ) {
        this.isUpdateCount = isUpdateCount;
        this.queryId = queryId;
        this.rowMetadata = rowMetadata;
        this.rowPage = rowPage;
        this.rowPageLast = rowPageLast;
        this.updatedCount = updatedCount;
        this.error = error;
    }

    public static SqlExecuteResponse rowsResponse(
        QueryId queryId,
        List<SqlColumnMetadata> rowMetadata,
        List<List<Data>> rowPage,
        boolean rowPageLast
    ) {
        return new SqlExecuteResponse(false, queryId, rowMetadata, rowPage, rowPageLast, 0, null);
    }

    public static SqlExecuteResponse errorResponse(SqlError error) {
        return new SqlExecuteResponse(false, null, null, null, true, 0, error);
    }

    // used by Jet
    @SuppressWarnings("unused")
    public static SqlExecuteResponse updateCountResponse(long updatedCount) {
        return new SqlExecuteResponse(true, null, null, null, true, updatedCount, null);
    }

    public boolean isUpdateCount() {
        return isUpdateCount;
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

    public long getUpdatedCount() {
        return updatedCount;
    }
}
