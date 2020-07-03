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

import com.hazelcast.sql.SqlRowMetadata;
import com.hazelcast.sql.impl.QueryId;

public class SqlExecuteResponse {

    private final QueryId queryId;
    private final SqlRowMetadata rowMetadata;
    private final SqlPage page;
    private final SqlError error;

    public SqlExecuteResponse(QueryId queryId, SqlRowMetadata rowMetadata, SqlPage page, SqlError error) {
        this.queryId = queryId;
        this.rowMetadata = rowMetadata;
        this.page = page;
        this.error = error;
    }

    public QueryId getQueryId() {
        return queryId;
    }

    public SqlRowMetadata getRowMetadata() {
        return rowMetadata;
    }

    public SqlPage getPage() {
        return page;
    }

    public SqlError getError() {
        return error;
    }
}
