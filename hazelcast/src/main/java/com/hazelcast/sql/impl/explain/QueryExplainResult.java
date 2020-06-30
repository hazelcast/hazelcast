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

package com.hazelcast.sql.impl.explain;

import com.hazelcast.sql.SqlColumnMetadata;
import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlRowMetadata;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Cursor for explain operation.
 */
public class QueryExplainResult implements SqlResult {

    private static final String COLUMN_NAME = "ITEM";

    private static final SqlColumnMetadata COLUMN_METADATA = new SqlColumnMetadata(COLUMN_NAME, SqlColumnType.VARCHAR);
    static final SqlRowMetadata ROW_METADATA = new SqlRowMetadata(Collections.singletonList(COLUMN_METADATA));

    private final List<SqlRow> rows;

    public QueryExplainResult(List<SqlRow> rows) {
        this.rows = rows;
    }

    @Nonnull
    @Override
    public SqlRowMetadata getRowMetadata() {
        return ROW_METADATA;
    }

    @Override
    public void close() {
        // No-op.
    }

    @Override
    @Nonnull
    public Iterator<SqlRow> iterator() {
        return rows.iterator();
    }
}
