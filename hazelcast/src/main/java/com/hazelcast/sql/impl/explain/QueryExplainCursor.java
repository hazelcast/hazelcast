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
import com.hazelcast.sql.SqlCursor;
import com.hazelcast.sql.SqlRow;

import java.util.Iterator;
import java.util.List;

/**
 * Cursor for explain operation.
 */
public class QueryExplainCursor implements SqlCursor {

    private static final String COLUMN_NAME = "ITEM";

    private static final SqlColumnMetadata COLUMN_METADATA = new SqlColumnMetadata(COLUMN_NAME, SqlColumnType.VARCHAR);
    private final List<SqlRow> rows;

    public QueryExplainCursor(List<SqlRow> rows) {
        this.rows = rows;
    }

    @Override
    public int getColumnCount() {
        return 1;
    }

    @Override
    public SqlColumnMetadata getColumnMetadata(int index) {
        if (index == 0) {
            return COLUMN_METADATA;
        }

        throw new IllegalArgumentException("Index out of bounds: " + index);
    }

    @Override
    public void close() throws Exception {
        // No-op.
    }

    @SuppressWarnings("NullableProblems")
    @Override
    public Iterator<SqlRow> iterator() {
        return rows.iterator();
    }
}
