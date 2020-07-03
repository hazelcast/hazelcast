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

package com.hazelcast.sql.jdbc.impl;

import com.hazelcast.sql.SqlQuery;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.impl.client.SqlClientResult;

import java.util.Iterator;

/**
 * JDBC cursor.
 */
public class JdbcCursor {
    /** Cursor. */
    private final SqlClientResult cursor;

    /** Iterator. */
    private final Iterator<SqlRow> iterator;

    public JdbcCursor(SqlClientResult cursor, Iterator<SqlRow> iterator) {
        this.cursor = cursor;
        this.iterator = iterator;
    }

    public int getPageSize() {
        return cursor.getCursorBufferSize();
    }

    public void setPageSize(int pageSize) {
        assert pageSize >= 0;

        cursor.setCursorBufferSize(pageSize == 0 ? SqlQuery.DEFAULT_CURSOR_BUFFER_SIZE : pageSize);
    }

    public boolean hasNextRow() {
        return iterator.hasNext();
    }

    public SqlRow getNextRow() {
        if (!hasNextRow()) {
            return null;
        }

        return iterator.next();
    }

    public int getColumnCount() {
        return cursor.getRowMetadata().getColumnCount();
    }

    public void close() {
        try {
            cursor.close();
        } catch (Exception e) {
            // TODO: Handle.
        }
    }
}
