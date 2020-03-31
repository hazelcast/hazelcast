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

package com.hazelcast.sql.impl;

import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlColumnMetadata;
import com.hazelcast.sql.SqlCursor;
import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.impl.plan.Plan;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.state.QueryInitiatorState;
import com.hazelcast.sql.impl.state.QueryState;

import javax.annotation.Nonnull;
import java.util.Iterator;

/**
 * Cursor implementation.
 */
public class SqlCursorImpl implements SqlCursor {

    private final QueryState state;
    private final QueryMetadata metadata;
    private Iterator<SqlRow> iterator;

    public SqlCursorImpl(QueryState state) {
        this.state = state;

        metadata = state.getInitiatorState().getMetadata();
    }

    @Override
    public int getColumnCount() {
        return metadata.getColumnCount();
    }

    @Override
    public SqlColumnMetadata getColumnMetadata(int index) {
        int columnCount = metadata.getColumnCount();

        if (index < 0 || index >= columnCount) {
            throw new IllegalArgumentException("Column index is out of range: " + index);
        }

        return QueryUtils.getColumnMetadata(metadata.getColumnType(index));
    }

    @Override @Nonnull
    public Iterator<SqlRow> iterator() {
        if (iterator == null) {
            Iterator<SqlRow> iterator0 = new RowToSqlRowIterator(getQueryInitiatorState().getResultProducer().iterator());

            iterator = iterator0;

            return iterator0;
        } else {
            throw HazelcastSqlException.error("Iteartor can be requested only once.");
        }
    }

    @Override
    public void close() {
        closeOnError(HazelcastSqlException.error(SqlErrorCode.CANCELLED, "Query was cancelled by user."));
    }

    public void closeOnError(HazelcastSqlException error) {
        state.cancel(error);
    }

    public QueryId getQueryId() {
        return getQueryInitiatorState().getQueryId();
    }

    public Plan getPlan() {
        return getQueryInitiatorState().getPlan();
    }

    private QueryInitiatorState getQueryInitiatorState() {
        return state.getInitiatorState();
    }

    private static final class RowToSqlRowIterator implements Iterator<SqlRow> {

        private final Iterator<Row> delegate;

        private RowToSqlRowIterator(Iterator<Row> delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public SqlRow next() {
            return new SqlRowImpl(delegate.next());
        }
    }
}
