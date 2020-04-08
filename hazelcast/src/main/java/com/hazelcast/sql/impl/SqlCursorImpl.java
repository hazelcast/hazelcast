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

import com.hazelcast.sql.SqlColumnMetadata;
import com.hazelcast.sql.SqlCursor;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.impl.plan.Plan;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.state.QueryInitiatorState;
import com.hazelcast.sql.impl.state.QueryState;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.NoSuchElementException;

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
            throw QueryException.error("Iteartor can be requested only once.");
        }
    }

    @Override
    public void close() {
        closeOnError(QueryException.cancelledByUser());
    }

    public void closeOnError(QueryException error) {
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

    private final class RowToSqlRowIterator implements Iterator<SqlRow> {

        private final Iterator<Row> delegate;

        private RowToSqlRowIterator(Iterator<Row> delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean hasNext() {
            try {
                return delegate.hasNext();
            } catch (Exception e) {
                throw QueryUtils.toPublicException(e, state.getLocalMemberId());
            }
        }

        @Override
        public SqlRow next() {
            try {
                return new SqlRowImpl(delegate.next());
            } catch (NoSuchElementException e) {
                throw e;
            } catch (Exception e) {
                throw QueryUtils.toPublicException(e, state.getLocalMemberId());
            }
        }
    }
}
