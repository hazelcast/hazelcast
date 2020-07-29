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

import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlResultType;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlRowMetadata;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.state.QueryInitiatorState;
import com.hazelcast.sql.impl.state.QueryState;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Cursor implementation.
 */
public class SqlResultImpl implements SqlResult {

    private final SqlResultType resultType;
    private final QueryState state;
    private final SqlRowMetadata rowMetadata;
    private Iterator<SqlRow> iterator;

    public SqlResultImpl(SqlResultType resultType, QueryState state) {
        this.resultType = resultType;
        this.state = state;
        assert resultType == SqlResultType.ROWS ^ state == null : "resultType=" + resultType + ", state=" + state;

        rowMetadata = state != null ? state.getInitiatorState().getRowMetadata() : null;
    }

    @Nonnull
    @Override
    public SqlRowMetadata getRowMetadata() {
        checkIsRowsResult();
        return rowMetadata;
    }

    @Nonnull
    @Override
    public Iterator<SqlRow> iterator() {
        checkIsRowsResult();

        if (iterator == null) {
            Iterator<SqlRow> iterator0 = new RowToSqlRowIterator(getQueryInitiatorState().getResultProducer().iterator());

            iterator = iterator0;

            return iterator0;
        } else {
            throw new IllegalStateException("Iterator can be requested only once.");
        }
    }

    @Nonnull
    @Override
    public SqlResultType getResultType() {
        return resultType;
    }

    @Override
    public void close() {
        closeOnError(QueryException.cancelledByUser());
    }

    private void checkIsRowsResult() {
        if (resultType != SqlResultType.ROWS) {
            throw new IllegalStateException("Not a " + SqlResultType.ROWS.name() + " result");
        }
    }

    public void closeOnError(QueryException error) {
        if (state != null) {
            state.cancel(error);
        }
    }

    /**
     * Return the query ID or null if it's not a result with ROWS.
     */
    @Nullable
    public QueryId getQueryId() {
        if (resultType != SqlResultType.ROWS) {
            return null;
        }
        return getQueryInitiatorState().getQueryId();
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
                return new SqlRowImpl(rowMetadata, delegate.next());
            } catch (NoSuchElementException e) {
                throw e;
            } catch (Exception e) {
                throw QueryUtils.toPublicException(e, state.getLocalMemberId());
            }
        }
    }
}
