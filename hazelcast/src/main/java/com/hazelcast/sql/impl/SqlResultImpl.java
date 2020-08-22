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

import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlRowMetadata;
import com.hazelcast.sql.impl.plan.Plan;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.state.QueryInitiatorState;
import com.hazelcast.sql.impl.state.QueryState;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.NoSuchElementException;

/**
 * Cursor implementation.
 */
public final class SqlResultImpl extends AbstractSqlResult {

    private final boolean isUpdateCount;
    private final QueryState state;
    private final SqlRowMetadata rowMetadata;
    private ResultIterator<SqlRow> iterator;
    private final long updatedCount;

    private SqlResultImpl(boolean isUpdateCount, QueryState state, long updatedCount) {
        this.isUpdateCount = isUpdateCount;
        this.state = state;
        this.updatedCount = updatedCount;
        assert isUpdateCount ^ state != null : "isUpdateCount" + isUpdateCount + ", state=" + state;

        rowMetadata = state != null ? state.getInitiatorState().getRowMetadata() : null;
    }

    public static SqlResultImpl createRowsResult(QueryState state) {
        return new SqlResultImpl(false, state, 0);
    }

    public static SqlResultImpl createUpdateCountResult(long updatedCount) {
        return new SqlResultImpl(true, null, updatedCount);
    }

    @Nonnull
    @Override
    public SqlRowMetadata getRowMetadata() {
        checkIsRowsResult();
        return rowMetadata;
    }

    @Nonnull
    @Override
    public ResultIterator<SqlRow> iterator() {
        checkIsRowsResult();

        if (iterator == null) {
            iterator = new RowToSqlRowIterator(getQueryInitiatorState().getResultProducer().iterator());

            return iterator;
        } else {
            throw new IllegalStateException("Iterator can be requested only once.");
        }
    }

    @Override
    public long updateCount() {
        if (!isUpdateCount) {
            throw new IllegalStateException("This result doesn't contain update count");
        }
        return updatedCount;
    }

    @Override
    public boolean isUpdateCount() {
        return isUpdateCount;
    }

    private void checkIsRowsResult() {
        if (isUpdateCount) {
            throw new IllegalStateException("This result contains only update count");
        }
    }

    public void closeOnError(QueryException error) {
        if (state != null) {
            state.cancel(error);
        }
    }

    /**
     * Return the query ID.
     *
     * @throws IllegalStateException If the result doesn't contain rows and
     *     therefore the query is complete when the result is returned to the
     *     user.
     */
    @Nullable
    public QueryId getQueryId() {
        checkIsRowsResult();
        return getQueryInitiatorState().getQueryId();
    }

    public Plan getPlan() {
        QueryInitiatorState initiatorState = getQueryInitiatorState();

        return initiatorState != null ? initiatorState.getPlan() : null;
    }

    private QueryInitiatorState getQueryInitiatorState() {
        return state.getInitiatorState();
    }

    private final class RowToSqlRowIterator implements ResultIterator<SqlRow> {

        private final ResultIterator<Row> delegate;

        private RowToSqlRowIterator(ResultIterator<Row> delegate) {
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
        public HasNextImmediatelyResult hasNextImmediately() {
            try {
                return delegate.hasNextImmediately();
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
