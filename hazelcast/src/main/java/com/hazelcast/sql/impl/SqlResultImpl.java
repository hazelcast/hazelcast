/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.util.Preconditions;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlRowMetadata;
import com.hazelcast.sql.impl.plan.Plan;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.state.QueryInitiatorState;
import com.hazelcast.sql.impl.state.QueryState;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Cursor implementation.
 */
public final class SqlResultImpl extends AbstractSqlResult {

    private final QueryState state;
    private final SqlRowMetadata rowMetadata;
    private ResultIterator<SqlRow> iterator;
    private final long updateCount;
    private final InternalSerializationService serializationService;

    private SqlResultImpl(QueryState state, long updateCount, InternalSerializationService serializationService) {
        assert updateCount >= 0 ^ state != null : "updateCount=" + updateCount + ", state=" + state;

        this.state = state;
        this.updateCount = updateCount;
        this.serializationService = serializationService;

        rowMetadata = state != null ? state.getInitiatorState().getRowMetadata() : null;
    }

    public static SqlResultImpl createRowsResult(QueryState state, InternalSerializationService serializationService) {
        return new SqlResultImpl(state, -1, serializationService);
    }

    public static SqlResultImpl createUpdateCountResult(long updateCount) {
        Preconditions.checkNotNegative(updateCount, "the updateCount must be >= 0");
        return new SqlResultImpl(null, updateCount, null);
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
        return updateCount;
    }

    private void checkIsRowsResult() {
        if (updateCount >= 0) {
            throw new IllegalStateException("This result contains only update count");
        }
    }

    @Override
    public void close(@Nullable QueryException error) {
        if (state != null) {
            state.cancel(error, false);
        }
    }

    @Override
    public boolean onParticipantGracefulShutdown(UUID memberId) {
        // not implemented for IMDG engine
        return false;
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

    @Override
    public Object deserialize(Object value) {
        try {
            return serializationService.toObject(value);
        } catch (Exception e) {
            throw QueryUtils.toPublicException(e, state.getLocalMemberId());
        }
    }

    @Override
    public Object deserialize(LazyTarget value) {
        try {
            return value.deserialize(serializationService);
        } catch (Exception e) {
            throw QueryUtils.toPublicException(e, state.getLocalMemberId());
        }
    }

    @Override
    public boolean isInfiniteRows() {
        return false;
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
        public HasNextResult hasNext(long timeout, TimeUnit timeUnit) {
            try {
                return delegate.hasNext(timeout, timeUnit);
            } catch (Exception e) {
                throw QueryUtils.toPublicException(e, state.getLocalMemberId());
            }
        }

        @Override
        public SqlRow next() {
            try {
                return new SqlRowImpl(rowMetadata, delegate.next(), SqlResultImpl.this);
            } catch (NoSuchElementException e) {
                throw e;
            } catch (Exception e) {
                throw QueryUtils.toPublicException(e, state.getLocalMemberId());
            }
        }
    }
}
