/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl;

import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlRowMetadata;
import com.hazelcast.sql.impl.AbstractSqlResult;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.QueryResultProducer;
import com.hazelcast.sql.impl.QueryUtils;
import com.hazelcast.sql.impl.ResultIterator;
import com.hazelcast.sql.impl.SqlRowImpl;
import com.hazelcast.sql.impl.row.JetSqlRow;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

class SqlResultImpl extends AbstractSqlResult {

    private final QueryId queryId;
    private final QueryResultProducer rootResultConsumer;
    private final SqlRowMetadata rowMetadata;
    private final boolean isInfiniteRows;

    private ResultIterator<SqlRow> iterator;

    SqlResultImpl(
            QueryId queryId,
            QueryResultProducer rootResultConsumer,
            SqlRowMetadata rowMetadata,
            boolean isInfiniteRows
    ) {
        this.queryId = queryId;
        this.rootResultConsumer = rootResultConsumer;
        this.rowMetadata = rowMetadata;
        this.isInfiniteRows = isInfiniteRows;
    }

    @Override
    public QueryId getQueryId() {
        return queryId;
    }

    @Override
    public boolean isInfiniteRows() {
        return isInfiniteRows;
    }

    @Nonnull @Override
    public SqlRowMetadata getRowMetadata() {
        return rowMetadata;
    }

    @Nonnull @Override
    public ResultIterator<SqlRow> iterator() {
        if (iterator == null) {
            iterator = new RowToSqlRowIterator(rootResultConsumer.iterator());

            return iterator;
        } else {
            throw new IllegalStateException("Iterator can be requested only once.");
        }
    }

    @Override
    public long updateCount() {
        return -1;
    }

    @Override
    public void close(@Nullable QueryException exception) {
        if (exception == null) {
            exception = QueryException.cancelledByUser();
        }
        rootResultConsumer.onError(exception);
    }

    private final class RowToSqlRowIterator implements ResultIterator<SqlRow> {

        private final ResultIterator<JetSqlRow> delegate;

        private RowToSqlRowIterator(ResultIterator<JetSqlRow> delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean hasNext() {
            try {
                return delegate.hasNext();
            } catch (Exception e) {
                throw QueryUtils.toPublicException(e, queryId.getMemberId());
            }
        }

        @Override
        public HasNextResult hasNext(long timeout, TimeUnit timeUnit) {
            try {
                return delegate.hasNext(timeout, timeUnit);
            } catch (Exception e) {
                throw QueryUtils.toPublicException(e, queryId.getMemberId());
            }
        }

        @Override
        public SqlRow next() {
            try {
                return new SqlRowImpl(getRowMetadata(), delegate.next());
            } catch (NoSuchElementException e) {
                throw e;
            } catch (Exception e) {
                throw QueryUtils.toPublicException(e, queryId.getMemberId());
            }
        }
    }
}
