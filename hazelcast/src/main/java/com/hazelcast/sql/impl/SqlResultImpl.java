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
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlRowMetadata;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.state.QueryInitiatorState;
import com.hazelcast.sql.impl.state.QueryState;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Cursor implementation.
 */
public class SqlResultImpl implements SqlResult {

    private final QueryState state;
    private final SqlRowMetadata rowMetadata;
    private Iterator<SqlRow> iterator;

    public SqlResultImpl(QueryState state) {
        this.state = state;

        rowMetadata = state.getInitiatorState().getRowMetadata();
    }

    @Nonnull
    @Override
    public SqlRowMetadata getRowMetadata() {
        return rowMetadata;
    }

    @Nonnull
    @Override
    public Iterator<SqlRow> iterator() {
        if (iterator == null) {
            Iterator<SqlRow> iterator0 = new RowToSqlRowIterator(getQueryInitiatorState().getResultProducer().iterator());

            iterator = iterator0;

            return iterator0;
        } else {
            throw new IllegalStateException("Iterator can be requested only once.");
        }
    }

    @Override
    public void close() {
        closeOnError(QueryException.cancelledByUser());
    }

    public void closeOnError(QueryException error) {
        state.cancel(error);
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
