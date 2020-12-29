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

package com.hazelcast.sql.impl.client;

import com.hazelcast.internal.nio.Connection;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlRowMetadata;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.SqlRowImpl;
import com.hazelcast.sql.impl.row.HeapRow;
import com.hazelcast.sql.impl.row.Row;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Client-side cursor.
 */
public class SqlClientResult implements SqlResult {

    private final SqlClientService service;
    private final Connection connection;
    private final QueryId queryId;
    private final SqlRowMetadata rowMetadata;
    private final ClientIterator iterator;
    private final int cursorBufferSize;
    private final long updateCount;

    private boolean closed;
    private boolean iteratorAccessed;

    public SqlClientResult(
            SqlClientService service,
            Connection connection,
            QueryId queryId,
            SqlRowMetadata rowMetadata,
            List<List<Object>> columns,
            boolean rowPageLast,
            int cursorBufferSize,
            long updateCount
    ) {
        this.service = service;
        this.connection = connection;
        this.queryId = queryId;
        this.rowMetadata = rowMetadata;
        this.cursorBufferSize = cursorBufferSize;
        this.updateCount = updateCount;

        if (updateCount >= 0) {
            iterator = null;
        } else {
            assert updateCount == -1;
            assert rowMetadata != null;
            iterator = new ClientIterator();
            iterator.onNextPage(columns, rowPageLast);
        }
    }

    @Nonnull
    @Override
    public SqlRowMetadata getRowMetadata() {
        checkIsRowsResult();
        assert rowMetadata != null;
        return rowMetadata;
    }

    @Override
    @Nonnull
    public Iterator<SqlRow> iterator() {
        if (iteratorAccessed) {
            throw new IllegalStateException("Iterator can be requested only once");
        }

        checkIsRowsResult();

        iteratorAccessed = true;
        return iterator;
    }

    @Override
    public long updateCount() {
        return updateCount;
    }

    @Override
    public void close() {
        if (iterator == null) {
            return;
        }

        try {
            if (!closed) {
                if (iterator.last) {
                    // The last page accessed, so the remote cursor is already closed. No-op.
                    return;
                }

                service.close(connection, queryId);
            }
        } finally {
            closed = true;
        }
    }

    private void checkIsRowsResult() {
        if (iterator == null) {
            throw new IllegalStateException("This result contains only update count");
        }
    }

    private void fetchNextPage(ClientIterator iterator) {
        SqlPage page = service.fetch(connection, queryId, cursorBufferSize);

        iterator.onNextPage(page.getColumns(), page.isLast());
    }

    /**
     * Implementation of lazy iterator, which fetches results as needed.
     */
    private class ClientIterator implements Iterator<SqlRow> {

        private List<List<Object>> currentColumns;
        private int currentRowCount;
        private int currentPosition;
        private boolean last;

        @Override
        public boolean hasNext() {
            if (closed) {
                throw service.rethrow(QueryException.cancelledByUser());
            }

            while (currentPosition == currentRowCount) {
                // Reached end of the page. Try fetching the next one if possible.
                if (!last) {
                    fetchNextPage(this);
                } else {
                    // No more pages expected, so return false.
                    return false;
                }
            }

            return true;
        }

        @Override
        public SqlRow next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            Row row = getCurrentRow();
            currentPosition++;
            return new SqlRowImpl(rowMetadata, row);
        }

        private void onNextPage(List<List<Object>> columns, boolean rowPageLast) {
            currentColumns = columns;
            currentRowCount = columns.get(0).size();
            currentPosition = 0;

            this.last = rowPageLast;
        }

        private Row getCurrentRow() {
            Object[] values = new Object[rowMetadata.getColumnCount()];
            int index = 0;
            for (List<Object> aColumn : currentColumns) {
                values[index] = service.deserializeRowValue(aColumn.get(currentPosition));
                index++;
            }
            return new HeapRow(values);
        }
    }
}
