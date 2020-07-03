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
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlRowMetadata;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.SqlRowImpl;
import com.hazelcast.sql.impl.row.Row;

import javax.annotation.Nonnull;
import java.util.ArrayList;
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
    private final ClientIterator iterator = new ClientIterator();

    private int cursorBufferSize;
    private boolean closed;
    private boolean iteratorAccessed;

    public SqlClientResult(
        SqlClientService service,
        Connection connection,
        QueryId queryId,
        SqlRowMetadata rowMetadata,
        SqlPage page,
        int cursorBufferSize
    ) {
        this.service = service;
        this.connection = connection;
        this.queryId = queryId;
        this.rowMetadata = rowMetadata;
        this.cursorBufferSize = cursorBufferSize;

        iterator.onNextPage(page);
    }

    @Nonnull
    @Override
    public SqlRowMetadata getRowMetadata() {
        return rowMetadata;
    }

    @Override
    @Nonnull
    public Iterator<SqlRow> iterator() {
        if (!iteratorAccessed) {
            iteratorAccessed = true;

            return iterator;
        } else {
            throw new IllegalStateException("Iterator could be requested only once");
        }
    }

    @Override
    public void close() {
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

    public int getCursorBufferSize() {
        return cursorBufferSize;
    }

    public void setCursorBufferSize(int cursorBufferSize) {
        assert cursorBufferSize >= 0;

        this.cursorBufferSize = cursorBufferSize;
    }

    private void fetchNextPage(ClientIterator iterator) {
        SqlPage page = service.fetch(connection, queryId, cursorBufferSize);

        iterator.onNextPage(page);
    }

    private List<Row> convertPageRows(List<Data> serializedRows) {
        List<Row> rows = new ArrayList<>(serializedRows.size());

        for (Data serializedRow : serializedRows) {
            rows.add(service.deserializeRow(serializedRow));
        }

        return rows;
    }

    /**
     * Implementation of lazy iterator, which fetches results as needed.
     */
    private class ClientIterator implements Iterator<SqlRow> {

        private List<Row> currentRows;
        private int currentPosition;
        private boolean last;

        @Override
        public boolean hasNext() {
            if (closed) {
                throw service.rethrow(QueryException.cancelledByUser());
            }

            while (currentPosition == currentRows.size()) {
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

            Row row = currentRows.get(currentPosition++);

            return new SqlRowImpl(rowMetadata, row);
        }

        private void onNextPage(SqlPage page) {
            currentRows = convertPageRows(page.getRows());
            currentPosition = 0;

            this.last = page.isLast();
        }
    }
}
