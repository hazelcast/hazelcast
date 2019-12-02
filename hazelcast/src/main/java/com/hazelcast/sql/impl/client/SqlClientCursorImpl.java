/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.util.BiTuple;
import com.hazelcast.sql.SqlCursor;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.impl.QueryId;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Client-side cursor.
 */
public class SqlClientCursorImpl implements SqlCursor {
    /** Service. */
    private final SqlClientServiceImpl service;

    /** Connection which should be used to fetch further results. */
    private final Connection connection;

    /** Query ID. */
    private final QueryId queryId;

    /** Client iterator. */
    private final ClientIterator iterator = new ClientIterator();

    /** Whether the cursor is closed. */
    private boolean closed;

    /** Whether iterator was accessed. */
    private boolean iteratorAccessed;

    public SqlClientCursorImpl(SqlClientServiceImpl service, Connection connection, QueryId queryId) {
        this.service = service;
        this.connection = connection;
        this.queryId = queryId;
    }

    @Override
    public void close() throws Exception {
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

    @SuppressWarnings("NullableProblems")
    @Override
    public Iterator<SqlRow> iterator() {
        if (!iteratorAccessed) {
            iteratorAccessed = true;

            fetchNextPage(iterator);

            return iterator;
        } else {
            throw new IllegalStateException("Iterator could be requested only once");
        }
    }

    private void fetchNextPage(ClientIterator iterator) {
        BiTuple<List<SqlRow>, Boolean> nextPage = service.fetch(connection, queryId);

        iterator.onNextPage(nextPage.element1, nextPage.element2);
    }

    /**
     * Implementation of lazy iterator, which fetches results as needed.
     */
    private class ClientIterator implements Iterator<SqlRow> {
        /** Current page. */
        private List<SqlRow> currentPage;

        /** Position in the current page. */
        private int currentPagePosition;

        /** Last page flag. */
        private boolean last;

        @Override
        public boolean hasNext() {
            if (closed) {
                throw new IllegalStateException("Cursor was closed.");
            }

            if (currentPagePosition == currentPage.size()) {
                // Reached end of the page. Try fetching the next one if possible.
                if (!last) {
                    fetchNextPage(this);
                } else {
                    // No more pages expected, so return false.
                    return false;
                }
            }

            // It may happen that the next page has no results. It should be the last page then.
            if (currentPage.size() == currentPagePosition) {
                assert currentPage.size() == 0;
                assert last;

                return false;
            } else {
                return true;
            }
        }

        @Override
        public SqlRow next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            return currentPage.get(currentPagePosition++);
        }

        /**
         * Accept the next page.
         *
         * @param page Page.
         * @param last Last page flag.
         */
        private void onNextPage(List<SqlRow> page, boolean last) {
            currentPage = page;
            currentPagePosition = 0;
            this.last = last;
        }
    }
}
