/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlRowMetadata;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.QueryUtils;
import com.hazelcast.sql.impl.ResultIterator;
import com.hazelcast.sql.impl.SqlRowImpl;
import com.hazelcast.sql.impl.row.JetSqlRow;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nonnull;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * A wrapper around the normal client result that tracks the first response, and manages close requests.
 */
public class SqlClientResult implements SqlResult {

    private final SqlClientService service;
    private final Connection connection;
    private final QueryId queryId;
    private final int cursorBufferSize;

    /** Mutex to synchronize access between operations. */
    private final Object mux = new Object();

    /** The current result state. */
    private State state;

    /** Whether the iterator has already been requested. When {@code true}, future calls to iterator() will throw an error. */
    private boolean iteratorRequested;

    /** Whether the result is closed. When {@code true}, there is no need to send the "cancel" request to the server. */
    private boolean closed;

    /** Fetch descriptor. Available when the fetch operation is in progress. */
    private SqlFetchResult fetch;

    public SqlClientResult(SqlClientService service, Connection connection, QueryId queryId, int cursorBufferSize) {
        this.service = service;
        this.connection = connection;
        this.queryId = queryId;
        this.cursorBufferSize = cursorBufferSize;
    }

    /**
     * Invoked when the {@code execute} operation completes normally.
     */
    public void onExecuteResponse(
        SqlRowMetadata rowMetadata,
        SqlPage rowPage,
        long updateCount
    ) {
        synchronized (mux) {
            if (closed) {
                // The result is already closed, ignore the response.
                return;
            }

            if (rowMetadata != null) {
                ClientIterator iterator = new ClientIterator(rowMetadata);
                iterator.onNextPage(rowPage);

                state = new State(iterator, -1, null);
            } else {
                state = new State(null, updateCount, null);

                markClosed();
            }

            mux.notifyAll();
        }
    }

    /**
     * Invoked when the {@code execute} operation completes with an error.
     */
    public void onExecuteError(RuntimeException error) {
        synchronized (mux) {
            if (closed) {
                return;
            }

            state = new State(null, -1, error);

            mux.notifyAll();
        }
    }

    @SuppressFBWarnings("NP_NONNULL_RETURN_VIOLATION")
    @Nonnull
    @Override
    public SqlRowMetadata getRowMetadata() {
        State state = awaitState();

        ClientIterator iterator = state.iterator;

        if (iterator == null) {
            throw new IllegalStateException("This result contains only update count");
        } else {
            return iterator.rowMetadata;
        }
    }

    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE")
    @Nonnull
    @Override
    public ResultIterator<SqlRow> iterator() {
        State state = awaitState();

        ClientIterator iterator = state.iterator;

        if (iterator == null) {
            throw new IllegalStateException("This result contains only update count");
        }

        if (iteratorRequested) {
            throw new IllegalStateException("Iterator can be requested only once");
        }

        iteratorRequested = true;

        return iterator;
    }

    @Override
    public long updateCount() {
        State state = awaitState();

        return state.updateCount;
    }

    @Override
    public void close() {
        synchronized (mux) {
            try {
                // Do nothing if the result is already closed.
                if (closed) {
                    return;
                }

                // If the cancellation is initiated before the first response is received, then throw cancellation errors on
                // the dependent methods (update count, row metadata, iterator).
                if (state == null) {
                    onExecuteError(QueryException.cancelledByUser());
                }

                // Make sure that all subsequent fetches will fail.
                if (fetch == null) {
                    fetch = new SqlFetchResult();
                }

                onFetchFinished(null, QueryException.cancelledByUser());

                // Send the close request.
                service.close(connection, queryId);
            } finally {
                // Set the closed flag to avoid multiple close requests.
                closed = true;
            }
        }
    }

    /**
     * Mark result as closed. Invoked when we received an update count or the last page.
     */
    private void markClosed() {
        synchronized (mux) {
            closed = true;
        }
    }

    /**
     * Fetches the next page.
     */
    private SqlPage fetch(long timeoutNanos) {
        synchronized (mux) {
            if (fetch != null) {
                if (fetch.getError() != null) {
                    // Re-throw previously logged error on successive fetch attempts.
                    throw wrap(fetch.getError());
                }
            } else {
                // Initiate the fetch.
                fetch = new SqlFetchResult();
                service.fetchAsync(connection, queryId, cursorBufferSize, this);
            }

            // Await the response.
            long waitNanos = timeoutNanos;
            while (fetch.isPending() && waitNanos > 0) {
                try {
                    long startNanos = System.nanoTime();
                    TimeUnit.NANOSECONDS.timedWait(mux, waitNanos);
                    waitNanos -= (System.nanoTime() - startNanos);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw wrap(QueryException.error("Interrupted while waiting for the response from the server.", e));
                }
            }

            if (fetch.isPending()) {
                return null;
            }

            if (fetch.getError() != null) {
                throw wrap(fetch.getError());
            } else {
                SqlPage page = fetch.getPage();
                assert page != null;
                fetch = null;
                return page;
            }
        }
    }

    /**
     * Callback invoked when the fetch operation is finished.
     */
    public void onFetchFinished(SqlPage page, RuntimeException error) {
        synchronized (mux) {
            assert fetch != null && fetch.isPending();

            fetch.onResult(page, error);

            mux.notifyAll();
        }
    }

    /**
     * Await the result, throwing an error if something went wrong.
     */
    private State awaitState() {
        State state = awaitStateNoThrow();

        if (state.error != null) {
            throw wrap(state.error);
        }

        return state;
    }

    /**
     * Await for the result, and return an associated error, if any.
     */
    private State awaitStateNoThrow() {
        synchronized (mux) {
            while (state == null) {
                try {
                    mux.wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();

                    QueryException error =
                            QueryException.error("Interrupted while waiting for the response from the server.", e);

                    return new State(null, -1, error);
                }
            }

            return state;
        }
    }

    private HazelcastSqlException wrap(Throwable error) {
        throw QueryUtils.toPublicException(error, service.getClientId());
    }

    private static final class State {

        private final ClientIterator iterator;
        private final long updateCount;
        private final RuntimeException error;

        private State(ClientIterator iterator, long updateCount, RuntimeException error) {
            this.iterator = iterator;
            this.updateCount = updateCount;
            this.error = error;
        }
    }

    private final class ClientIterator implements ResultIterator<SqlRow> {
        private final SqlRowMetadata rowMetadata;
        private SqlPage currentPage;
        private int currentRowCount;
        private int currentPosition;
        private boolean last;

        private ClientIterator(SqlRowMetadata rowMetadata) {
            assert rowMetadata != null;

            this.rowMetadata = rowMetadata;
        }

        @Override
        public HasNextResult hasNext(long timeout, TimeUnit timeUnit) {
            if (currentPosition == currentRowCount) {
                // Reached end of the page. Try fetching the next one if possible.
                if (!last) {
                    SqlPage page = fetch(timeUnit.toNanos(timeout));
                    if (page == null) {
                        return HasNextResult.TIMEOUT;
                    }
                    onNextPage(page);
                } else {
                    // No more pages expected, so return false.
                    return HasNextResult.DONE;
                }
            }

            // We could fetch the last page
            if (currentPosition == currentRowCount) {
                assert last;
                return HasNextResult.DONE;
            }

            return HasNextResult.YES;
        }

        @Override
        public boolean hasNext() {
            return hasNext(Long.MAX_VALUE, NANOSECONDS) == HasNextResult.YES;
        }

        @Override
        public SqlRow next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            JetSqlRow row = getCurrentRow();
            currentPosition++;
            return new SqlRowImpl(rowMetadata, row);
        }

        private void onNextPage(SqlPage page) {
            currentPage = page;
            currentRowCount = page.getRowCount();
            currentPosition = 0;

            if (page.isLast()) {
                this.last = true;

                markClosed();
            }
        }

        private JetSqlRow getCurrentRow() {
            Object[] values = new Object[rowMetadata.getColumnCount()];

            for (int i = 0; i < currentPage.getColumnCount(); i++) {
                values[i] = currentPage.getColumnValueForClient(i, currentPosition);
            }

            return new JetSqlRow(service.getSerializationService(), values);
        }
    }
}
