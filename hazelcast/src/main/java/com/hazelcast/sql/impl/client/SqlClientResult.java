/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.impl.connection.ClientConnection;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlRowMetadata;
import com.hazelcast.sql.SqlStatement;
import com.hazelcast.sql.impl.CoreQueryUtils;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.ResultIterator;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.SqlRowImpl;
import com.hazelcast.sql.impl.row.JetSqlRow;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nonnull;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * A wrapper around the normal client result that tracks the first response, and manages close requests.
 */
public class SqlClientResult implements SqlResult {
    private final SqlClientService service;
    private final int cursorBufferSize;
    private final Function<QueryId, ClientMessage> sqlExecuteMessageSupplier;
    private final boolean selectQuery;
    private volatile QueryId queryId;
    private ClientConnection connection;
    private int resubmissionCount;

    /** Mutex to synchronize access between operations. */
    private final Object mux = new Object();

    /** The current result state. */
    private State state;

    /** Whether the iterator has already been requested. When {@code true}, future calls to iterator() will throw an error. */
    private boolean iteratorRequested;

    /** Whether the result is closed. When {@code true}, there is no need to send the "cancel" request to the server. */
    private boolean closed;

    /** Whether any SqlRow was returned from an iterator. */
    private volatile boolean returnedAnyResult;

    /** Whether the result set is unbounded. */
    private volatile Boolean isInfiniteRows;

    /** Fetch descriptor. Available when the fetch operation is in progress. */
    private SqlFetchResult fetch;

    /** Whether the last fetch() invoked resubmission. */
    private boolean lastFetchResubmitted;

    public SqlClientResult(
            SqlClientService service,
            ClientConnection connection,
            QueryId queryId,
            int cursorBufferSize,
            Function<QueryId, ClientMessage> sqlExecuteMessageSupplier,
            SqlStatement statement
    ) {
        this.service = service;
        this.connection = connection;
        this.queryId = queryId;
        this.cursorBufferSize = cursorBufferSize;
        this.sqlExecuteMessageSupplier = sqlExecuteMessageSupplier;
        this.selectQuery = statement.getSql().trim().toLowerCase().startsWith("select");
    }

    /**
     * Invoked when the {@code execute} operation completes normally.
     */
    public void onExecuteResponse(
        SqlRowMetadata rowMetadata,
        SqlPage rowPage,
        long updateCount,
        Boolean isInfiniteRows
    ) {
        synchronized (mux) {
            this.isInfiniteRows = isInfiniteRows;
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

    public void onResubmissionResponse(SqlResubmissionResult result) {
        synchronized (mux) {
            if (closed) {
                // The result is already closed, ignore the response.
                return;
            }

            if (state != null && state.iterator != null && !state.iterator.rowMetadata.equals(result.getRowMetadata())) {
                throw new HazelcastSqlException(queryId.getMemberId(), SqlErrorCode.GENERIC,
                        "Row metadata changed after resubmission", null, null);
            }

            this.fetch = null;
            this.connection = result.getConnection();
            this.resubmissionCount++;

            if (result.getRowMetadata() != null) {
                ClientIterator iterator = state == null ? new ClientIterator(result.getRowMetadata()) : state.iterator;
                iterator.onNextPage(result.getRowPage());
                state = new State(iterator, -1, null);
            } else {
                state = new State(null, result.getUpdateCount(), null);
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
     * Mark the result as closed. Invoked when we receive an update count or the last page.
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
        lastFetchResubmitted = false;
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
                SqlResubmissionResult resubmissionResult = service.resubmitIfPossible(this, fetch.getError());
                if (resubmissionResult == null) {
                    throw wrap(fetch.getError());
                }
                lastFetchResubmitted = true;
                onResubmissionResponse(resubmissionResult);

                // In onResubmissionResponse we change currentPage on iterator, so we now need to return it.
                return state.iterator.currentPage;
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
        throw CoreQueryUtils.toPublicException(error, service.getClientId());
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
                    do {
                        SqlPage page = fetch(timeUnit.toNanos(timeout));
                        if (page == null) {
                            return HasNextResult.TIMEOUT;
                        }
                        onNextPage(page);
                        // The fetch() method may invoke resubmission that invokes SqlExecute operation. The SqlExecute may end
                        // without any results in the buffer. In that case we need to invoke fetch() again.
                    } while (lastFetchResubmitted && (!last && currentPosition == currentRowCount));
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
            returnedAnyResult = true;
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

    ClientMessage getSqlExecuteMessage(QueryId newId) {
        return sqlExecuteMessageSupplier.apply(newId);
    }

    boolean isSelectQuery() {
        return selectQuery;
    }

    boolean isReturnedAnyResult() {
        return returnedAnyResult;
    }

    QueryId getQueryId() {
        return queryId;
    }

    void setQueryId(QueryId queryId) {
        this.queryId = queryId;
    }

    boolean wasResubmission() {
        synchronized (mux) {
            return resubmissionCount > 0;
        }
    }

    public Boolean isInfiniteRows() {
        return isInfiniteRows;
    }
}
