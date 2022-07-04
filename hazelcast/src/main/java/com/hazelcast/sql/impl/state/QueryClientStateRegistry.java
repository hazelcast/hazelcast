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

package com.hazelcast.sql.impl.state;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlColumnMetadata;
import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.impl.AbstractSqlResult;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.ResultIterator;
import com.hazelcast.sql.impl.ResultIterator.HasNextResult;
import com.hazelcast.sql.impl.client.SqlPage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.sql.impl.ResultIterator.HasNextResult.DONE;
import static com.hazelcast.sql.impl.ResultIterator.HasNextResult.YES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Registry of active client cursors.
 */
public class QueryClientStateRegistry {

    private static final long DEFAULT_CLOSED_CURSOR_CLEANUP_TIMEOUT_NS = NANOSECONDS.convert(30, SECONDS);

    private final ConcurrentHashMap<QueryId, QueryClientState> clientCursors = new ConcurrentHashMap<>();
    private volatile long closedCursorCleanupTimeoutNs = DEFAULT_CLOSED_CURSOR_CLEANUP_TIMEOUT_NS;

    public SqlPage registerAndFetch(
        UUID clientId,
        AbstractSqlResult result,
        int cursorBufferSize,
        InternalSerializationService serializationService
    ) {
        QueryId queryId = result.getQueryId();

        QueryClientState clientCursor = new QueryClientState(clientId, queryId, result, false);

        boolean delete = false;

        try {
            // Register the cursor.
            QueryClientState previousClientCursor = clientCursors.putIfAbsent(queryId, clientCursor);

            // Check if the cursor is already closed.
            if (previousClientCursor != null) {
                assert previousClientCursor.isClosed();

                delete = true;

                QueryException error = QueryException.cancelledByUser();

                result.close(error);

                throw error;
            }

            // Fetch the next page.
            SqlPage page = fetchInternal(clientCursor, cursorBufferSize, serializationService, result.isInfiniteRows());

            delete = page.isLast();

            return page;
        } catch (Exception e) {
            delete = true;

            throw e;
        } finally {
            if (delete) {
                deleteClientCursor(queryId);
            }
        }
    }

    public SqlPage fetch(
        QueryId queryId,
        int cursorBufferSize,
        InternalSerializationService serializationService
    ) {
        QueryClientState clientCursor = clientCursors.get(queryId);

        if (clientCursor == null) {
            throw QueryException.error("Query cursor is not found (closed?): " + queryId);
        }

        try {
            SqlPage page = fetchInternal(clientCursor, cursorBufferSize, serializationService, false);

            if (page.isLast()) {
                deleteClientCursor(clientCursor.getQueryId());
            }

            return page;
        } catch (Exception e) {
            // Clear the cursor in the case of exception.
            deleteClientCursor(clientCursor.getQueryId());

            throw e;
        }
    }

    private SqlPage fetchInternal(
        QueryClientState clientCursor,
        int cursorBufferSize,
        InternalSerializationService serializationService,
        boolean respondImmediately
    ) {
        List<SqlColumnMetadata> columns = clientCursor.getSqlResult().getRowMetadata().getColumns();
        List<SqlColumnType> columnTypes = new ArrayList<>(columns.size());

        for (SqlColumnMetadata column : columns) {
            columnTypes.add(column.getType());
        }

        if (respondImmediately) {
            return SqlPage.fromRows(columnTypes, Collections.emptyList(), false, serializationService);
        }

        ResultIterator<SqlRow> iterator = clientCursor.getIterator();

        try {
            List<SqlRow> rows = new ArrayList<>(cursorBufferSize);
            boolean last = fetchPage(iterator, rows, cursorBufferSize);

            return SqlPage.fromRows(columnTypes, rows, last, serializationService);
        } catch (HazelcastSqlException e) {
            // We use public API to extract results from the cursor. The cursor may throw HazelcastSqlException only. When
            // it happens, the cursor is already closed with the error, so we just re-throw.
            throw e;
        } catch (Exception e) {
            // Any other exception indicates that something has happened outside of the internal query state. For example,
            // we may fail to serialize a specific column value to Data. We have to close the cursor in this case.
            AbstractSqlResult result = clientCursor.getSqlResult();

            QueryException error = QueryException.error("Failed to prepare the SQL result for the client: " + e.getMessage(), e);

            result.close(error);

            throw error;
        }
    }

    private static boolean fetchPage(
        ResultIterator<SqlRow> iterator,
        List<SqlRow> rows,
        int cursorBufferSize
    ) {
        assert cursorBufferSize > 0;

        if (!iterator.hasNext()) {
            return true;
        }

        HasNextResult hasNextResult;
        do {
            rows.add(iterator.next());

            hasNextResult = iterator.hasNext(0, SECONDS);
        } while (hasNextResult == YES && rows.size() < cursorBufferSize);

        return hasNextResult == DONE;
    }

    public void close(UUID clientId, QueryId queryId) {
        QueryClientState clientCursor =
            clientCursors.computeIfAbsent(queryId, (ignore) -> new QueryClientState(clientId, queryId, null, true));

        if (clientCursor.isClosed()) {
            // Received the "close" request before the "execute" request, do nothing.
            return;
        }

        // Received the "close" request after the "execute" request, close.
        close0(clientCursor);
    }

    public void closeOnError(QueryId queryId) {
        QueryClientState clientCursor = clientCursors.get(queryId);

        if (clientCursor != null) {
            close0(clientCursor);
        }
    }

    private void close0(QueryClientState clientCursor) {
        SqlResult result = clientCursor.getSqlResult();

        if (result != null) {
            result.close();
        }

        deleteClientCursor(clientCursor.getQueryId());
    }

    public void shutdown() {
        clientCursors.clear();
    }

    public void update(Set<UUID> activeClientIds) {
        long currentTimeNano = System.nanoTime();

        List<QueryClientState> victims = new ArrayList<>();

        for (QueryClientState clientCursor : clientCursors.values()) {
            // Close cursors that were opened by disconnected clients.
            if (!activeClientIds.contains(clientCursor.getClientId())) {
                victims.add(clientCursor);
            }

            // Close cursors created for the "cancel" operation, that are too old. This is needed to avoid a race
            // condition between the query cancellation on a client and the query completion on a server.
            if (clientCursor.isClosed() && clientCursor.getCreatedAtNano() + closedCursorCleanupTimeoutNs < currentTimeNano) {
                victims.add(clientCursor);
            }
        }

        for (QueryClientState victim : victims) {
            QueryException error = QueryException.clientMemberConnection(victim.getClientId());

            AbstractSqlResult result = victim.getSqlResult();

            if (result != null) {
                result.close(error);
            }

            deleteClientCursor(victim.getQueryId());
        }
    }

    private void deleteClientCursor(QueryId queryId) {
        clientCursors.remove(queryId);
    }

    public int getCursorCount() {
        return clientCursors.size();
    }

    /**
     * For testing only.
     */
    public void setClosedCursorCleanupTimeoutSeconds(long closedCursorCleanupTimeout) {
        closedCursorCleanupTimeoutNs = NANOSECONDS.convert(closedCursorCleanupTimeout, SECONDS);
    }
}
