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

package com.hazelcast.sql.impl.state;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlColumnMetadata;
import com.hazelcast.sql.SqlColumnType;
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
import java.util.stream.Collectors;

import static com.hazelcast.sql.impl.ResultIterator.HasNextResult.DONE;
import static com.hazelcast.sql.impl.ResultIterator.HasNextResult.YES;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Registry of active client cursors.
 */
public class QueryClientStateRegistry {

    private final ConcurrentHashMap<QueryId, QueryClientState> clientCursors = new ConcurrentHashMap<>();

    public SqlPage registerAndFetch(
        UUID clientId,
        AbstractSqlResult result,
        int cursorBufferSize,
        InternalSerializationService serializationService
    ) {
        QueryClientState clientCursor = new QueryClientState(clientId, result, false);

        boolean delete = false;

        try {
            // Register the cursor.
            QueryClientState previousClientCursor = clientCursors.putIfAbsent(result.getQueryId(), clientCursor);

            // Check if the cursor is already closed.
            if (previousClientCursor != null) {
                assert previousClientCursor.isClosed();

                delete = true;

                throw QueryException.cancelledByUser();
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
                deleteClientCursor(clientCursor);
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
                deleteClientCursor(clientCursor);
            }

            return page;
        } catch (Exception e) {
            // Clear the cursor in the case of exception.
            deleteClientCursor(clientCursor);

            throw e;
        }
    }

    private SqlPage fetchInternal(
        QueryClientState clientCursor,
        int cursorBufferSize,
        InternalSerializationService serializationService,
        boolean respondImmediately
    ) {
        // TODO: No streams
        List<SqlColumnMetadata> columns = clientCursor.getSqlResult().getRowMetadata().getColumns();
        List<SqlColumnType> sqlColumnTypes = columns.stream().map(SqlColumnMetadata::getType).collect(Collectors.toList());

        if (respondImmediately) {
            return new SqlPage(sqlColumnTypes, Collections.emptyList(), false);
        }

        ResultIterator<SqlRow> iterator = clientCursor.getIterator();

        try {
            List<List<Object>> page = new ArrayList<>(columns.size());

            int columnCount = columns.size();

            for (int i = 0; i < columnCount; i++) {
                page.add(new ArrayList<>());
            }

            boolean last = fetchPage(iterator, page, cursorBufferSize, serializationService, columnCount);

            return new SqlPage(sqlColumnTypes, page, last);
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
        List<List<Object>> page,
        int cursorBufferSize,
        InternalSerializationService serializationService,
        int columnCount
    ) {
        assert cursorBufferSize > 0;

        if (!iterator.hasNext()) {
            return true;
        }

        HasNextResult hasNextResult;
        int count = 0;
        do {
            count++;

            SqlRow row = iterator.next();

            for (int i = 0; i < columnCount; i++) {
                List<Object> convertedColumn = page.get(i);

                Object object = row.getObject(i);

                // TODO: ??? Generalize to all objects
                if (object instanceof Portable) {
                    convertedColumn.add(serializationService.toData(object));
                } else {
                    convertedColumn.add(object);
                }
            }
            hasNextResult = iterator.hasNext(0, SECONDS);
        } while (hasNextResult == YES && count < cursorBufferSize);

        return hasNextResult == DONE;
    }

    public void close(UUID clientId, QueryId queryId) {
        QueryClientState clientCursor =
            clientCursors.computeIfAbsent(queryId, (ignore) -> new QueryClientState(clientId, null, true));

        if (clientCursor.isClosed()) {
            // Received the "close" request before the "execute" request, do nothing.
            return;
        }

        // Received the "close" request after the "execute" request, close.
        clientCursor.getSqlResult().close();

        deleteClientCursor(clientCursor);
    }

    public void shutdown() {
        clientCursors.clear();
    }

    public void update(Set<UUID> activeClientIds) {
        List<QueryClientState> victims = new ArrayList<>();

        for (QueryClientState clientCursor : clientCursors.values()) {
            if (!activeClientIds.contains(clientCursor.getClientId())) {
                victims.add(clientCursor);
            }
        }

        for (QueryClientState victim : victims) {
            QueryException error = QueryException.clientMemberConnection(victim.getClientId());

            victim.getSqlResult().close(error);

            deleteClientCursor(victim);
        }
    }

    private void deleteClientCursor(QueryClientState cursor) {
        clientCursors.remove(cursor.getQueryId());
    }

    public int getCursorCount() {
        return clientCursors.size();
    }
}
