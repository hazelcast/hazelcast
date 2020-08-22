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

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.impl.AbstractSqlResult;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.ResultIterator;
import com.hazelcast.sql.impl.ResultIterator.HasNextImmediatelyResult;
import com.hazelcast.sql.impl.client.SqlPage;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.sql.impl.ResultIterator.HasNextImmediatelyResult.DONE;
import static com.hazelcast.sql.impl.ResultIterator.HasNextImmediatelyResult.YES;

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
        QueryClientState clientCursor = new QueryClientState(clientId, result);

        SqlPage page = fetchInternal(clientCursor, cursorBufferSize, serializationService);

        if (!page.isLast()) {
            clientCursors.put(result.getQueryId(), clientCursor);
        }

        return page;
    }

    public SqlPage fetch(
        UUID clientId,
        QueryId queryId,
        int cursorBufferSize,
        InternalSerializationService serializationService
    ) {
        QueryClientState clientCursor = getClientCursor(clientId, queryId);

        if (clientCursor == null) {
            throw QueryException.error("Query cursor is not found (closed?): " + queryId);
        }

        SqlPage page = fetchInternal(clientCursor, cursorBufferSize, serializationService);

        if (page.isLast()) {
            deleteClientCursor(clientCursor);
        }

        return page;
    }

    private SqlPage fetchInternal(
        QueryClientState clientCursor,
        int cursorBufferSize,
        InternalSerializationService serializationService
    ) {
        ResultIterator<SqlRow> iterator = clientCursor.getIterator();

        List<List<Data>> page = new ArrayList<>(cursorBufferSize);
        boolean last = fetchPage(iterator, page, cursorBufferSize, serializationService);

        if (last) {
            deleteClientCursor(clientCursor);
        }

        return new SqlPage(page, last);
    }

    private static boolean fetchPage(
        ResultIterator<SqlRow> iterator,
        List<List<Data>> page,
        int cursorBufferSize,
        InternalSerializationService serializationService
    ) {
        assert cursorBufferSize > 0;

        if (!iterator.hasNext()) {
            return true;
        }

        HasNextImmediatelyResult hasNextResult;
        do {
            SqlRow row = iterator.next();
            List<Data> convertedRow = convertRow(row, serializationService);

            page.add(convertedRow);
            hasNextResult = iterator.hasNextImmediately();
        } while (hasNextResult == YES && page.size() < cursorBufferSize);

        return hasNextResult == DONE;
    }

    private static List<Data> convertRow(SqlRow row, InternalSerializationService serializationService) {
        int columnCount = row.getMetadata().getColumnCount();

        List<Data> values = new ArrayList<>(columnCount);

        for (int i = 0; i < columnCount; i++) {
            values.add(serializationService.toData(row.getObject(i)));
        }

        return values;
    }

    public void close(UUID clientId, QueryId queryId) {
        QueryClientState clientCursor = getClientCursor(clientId, queryId);

        if (clientCursor != null) {
            clientCursor.getSqlResult().close();

            deleteClientCursor(clientCursor);
        }
    }

    public void reset() {
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

            victim.getSqlResult().closeOnError(error);

            deleteClientCursor(victim);
        }
    }

    private QueryClientState getClientCursor(UUID clientId, QueryId queryId) {
        QueryClientState cursor = clientCursors.get(queryId);

        if (cursor == null || !cursor.getClientId().equals(clientId)) {
            return null;
        }

        return cursor;
    }

    private void deleteClientCursor(QueryClientState cursor) {
        clientCursors.remove(cursor.getQueryId());
    }

    public int getCursorCount() {
        return clientCursors.size();
    }
}
