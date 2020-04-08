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

import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.SqlCursorImpl;
import com.hazelcast.sql.impl.SqlRowImpl;
import com.hazelcast.sql.impl.row.Row;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry of active client cursors.
 */
public class QueryClientStateRegistry {
    /** Registered client cursors. */
    private final ConcurrentHashMap<QueryId, QueryClientState> clientCursors = new ConcurrentHashMap<>();

    public void register(UUID clientId, SqlCursorImpl cursor) {
        QueryClientState clientCursor = new QueryClientState(clientId, cursor);

        clientCursors.put(cursor.getQueryId(), clientCursor);
    }

    public SqlClientPage fetch(UUID clientId, QueryId queryId, int pageSize) {
        QueryClientState clientCursor = getClientCursor(clientId, queryId);

        if (clientCursor == null) {
            throw QueryException.error("Cursor not found (closed?): " + queryId);
        }

        Iterator<SqlRow> iterator = clientCursor.getIterator();

        List<Row> rows = new ArrayList<>(pageSize);

        while (iterator.hasNext()) {
            SqlRow row = iterator.next();

            // TODO: Avoid this double wrap-unwrap. Instead, we should fetch Row from the cursor here.
            rows.add(((SqlRowImpl) row).getDelegate());

            if (rows.size() == pageSize) {
                break;
            }
        }

        boolean last = !iterator.hasNext();

        if (last) {
            deleteClientCursor(clientCursor);
        }

        return new SqlClientPage(rows, last);
    }

    public void close(UUID clientId, QueryId queryId) {
        QueryClientState clientCursor = getClientCursor(clientId, queryId);

        if (clientCursor == null) {
            return;
        }

        clientCursor.getCursor().close();

        deleteClientCursor(clientCursor);
    }

    public void reset() {
        clientCursors.clear();
    }

    public void update(Collection<UUID> activeClientIds) {
        List<QueryClientState> victims = new ArrayList<>();

        for (QueryClientState clientCursor : clientCursors.values()) {
            if (activeClientIds.contains(clientCursor.getClientId())) {
                victims.add(clientCursor);
            }
        }

        for (QueryClientState victim : victims) {
            QueryException error = QueryException.clientLeave(victim.getClientId());

            victim.getCursor().closeOnError(error);

            deleteClientCursor(victim);
        }
    }

    private QueryClientState getClientCursor(UUID clientId, QueryId queryId) {
        QueryClientState cursor = clientCursors.get(queryId);

        if (cursor == null || (clientId != null && !cursor.getClientId().equals(clientId))) {
            return null;
        }

        return cursor;
    }

    private void deleteClientCursor(QueryClientState cursor) {
        clientCursors.remove(cursor.getQueryId());
    }
}
