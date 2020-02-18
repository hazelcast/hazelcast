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

import com.hazelcast.client.Client;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.SqlCursorImpl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry of active client cursors.
 */
public class SqlClientQueryRegistry {
    /** Node engine. */
    private final NodeEngine nodeEngine;

    /** Registered client cursors. */
    private final ConcurrentHashMap<QueryId, SqlClientState> clientCursors = new ConcurrentHashMap<>();

    public SqlClientQueryRegistry(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    public void execute(UUID clientId, QueryId queryId, SqlCursorImpl cursor) {
        SqlClientState clientCursor = new SqlClientState(clientId, queryId, cursor);

        clientCursors.put(queryId, clientCursor);

        // TODO: We should avoid that check and do periodic cleanup instead. An alternative - use locks, but let's try to avoid
        //  them on the hot path.
        // Double-check for client disconnect.
        boolean delete = true;

        for (Client client : nodeEngine.getHazelcastInstance().getClientService().getConnectedClients()) {
            if (clientId.equals(client.getUuid())) {
                delete = false;

                break;
            }
        }

        if (delete) {
            close(clientId, queryId);
        }
    }

    public SqlClientPage fetch(UUID clientId, QueryId queryId, int pageSize) {
        SqlClientState clientCursor = getClientCursor(clientId, queryId);

        if (clientCursor == null) {
            throw HazelcastSqlException.error("Cursor not found (closed?): " + queryId);
        }

        Iterator<SqlRow> iterator = clientCursor.getIterator();

        List<SqlRow> rows = new ArrayList<>(pageSize);

        while (iterator.hasNext()) {
            SqlRow row = iterator.next();

            rows.add(row);

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
        SqlClientState clientCursor = getClientCursor(clientId, queryId);

        if (clientCursor == null) {
            return;
        }

        clientCursor.getCursor().close();

        deleteClientCursor(clientCursor);
    }

    public void onClientDisconnected(UUID clientId) {
        Set<QueryId> victims = new HashSet<>();

        for (SqlClientState clientCursor : clientCursors.values()) {
            if (clientCursor.getClientId().equals(clientId)) {
                victims.add(clientCursor.getQueryId());
            }
        }

        for (QueryId victim : victims) {
            close(clientId, victim);
        }
    }

    private SqlClientState getClientCursor(UUID clientId, QueryId queryId) {
        SqlClientState cursor = clientCursors.get(queryId);

        if (cursor == null || !cursor.getClientId().equals(clientId)) {
            return null;
        }

        return cursor;
    }

    private void deleteClientCursor(SqlClientState cursor) {
        clientCursors.remove(cursor.getQueryId());
    }
}
