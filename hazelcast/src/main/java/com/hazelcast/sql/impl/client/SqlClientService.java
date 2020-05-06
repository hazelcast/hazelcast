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

import com.hazelcast.client.impl.ClientDelegatingFuture;
import com.hazelcast.client.impl.clientside.ClientMessageDecoder;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.SqlCloseCodec;
import com.hazelcast.client.impl.protocol.codec.SqlExecuteCodec;
import com.hazelcast.client.impl.protocol.codec.SqlFetchCodec;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.client.impl.spi.impl.ClientInvocationFuture;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.util.BiTuple;
import com.hazelcast.sql.SqlCursor;
import com.hazelcast.sql.SqlQuery;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.SqlUpdate;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.row.Row;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.hazelcast.internal.util.ExceptionUtil.rethrow;

/**
 * Client-side implementation of SQL service.
 */
// TODO: Need to improve query ID serialization: either make it first-class citizen for the protocol, or use another UUID
//  to remap from QueryId to more light-weight UUID. The latter might be not good from the manageability standpoint, as user
//  will have two IDs at hands.
public class SqlClientService implements SqlService {
    /** Decoder for execute request. */
    private static final ClientMessageDecoder<SqlClientExecuteResponse> EXECUTE_DECODER =
        clientMessage -> {
            SqlExecuteCodec.ResponseParameters response = SqlExecuteCodec.decodeResponse(clientMessage);

            return new SqlClientExecuteResponse(response.queryId, response.columnCount);
        };

    /** Decoder for fetch request. */
    private static final ClientMessageDecoder<BiTuple<List<Data>, Boolean>> FETCH_DECODER = clientMessage -> {
        SqlFetchCodec.ResponseParameters response = SqlFetchCodec.decodeResponse(clientMessage);

        return BiTuple.of(response.rows, response.last);
    };

    /** Decoder for close request. */
    private static final ClientMessageDecoder<Void> CLOSE_DECODER = clientMessage -> {
        SqlCloseCodec.decodeResponse(clientMessage);

        return null;
    };

    /** Client. */
    private final HazelcastClientInstanceImpl client;

    public SqlClientService(HazelcastClientInstanceImpl client) {
        this.client = client;
    }

    @Override
    public SqlCursor query(SqlQuery query) {
        List<Object> params = query.getParameters();
        List<Data> params0;

        if (!params.isEmpty()) {
            params0 = new ArrayList<>(params.size());

            for (Object param : params) {
                params0.add(toData(param));
            }
        } else {
            params0 = null;
        }

        ClientMessage message = SqlExecuteCodec.encodeRequest(query.getSql(), params0);

        Connection connection = client.getConnectionManager().getRandomConnection();

        SqlClientExecuteResponse response = invoke(message, connection, EXECUTE_DECODER);

        QueryId queryId = toObject(response.getQueryId());

        return new SqlClientCursorImpl(this, connection, queryId, response.getColumnCount(), query.getPageSize());
    }

    /**
     * Fetch the next page of the given query.
     *
     * @param connection Connection.
     * @param queryId Query ID.
     * @return Pair: fetched rows + last page flag.
     */
    public BiTuple<List<Row>, Boolean> fetch(Connection connection, QueryId queryId, int pageSize) {
        ClientMessage message = SqlFetchCodec.encodeRequest(toData(queryId), pageSize);

        BiTuple<List<Data>, Boolean> res = invoke(message, connection, FETCH_DECODER);

        List<Data> serializedRows = res.element1;
        boolean last = res.element2;

        List<Row> rows;

        if (serializedRows.isEmpty()) {
            rows = Collections.emptyList();
        } else {
            rows = new ArrayList<>(serializedRows.size());

            for (Data serializedRow : serializedRows) {
                rows.add(toObject(serializedRow));
            }
        }

        return BiTuple.of(rows, last);
    }

    /**
     * Close remote query cursor.
     *
     * @param conn Connection.
     * @param queryId Query ID.
     */
    void close(Connection conn, QueryId queryId) {
        ClientMessage request = SqlCloseCodec.encodeRequest(toData(queryId));

        invoke(request, conn, CLOSE_DECODER);
    }

    @Override
    public void update(SqlUpdate update) {
        throw new UnsupportedOperationException(); // TODO:
    }

    private <T> Data toData(T o) {
        return getSerializationService().toData(o);
    }

    private <T> T toObject(Data data) {
        return getSerializationService().toObject(data);
    }

    private InternalSerializationService getSerializationService() {
        return client.getSerializationService();
    }

    private <T> T invoke(ClientMessage request, Connection connection, ClientMessageDecoder<T> decoder) {
        try {
            ClientInvocation invocation = new ClientInvocation(client, request, null, connection);
            ClientInvocationFuture fut = invocation.invoke();
            return new ClientDelegatingFuture<T>(fut, getSerializationService(), decoder, false).get();
        } catch (Exception e) {
            throw rethrow(e);
        }
    }
}
