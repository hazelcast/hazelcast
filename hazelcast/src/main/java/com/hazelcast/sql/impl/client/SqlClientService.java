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

import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.SqlBetaCloseCodec;
import com.hazelcast.client.impl.protocol.codec.SqlBetaExecuteCodec;
import com.hazelcast.client.impl.protocol.codec.SqlBetaFetchCodec;
import com.hazelcast.client.impl.protocol.codec.SqlBetaMissingCodec;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.client.impl.spi.impl.ClientInvocationFuture;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.SqlException;
import com.hazelcast.sql.SqlQuery;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.QueryUtils;

import javax.annotation.Nonnull;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Client-side implementation of SQL service.
 */
public class SqlClientService implements SqlService {

    private final HazelcastClientInstanceImpl client;

    public SqlClientService(HazelcastClientInstanceImpl client) {
        this.client = client;
    }

    @Nonnull
    @Override
    public SqlResult query(@Nonnull SqlQuery query) {
        Connection connection = client.getConnectionManager().getRandomConnection(true);

        if (connection == null) {
            throw rethrow(QueryException.error(
                SqlErrorCode.CONNECTION_PROBLEM,
                "Client must be connected to at least one data member to execute SQL queries"
            ));
        }

        try {
            List<Object> params = query.getParameters();

            List<Data> params0 = new ArrayList<>(params.size());

            for (Object param : params) {
                params0.add(serializeParameter(param));
            }

            ClientMessage requestMessage = SqlBetaExecuteCodec.encodeRequest(
                query.getSql(),
                params0,
                query.getTimeoutMillis(),
                query.getCursorBufferSize()
            );

            ClientMessage responseMessage = invoke(requestMessage, connection);

            SqlBetaExecuteCodec.ResponseParameters response = SqlBetaExecuteCodec.decodeResponse(responseMessage);

            handleResponseError(response.error);

            return new SqlClientResult(
                this,
                connection,
                response.queryId,
                response.rowMetadata,
                response.rowPage,
                response.rowPageLast,
                query.getCursorBufferSize()
            );
        } catch (Exception e) {
            throw rethrow(e, connection);
        }
    }

    /**
     * Fetch the next page of the given query.
     *
     * @param connection Connection.
     * @param queryId Query ID.
     * @return Pair: fetched rows + last page flag.
     */
    public SqlPage fetch(Connection connection, String queryId, int cursorBufferSize) {
        try {
            ClientMessage requestMessage = SqlBetaFetchCodec.encodeRequest(queryId, cursorBufferSize);
            ClientMessage responseMessage = invoke(requestMessage, connection);
            SqlBetaFetchCodec.ResponseParameters responseParameters = SqlBetaFetchCodec.decodeResponse(responseMessage);

            handleResponseError(responseParameters.error);

            return new SqlPage(responseParameters.rowPage, responseParameters.rowPageLast);
        } catch (Exception e) {
            throw rethrow(e, connection);
        }
    }

    /**
     * Close remote query cursor.
     *
     * @param connection Connection.
     * @param queryId Query ID.
     */
    void close(Connection connection, String queryId) {
        try {
            ClientMessage requestMessage = SqlBetaCloseCodec.encodeRequest(queryId);

            invoke(requestMessage, connection);
        } catch (Exception e) {
            throw rethrow(e, connection);
        }
    }

    /**
     * Invokes a method that do not have an associated handler on the server side.
     * For testing purposes only.
     */
    public void missing() {
        Connection connection = client.getConnectionManager().getRandomConnection(false);

        if (connection == null) {
            throw rethrow(QueryException.error(
                SqlErrorCode.CONNECTION_PROBLEM,
                "Client is not connected to topology"
            ));
        }

        try {
            ClientMessage requestMessage = SqlBetaMissingCodec.encodeRequest();

            invoke(requestMessage, connection);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    private Data serializeParameter(Object parameter) {
        try {
            return getSerializationService().toData(parameter);
        } catch (Exception e) {
            throw rethrow(
                QueryException.error("Failed to serialize query parameter " + parameter + ": " + e.getMessage())
            );
        }
    }

    Object deserializeRowValue(Data data) {
        try {
            return getSerializationService().toObject(data);
        } catch (Exception e) {
            throw rethrow(
                QueryException.error("Failed to deserialize query result value: " + e.getMessage())
            );
        }
    }

    private UUID getClientId() {
        return client.getLocalEndpoint().getUuid();
    }

    private InternalSerializationService getSerializationService() {
        return client.getSerializationService();
    }

    private ClientMessage invoke(ClientMessage request, Connection connection) throws Exception {
        ClientInvocation invocation = new ClientInvocation(client, request, null, connection);

        ClientInvocationFuture fut = invocation.invoke();

        return fut.get();
    }

    private static void handleResponseError(SqlError error) {
        if (error != null) {
            throw new SqlException(error.getOriginatingMemberId(), error.getCode(), error.getMessage(), null);
        }
    }

    private RuntimeException rethrow(Exception cause, Connection connection) {
        if (!connection.isAlive()) {
            return QueryUtils.toPublicException(
                QueryException.memberConnection(connection.getRemoteAddress()),
                getClientId()
            );
        }

        return rethrow(cause);
    }

    RuntimeException rethrow(Exception cause) {
        // Make sure that AccessControlException is thrown as a top-level exception
        if (cause.getCause() instanceof AccessControlException) {
            return (AccessControlException) cause.getCause();
        }

        throw QueryUtils.toPublicException(cause, getClientId());
    }
}
