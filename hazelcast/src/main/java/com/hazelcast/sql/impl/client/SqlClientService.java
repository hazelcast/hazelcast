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
import com.hazelcast.client.impl.protocol.codec.SqlCloseCodec;
import com.hazelcast.client.impl.protocol.codec.SqlExecuteCodec;
import com.hazelcast.client.impl.protocol.codec.SqlFetchCodec;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.client.impl.spi.impl.ClientInvocationFuture;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlStatement;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRowMetadata;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.QueryId;
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

    private static final int SERVICE_ID_MASK = 0x00FF0000;
    private static final int SERVICE_ID_SHIFT = 16;

    private static final int METHOD_ID_MASK = 0x0000FF00;
    private static final int METHOD_ID_SHIFT = 8;

    /** ID of the SQL beta service. Should match the ID declared in Sql.yaml */
    private static final int SQL_SERVICE_ID = 33;

    private static final int INVALID_MESSAGE_ID = 255;

    private final HazelcastClientInstanceImpl client;

    public SqlClientService(HazelcastClientInstanceImpl client) {
        this.client = client;
    }

    @Nonnull
    @Override
    public SqlResult execute(@Nonnull SqlStatement statement) {
        Connection connection = client.getConnectionManager().getRandomConnection(true);

        if (connection == null) {
            throw rethrow(QueryException.error(
                SqlErrorCode.CONNECTION_PROBLEM,
                "Client must be connected to at least one data member to execute SQL queries"
            ));
        }

        try {
            List<Object> params = statement.getParameters();

            List<Data> params0 = new ArrayList<>(params.size());

            for (Object param : params) {
                params0.add(serializeParameter(param));
            }

            ClientMessage requestMessage = SqlExecuteCodec.encodeRequest(
                statement.getSql(),
                params0,
                statement.getTimeoutMillis(),
                statement.getCursorBufferSize()
            );

            ClientMessage responseMessage = invoke(requestMessage, connection);

            SqlExecuteCodec.ResponseParameters response = SqlExecuteCodec.decodeResponse(responseMessage);

            handleResponseError(response.error);

            return new SqlClientResult(
                this,
                connection,
                response.queryId,
                response.rowMetadata != null ? new SqlRowMetadata(response.rowMetadata) : null,
                response.rowPage,
                response.rowPageLast,
                statement.getCursorBufferSize(),
                response.updateCount
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
    public SqlPage fetch(Connection connection, QueryId queryId, int cursorBufferSize) {
        try {
            ClientMessage requestMessage = SqlFetchCodec.encodeRequest(queryId, cursorBufferSize);
            ClientMessage responseMessage = invoke(requestMessage, connection);
            SqlFetchCodec.ResponseParameters responseParameters = SqlFetchCodec.decodeResponse(responseMessage);

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
    void close(Connection connection, QueryId queryId) {
        try {
            ClientMessage requestMessage = SqlCloseCodec.encodeRequest(queryId);

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
            ClientMessage requestMessage = SqlCloseCodec.encodeRequest(QueryId.create(UuidUtil.newSecureUUID()));

            int messageType = requestMessage.getMessageType();
            int messageTypeWithInvalidMethodId = (messageType & (~METHOD_ID_MASK)) | (INVALID_MESSAGE_ID << METHOD_ID_SHIFT);

            requestMessage.setMessageType(messageTypeWithInvalidMethodId);

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
            throw new HazelcastSqlException(error.getOriginatingMemberId(), error.getCode(), error.getMessage(), null);
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

    public static boolean isSqlMessage(int messageType) {
        int serviceId = (messageType & SERVICE_ID_MASK) >> SERVICE_ID_SHIFT;

        return serviceId == SQL_SERVICE_ID;
    }
}
