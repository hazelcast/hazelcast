/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.impl.connection.ClientConnection;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.SqlCloseCodec;
import com.hazelcast.client.impl.protocol.codec.SqlExecuteCodec;
import com.hazelcast.client.impl.protocol.codec.SqlFetchCodec;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.client.impl.spi.impl.ClientInvocationFuture;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRowMetadata;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.SqlStatement;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.QueryUtils;
import com.hazelcast.sql.impl.SqlErrorCode;

import javax.annotation.Nonnull;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static com.hazelcast.internal.util.ExceptionUtil.withTryCatch;

/**
 * Client-side implementation of SQL service.
 */
public class SqlClientService implements SqlService {

    private static final int SERVICE_ID_MASK = 0x00FF0000;
    private static final int SERVICE_ID_SHIFT = 16;

    /** ID of the SQL beta service. Should match the ID declared in Sql.yaml */
    private static final int SQL_SERVICE_ID = 33;

    private final HazelcastClientInstanceImpl client;
    private final ILogger logger;

    public SqlClientService(HazelcastClientInstanceImpl client) {
        this.client = client;
        this.logger = client.getLoggingService().getLogger(getClass());
    }

    @Nonnull
    @Override
    public SqlResult execute(@Nonnull SqlStatement statement) {
        ClientConnection connection = client.getConnectionManager().getRandomConnection(true);

        if (connection == null) {
            throw rethrow(QueryException.error(
                SqlErrorCode.CONNECTION_PROBLEM,
                "Client must be connected to at least one data member to execute SQL queries"
            ));
        }

        QueryId id = QueryId.create(connection.getRemoteUuid());

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
                statement.getCursorBufferSize(),
                statement.getSchema(),
                SqlClientUtils.expectedResultTypeToByte(statement.getExpectedResultType()),
                id
            );

            SqlClientResult res = new SqlClientResult(
                this,
                connection,
                id,
                statement.getCursorBufferSize()
            );

            ClientInvocationFuture future = invokeAsync(requestMessage, connection);

            future.whenComplete(withTryCatch(logger,
                    (message, error) -> handleExecuteResponse(connection, res, message, error))).get();

            return res;
        } catch (Exception e) {
            throw rethrow(e, connection);
        }
    }

    private void handleExecuteResponse(
        ClientConnection connection,
        SqlClientResult res,
        ClientMessage message,
        Throwable error
    ) {
        if (error != null) {
            res.onExecuteError(rethrow(error, connection));

            return;
        }

        SqlExecuteCodec.ResponseParameters response = SqlExecuteCodec.decodeResponse(message);

        HazelcastSqlException responseError = handleResponseError(response.error);

        if (responseError != null) {
            res.onExecuteError(responseError);

            return;
        }

        res.onExecuteResponse(
            response.rowMetadata != null ? new SqlRowMetadata(response.rowMetadata) : null,
            response.rowPage,
            response.updateCount
        );
    }

    public void fetchAsync(Connection connection, QueryId queryId, int cursorBufferSize, SqlClientResult res) {
        ClientMessage requestMessage = SqlFetchCodec.encodeRequest(queryId, cursorBufferSize);

        ClientInvocationFuture future = invokeAsync(requestMessage, connection);

        future.whenComplete(withTryCatch(logger,
                (message, error) -> handleFetchResponse(connection, res, message, error)));
    }

    private void handleFetchResponse(Connection connection, SqlClientResult res, ClientMessage message, Throwable error) {
        if (error != null) {
            res.onFetchFinished(null, rethrow(error, connection));

            return;
        }

        SqlFetchCodec.ResponseParameters responseParameters = SqlFetchCodec.decodeResponse(message);

        HazelcastSqlException responseError = handleResponseError(responseParameters.error);

        if (responseError != null) {
            res.onFetchFinished(null, responseError);

            return;
        }

        assert responseParameters.rowPage != null;

        res.onFetchFinished(responseParameters.rowPage, null);
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
     * For testing only.
     */
    public Connection getRandomConnection() {
        Connection connection = client.getConnectionManager().getRandomConnection(false);

        if (connection == null) {
            throw rethrow(QueryException.error(
                SqlErrorCode.CONNECTION_PROBLEM,
                "Client is not connected to topology"
            ));
        }

        return connection;
    }

    /**
     * For testing only.
     */
    public ClientMessage invokeOnConnection(Connection connection, ClientMessage request) {
        try {
            return invoke(request, connection);
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

    Object deserializeRowValue(Object value) {
        try {
            return getSerializationService().toObject(value);
        } catch (Exception e) {
            throw rethrow(
                QueryException.error("Failed to deserialize query result value: " + e.getMessage())
            );
        }
    }

    public UUID getClientId() {
        return client.getLocalEndpoint().getUuid();
    }

    private InternalSerializationService getSerializationService() {
        return client.getSerializationService();
    }

    private ClientInvocationFuture invokeAsync(ClientMessage request, Connection connection) {
        ClientInvocation invocation = new ClientInvocation(client, request, null, connection);

        return invocation.invoke();
    }

    private ClientMessage invoke(ClientMessage request, Connection connection) throws Exception {
        ClientInvocationFuture fut = invokeAsync(request, connection);

        return fut.get();
    }

    private static HazelcastSqlException handleResponseError(SqlError error) {
        if (error != null) {
            return new HazelcastSqlException(error.getOriginatingMemberId(), error.getCode(), error.getMessage(), null);
        } else {
            return null;
        }
    }

    private RuntimeException rethrow(Throwable cause, Connection connection) {
        if (!connection.isAlive()) {
            return QueryUtils.toPublicException(
                QueryException.memberConnection(connection.getRemoteAddress()),
                getClientId()
            );
        }

        return rethrow(cause);
    }

    RuntimeException rethrow(Throwable cause) {
        // Make sure that AccessControlException is thrown as a top-level exception
        if (cause.getCause() instanceof AccessControlException) {
            return (AccessControlException) cause.getCause();
        }

        return QueryUtils.toPublicException(cause, getClientId());
    }

    public static boolean isSqlMessage(int messageType) {
        int serviceId = (messageType & SERVICE_ID_MASK) >> SERVICE_ID_SHIFT;

        return serviceId == SQL_SERVICE_ID;
    }
}
