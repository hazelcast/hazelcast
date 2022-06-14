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

package com.hazelcast.sql.impl.client;

import com.hazelcast.client.config.ClientSqlResubmissionMode;
import com.hazelcast.client.impl.ClientDelegatingFuture;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.connection.ClientConnection;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.SqlCloseCodec;
import com.hazelcast.client.impl.protocol.codec.SqlExecuteCodec;
import com.hazelcast.client.impl.protocol.codec.SqlFetchCodec;
import com.hazelcast.client.impl.protocol.codec.SqlMappingDdlCodec;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.client.impl.spi.impl.ClientInvocationFuture;
import com.hazelcast.cluster.Member;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.ConnectionType;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.client.properties.ClientProperty.INVOCATION_RETRY_PAUSE_MILLIS;
import static com.hazelcast.client.properties.ClientProperty.INVOCATION_TIMEOUT_SECONDS;
import static com.hazelcast.internal.util.ExceptionUtil.withTryCatch;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Client-side implementation of SQL service.
 */
public class SqlClientService implements SqlService {
    private static final int MAX_FAST_INVOCATION_COUNT = 5;

    private final HazelcastClientInstanceImpl client;
    private final ILogger logger;

    /**
     * The field to indicate whether a query should update phone home statistics or not.
     * For example, the queries issued from the MC client will not update the statistics
     * because they cause a significant distortion.
     */
    private final boolean skipUpdateStatistics;
    private final long invocationTimeoutNano;
    private final long retryPauseMillis;

    public SqlClientService(HazelcastClientInstanceImpl client) {
        this.client = client;
        this.logger = client.getLoggingService().getLogger(getClass());
        this.skipUpdateStatistics = skipUpdateStatistics();
        long invocationTimeoutMillis = client.getProperties().getPositiveMillisOrDefault(INVOCATION_TIMEOUT_SECONDS);
        this.invocationTimeoutNano = TimeUnit.MILLISECONDS.toNanos(invocationTimeoutMillis);
        this.retryPauseMillis = client.getProperties().getPositiveMillisOrDefault(INVOCATION_RETRY_PAUSE_MILLIS);
    }

    @Nonnull
    @Override
    public SqlResult execute(@Nonnull SqlStatement statement) {
        ClientConnection connection = getQueryConnection();
        QueryId id = QueryId.create(connection.getRemoteUuid());

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
                statement.getExpectedResultType().getId(),
                id,
                skipUpdateStatistics
        );

        SqlClientResult res = new SqlClientResult(
                this,
                connection,
                id,
                statement.getCursorBufferSize(),
                new SqlResubmissionContext(requestMessage, statement)
        );

        try {
            ClientMessage message = invoke(requestMessage, connection);
            handleExecuteResponse(res, message);
            return res;
        } catch (Exception e) {
            RuntimeException error = rethrow(e, connection);
            res.onExecuteError(error);
            throw error;
        }
    }

    @SuppressWarnings("BusyWait")
    SqlResubmissionResult resubmitIfNeeded(SqlClientResult res, RuntimeException error) {
        if (!(error instanceof HazelcastSqlException) || !shouldResubmit(res)) {
            return null;
        }

        long resubmissionStartTime = System.nanoTime();
        int invokeCount = 0;

        ClientConnection connection = getQueryConnection();
        do {
            logger.info("Resubmitting query");
            try {
                ClientMessage message = invoke(res.getResubmissionContext().getRequestMessage(), connection);
                return createResubmissionResult(res, message, connection);
            } catch (Exception ignored) {
                logger.info("Exception while resubmission, ignoring");
            }

            if (invokeCount++ >= MAX_FAST_INVOCATION_COUNT) {
                long delayMillis = Math.min(1L << (invokeCount - MAX_FAST_INVOCATION_COUNT), retryPauseMillis);
                try {
                    Thread.sleep(delayMillis);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return null;
                }
            }
        } while (System.nanoTime() - resubmissionStartTime <= invocationTimeoutNano);

        return null;
    }

    private boolean shouldResubmit(SqlClientResult res) {
        SqlResubmissionContext resubmissionContext = res.getResubmissionContext();
        ClientSqlResubmissionMode resubmissionMode = client.getClientConfig().getSqlResubmissionMode();
        switch (resubmissionMode) {
            case NEVER:
                return false;
            case RETRY_SELECTS:
                return resubmissionContext.isSelectQuery() && !res.isReturnedAnyResult();
            case RETRY_SELECTS_ALLOW_DUPLICATES:
                return resubmissionContext.isSelectQuery();
            case RETRY_ALL:
                return true;
        }
        throw new IllegalStateException("Unknown resubmission mode: " + resubmissionMode);
    }

    private boolean skipUpdateStatistics() {
        String connectionType = client.getConnectionManager().getConnectionType();
        return connectionType.equals(ConnectionType.MC_JAVA_CLIENT);
    }

    private SqlResubmissionResult createResubmissionResult(SqlClientResult res, ClientMessage message, Connection connection) {
        SqlExecuteCodec.ResponseParameters response = SqlExecuteCodec.decodeResponse(message);
        SqlError sqlError = response.error;
        if (sqlError != null) {
            return new SqlResubmissionResult(sqlError);
        } else {
            SqlRowMetadata rowMetadata = response.rowMetadata != null ? new SqlRowMetadata(response.rowMetadata) : null;
            return new SqlResubmissionResult(connection, rowMetadata, response.rowPage, response.updateCount);
        }
    }

    private void handleExecuteResponse(
            SqlClientResult res,
            ClientMessage message
    ) {
        SqlExecuteCodec.ResponseParameters response = SqlExecuteCodec.decodeResponse(message);
        SqlError sqlError = response.error;
        if (sqlError != null) {
            throw new HazelcastSqlException(
                    sqlError.getOriginatingMemberId(),
                    sqlError.getCode(),
                    sqlError.getMessage(),
                    null,
                    sqlError.getSuggestion()
            );
        } else {
            res.onExecuteResponse(
                    response.rowMetadata != null ? new SqlRowMetadata(response.rowMetadata) : null,
                    response.rowPage,
                    response.updateCount
            );
        }
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
     * @param queryId    Query ID.
     */
    void close(Connection connection, QueryId queryId) {
        try {
            ClientMessage requestMessage = SqlCloseCodec.encodeRequest(queryId);

            invoke(requestMessage, connection);
        } catch (Exception e) {
            throw rethrow(e, connection);
        }
    }

    // public for testing only
    public ClientConnection getQueryConnection() {
        try {
            ClientConnection connection = client.getConnectionManager().getConnectionForSql();

            if (connection == null) {
                throw rethrow(QueryException.error(SqlErrorCode.CONNECTION_PROBLEM, "Client is not connected"));
            }

            return connection;
        } catch (Exception e) {
            throw rethrow(e);
        }
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

    public UUID getClientId() {
        return client.getLocalEndpoint().getUuid();
    }

    InternalSerializationService getSerializationService() {
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
            return new HazelcastSqlException(
                    error.getOriginatingMemberId(),
                    error.getCode(),
                    error.getMessage(),
                    null,
                    error.getSuggestion()
            );
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

    /**
     * Gets a SQL Mapping suggestion for the given IMap name.
     * <p>
     * Used by Management Center.
     */
    @Nonnull
    public CompletableFuture<String> mappingDdl(Member member, String mapName) {
        checkNotNull(mapName);

        ClientInvocation invocation = new ClientInvocation(client, SqlMappingDdlCodec.encodeRequest(mapName),
                null, member.getUuid());

        return new ClientDelegatingFuture<>(invocation.invoke(), client.getSerializationService(),
                SqlMappingDdlCodec::decodeResponse);
    }
}
