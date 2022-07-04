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
import static com.hazelcast.sql.impl.SqlErrorCode.CONNECTION_PROBLEM;
import static com.hazelcast.sql.impl.SqlErrorCode.PARTITION_DISTRIBUTION;
import static com.hazelcast.sql.impl.SqlErrorCode.TOPOLOGY_CHANGE;

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
    private final long resubmissionTimeoutNano;
    private final long resubmissionRetryPauseMillis;

    public SqlClientService(HazelcastClientInstanceImpl client) {
        this.client = client;
        this.logger = client.getLoggingService().getLogger(getClass());
        this.skipUpdateStatistics = skipUpdateStatistics();
        long resubmissionTimeoutMillis = client.getProperties().getPositiveMillisOrDefault(INVOCATION_TIMEOUT_SECONDS);
        this.resubmissionTimeoutNano = TimeUnit.MILLISECONDS.toNanos(resubmissionTimeoutMillis);
        this.resubmissionRetryPauseMillis = client.getProperties().getPositiveMillisOrDefault(INVOCATION_RETRY_PAUSE_MILLIS);
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
                requestMessage,
                statement
        );

        try {
            ClientMessage message = invoke(requestMessage, connection);
            handleExecuteResponse(res, message);
            return res;
        } catch (Exception e) {
            RuntimeException error = rethrow(e, connection);
            SqlResubmissionResult resubmissionResult = resubmitIfPossible(res, error);
            if (resubmissionResult == null) {
                res.onExecuteError(error);
                throw error;
            }
            handleExecuteResubmittedResponse(res, resubmissionResult);
            return res;
        }
    }

    @SuppressWarnings("BusyWait")
    SqlResubmissionResult resubmitIfPossible(SqlClientResult result, RuntimeException error) {
        if (!shouldResubmit(error) || !shouldResubmit(result)) {
            return null;
        }

        long resubmissionStartTime = System.nanoTime();
        int invokeCount = 0;

        SqlResubmissionResult resubmissionResult = null;
        do {
            logger.info("Resubmitting query: " + result.getQueryId());
            ClientConnection connection = null;
            try {
                connection = getQueryConnection();
                ClientMessage message = invoke(result.getSqlExecuteMessage(), connection);
                resubmissionResult = createResubmissionResult(message, connection);
                if (resubmissionResult.getSqlError() == null) {
                    logger.info("Resubmitting query: " + result.getQueryId() + " ended without error");
                } else {
                    logger.info("Resubmitting query: " + result.getQueryId() + " ended with error");
                }
                if (resubmissionResult.getSqlError() == null || !shouldResubmit(resubmissionResult.getSqlError())) {
                    return resubmissionResult;
                }
            } catch (Exception e) {
                logger.info("Resubmitting query: " + result.getQueryId() + " ended with exception");
                RuntimeException rethrown = connection == null ? (RuntimeException) e : rethrow(e, connection);
                if (!shouldResubmit(rethrown)) {
                    throw rethrown;
                }
            }

            if (invokeCount++ >= MAX_FAST_INVOCATION_COUNT) {
                long delayMillis = Math.min(1L << (invokeCount - MAX_FAST_INVOCATION_COUNT), resubmissionRetryPauseMillis);
                try {
                    Thread.sleep(delayMillis);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return resubmissionResult;
                }
            }
        } while (System.nanoTime() - resubmissionStartTime <= resubmissionTimeoutNano);

        logger.info("Resubmitting query timed out");

        return resubmissionResult;
    }

    private boolean shouldResubmit(Exception error) {
        return (error instanceof HazelcastSqlException) && (shouldResubmit(((HazelcastSqlException) error).getCode()));
    }

    private boolean shouldResubmit(SqlError error) {
        return shouldResubmit(error.getCode());
    }

    private boolean shouldResubmit(int errorCode) {
        return errorCode == CONNECTION_PROBLEM || errorCode == PARTITION_DISTRIBUTION || errorCode == TOPOLOGY_CHANGE;
    }

    private boolean shouldResubmit(SqlClientResult result) {
        ClientSqlResubmissionMode resubmissionMode = client.getClientConfig().getSqlConfig().getResubmissionMode();
        switch (resubmissionMode) {
            case NEVER:
                return false;
            case RETRY_SELECTS:
                return result.isSelectQuery() && !result.isReturnedAnyResult();
            case RETRY_SELECTS_ALLOW_DUPLICATES:
                return result.isSelectQuery();
            case RETRY_ALL:
                return true;
            default:
                throw new IllegalStateException("Unknown resubmission mode: " + resubmissionMode);
        }
    }

    private boolean skipUpdateStatistics() {
        String connectionType = client.getConnectionManager().getConnectionType();
        return connectionType.equals(ConnectionType.MC_JAVA_CLIENT);
    }

    private SqlResubmissionResult createResubmissionResult(ClientMessage message, Connection connection) {
        SqlExecuteCodec.ResponseParameters response = SqlExecuteCodec.decodeResponse(message);
        SqlError sqlError = response.error;
        if (sqlError != null) {
            return new SqlResubmissionResult(sqlError);
        } else {
            SqlRowMetadata rowMetadata = response.rowMetadata != null ? new SqlRowMetadata(response.rowMetadata) : null;
            return new SqlResubmissionResult(connection, rowMetadata, response.rowPage, response.updateCount);
        }
    }

    private void handleExecuteResponse(SqlClientResult res, ClientMessage message) {
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

    private void handleExecuteResubmittedResponse(SqlClientResult res, SqlResubmissionResult resubmissionResult) {
        if (resubmissionResult.getSqlError() != null) {
            SqlError sqlError = resubmissionResult.getSqlError();
            throw new HazelcastSqlException(
                    sqlError.getOriginatingMemberId(),
                    sqlError.getCode(),
                    sqlError.getMessage(),
                    null,
                    sqlError.getSuggestion()
            );
        } else {
            res.onResubmissionResponse(resubmissionResult);
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
                throw rethrow(QueryException.error(CONNECTION_PROBLEM, "Client is not connected"));
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
