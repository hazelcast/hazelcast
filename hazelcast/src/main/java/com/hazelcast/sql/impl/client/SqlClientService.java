/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.internal.nio.ConnectionType;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.util.collection.ReadOptimizedLruCache;
import com.hazelcast.logging.ILogger;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRowMetadata;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.SqlStatement;
import com.hazelcast.sql.impl.CoreQueryUtils;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.QueryId;

import javax.annotation.Nonnull;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hazelcast.client.properties.ClientProperty.INVOCATION_RETRY_PAUSE_MILLIS;
import static com.hazelcast.client.properties.ClientProperty.INVOCATION_TIMEOUT_SECONDS;
import static com.hazelcast.client.properties.ClientProperty.PARTITION_ARGUMENT_CACHE_SIZE;
import static com.hazelcast.internal.util.ConcurrencyUtil.CALLER_RUNS;
import static com.hazelcast.internal.util.ExceptionUtil.withTryCatch;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.jet.impl.util.LoggingUtil.logFinest;
import static com.hazelcast.sql.impl.SqlErrorCode.CONNECTION_PROBLEM;
import static com.hazelcast.sql.impl.SqlErrorCode.PARTITION_DISTRIBUTION;
import static com.hazelcast.sql.impl.SqlErrorCode.RESTARTABLE_ERROR;
import static com.hazelcast.sql.impl.SqlErrorCode.TOPOLOGY_CHANGE;

/**
 * Client-side implementation of SQL service.
 */
public class SqlClientService implements SqlService {
    private static final int MAX_FAST_INVOCATION_COUNT = 5;

    @SuppressWarnings("checkstyle:VisibilityModifier")
    public final ReadOptimizedLruCache<String, Integer> partitionArgumentIndexCache;

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
    private final boolean isSmartRouting;

    public SqlClientService(HazelcastClientInstanceImpl client) {
        this.client = client;
        this.logger = client.getLoggingService().getLogger(getClass());
        this.skipUpdateStatistics = skipUpdateStatistics();
        long resubmissionTimeoutMillis = client.getProperties().getPositiveMillisOrDefault(INVOCATION_TIMEOUT_SECONDS);
        this.resubmissionTimeoutNano = TimeUnit.MILLISECONDS.toNanos(resubmissionTimeoutMillis);
        this.resubmissionRetryPauseMillis = client.getProperties().getPositiveMillisOrDefault(INVOCATION_RETRY_PAUSE_MILLIS);

        this.isSmartRouting = !client.getConnectionManager().isUnisocketClient();
        final int partitionArgCacheSize = client.getProperties().getInteger(PARTITION_ARGUMENT_CACHE_SIZE);
        final int partitionArgCacheThreshold = partitionArgCacheSize + Math.min(partitionArgCacheSize / 10, 50);
        this.partitionArgumentIndexCache = new ReadOptimizedLruCache<>(partitionArgCacheSize, partitionArgCacheThreshold);
    }

    @Nonnull
    @Override
    public SqlResult execute(@Nonnull SqlStatement statement) {
        Integer argIndex = statement.getPartitionArgumentIndex() != -1
                ? statement.getPartitionArgumentIndex()
                : partitionArgumentIndexCache.getOrDefault(statement.getSql(), -1);
        Integer partitionId = extractPartitionId(statement, argIndex);
        ClientConnection connection = partitionId != null
                ? getQueryConnection(partitionId)
                : getQueryConnection();
        QueryId id = QueryId.create(connection.getRemoteUuid());

        List<Object> params = statement.getParameters();
        List<Data> params0 = new ArrayList<>(params.size());

        for (Object param : params) {
            params0.add(serializeParameter(param));
        }

        Function<QueryId, ClientMessage> requestMessageSupplier = (queryId) -> SqlExecuteCodec.encodeRequest(
                statement.getSql(),
                params0,
                statement.getTimeoutMillis(),
                statement.getCursorBufferSize(),
                statement.getSchema(),
                statement.getExpectedResultType().getId(),
                queryId,
                skipUpdateStatistics
        );
        ClientMessage requestMessage = requestMessageSupplier.apply(id);

        SqlClientResult res = new SqlClientResult(
                this,
                connection,
                id,
                statement.getCursorBufferSize(),
                requestMessageSupplier,
                statement
        );

        try {
            ClientMessage message = invoke(requestMessage, connection);
            handleExecuteResponse(statement, argIndex, res, message);
            return res;
        } catch (Exception e) {
            RuntimeException error = rethrow(e, connection);
            SqlResubmissionResult resubmissionResult = resubmitIfPossible(res, error);
            if (resubmissionResult == null) {
                throw error;
            }
            res.onResubmissionResponse(resubmissionResult);
            return res;
        }
    }

    SqlResubmissionResult resubmitIfPossible(SqlClientResult result, RuntimeException error) {
        if (!shouldResubmit(error) || !shouldResubmit(result)) {
            return null;
        }

        SqlResubmissionResult resubmissionResult = resubmitIfPossible0(result, error);
        if (resubmissionResult.getSqlError() != null) {
            SqlError sqlError = resubmissionResult.getSqlError();
            throw new HazelcastSqlException(
                    sqlError.getOriginatingMemberId(),
                    sqlError.getCode(),
                    sqlError.getMessage(),
                    null,
                    sqlError.getSuggestion()
            );
        }
        return resubmissionResult;
    }

    @SuppressWarnings({"BusyWait", "checkstyle:cyclomaticcomplexity", "checkstyle:MagicNumber"})
    private SqlResubmissionResult resubmitIfPossible0(SqlClientResult result, RuntimeException error) {
        long resubmissionStartTime = System.nanoTime();
        int invokeCount = 0;

        SqlResubmissionResult resubmissionResult = null;
        do {
            ClientConnection connection = null;
            try {
                connection = getQueryConnection();
                QueryId queryId = QueryId.create(connection.getRemoteUuid());
                logFinest(logger, "Resubmitting query: %s with new query id %s", result.getQueryId(), queryId);
                result.setQueryId(queryId);
                ClientMessage message = invoke(result.getSqlExecuteMessage(queryId), connection);
                resubmissionResult = createResubmissionResult(message, connection);
                if (resubmissionResult.getSqlError() == null) {
                    logFinest(logger, "Resubmitting query: %s ended without error", result.getQueryId());
                } else {
                    logFinest(logger, "Resubmitting query: %s ended with error", result.getQueryId());
                }
                if (resubmissionResult.getSqlError() == null || !shouldResubmit(resubmissionResult.getSqlError())) {
                    return resubmissionResult;
                }
            } catch (Exception e) {
                logFinest(logger, "Resubmitting query: %s ended with exception", result.getQueryId());
                RuntimeException rethrown = connection == null ? (RuntimeException) e : rethrow(e, connection);
                if (!shouldResubmit(rethrown)) {
                    throw rethrown;
                }
            }

            // We measure the retry count and retry timeout for each individual resubmission. If a resubmission succeeds,
            // and then later some fetch of the same query fails, we'll try to resubmit again, and we'll not include
            // the number and time spent in the previous resubmission.
            if (invokeCount++ >= MAX_FAST_INVOCATION_COUNT) {
                long delayMillis =
                        Math.min(1L << Math.min(62, invokeCount - MAX_FAST_INVOCATION_COUNT), resubmissionRetryPauseMillis);
                try {
                    Thread.sleep(delayMillis);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return returnNonNullOrThrow(resubmissionResult, error, true);
                }
            }
        } while (System.nanoTime() - resubmissionStartTime <= resubmissionTimeoutNano);

        logger.finest("Resubmitting query timed out");

        return returnNonNullOrThrow(resubmissionResult, error, false);
    }

    private SqlResubmissionResult returnNonNullOrThrow(
            SqlResubmissionResult result,
            RuntimeException originalError,
            boolean wasInterrupted
    ) {
        if (result == null) {
            // We have nothing to return to the caller, no valid result and no new exception, so we throw the original error
            // packed in proper exception so the caller has valid information what happened.
            if (wasInterrupted) {
                throw new HazelcastException("Query resubmission was interrupted", originalError);
            }
            throw new OperationTimeoutException("Query resubmission timed out", originalError);
        }
        return result;
    }

    private boolean shouldResubmit(Exception error) {
        return (error instanceof HazelcastSqlException) && (shouldResubmit(((HazelcastSqlException) error).getCode()));
    }

    private boolean shouldResubmit(SqlError error) {
        return shouldResubmit(error.getCode());
    }

    private boolean shouldResubmit(int errorCode) {
        return errorCode == CONNECTION_PROBLEM || errorCode == PARTITION_DISTRIBUTION
                || errorCode == TOPOLOGY_CHANGE || errorCode == RESTARTABLE_ERROR;
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

    private SqlResubmissionResult createResubmissionResult(ClientMessage message, ClientConnection connection) {
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
            SqlStatement statement,
            int originalPartitionArgumentIndex,
            SqlClientResult res,
            ClientMessage message
    ) {
        SqlExecuteCodec.ResponseParameters response = SqlExecuteCodec.decodeResponse(message);
        SqlError sqlError = response.error;
        if (sqlError != null) {
            Throwable cause = null;
            if (sqlError.isCauseStackTraceExists()) {
                cause = new Exception(sqlError.getCauseStackTrace());
            }
            throw new HazelcastSqlException(
                    sqlError.getOriginatingMemberId(),
                    sqlError.getCode(),
                    sqlError.getMessage(),
                    cause,
                    sqlError.getSuggestion()
            );
        } else {
            if (isSmartRouting && response.partitionArgumentIndex != originalPartitionArgumentIndex) {
                if (response.partitionArgumentIndex != -1) {
                    partitionArgumentIndexCache.put(statement.getSql(), response.partitionArgumentIndex);
                    // We're writing to a non-volatile field from multiple threads. But it's safe because all
                    // writers write the same value, so it should be idempotent. Also, another thread might
                    // not observe the written value, but that's not an issue either, the thread will fall
                    // back to using query the partitionArgumentIndexCache, unless there's a change
                    // from one argument to another, which can happen only if there's concurrent DDL.
                    statement.setPartitionArgumentIndex(response.partitionArgumentIndex);
                } else {
                    partitionArgumentIndexCache.remove(statement.getSql());
                }
            }
            res.onExecuteResponse(
                    response.rowMetadata != null ? new SqlRowMetadata(response.rowMetadata) : null,
                    response.rowPage,
                    response.updateCount,
                    response.isIsInfiniteRowsExists ? response.isInfiniteRows : null
            );
        }
    }

    public void fetchAsync(ClientConnection connection, QueryId queryId, int cursorBufferSize, SqlClientResult res) {
        ClientMessage requestMessage = SqlFetchCodec.encodeRequest(queryId, cursorBufferSize);

        ClientInvocationFuture future = invokeAsync(requestMessage, connection);

        future.whenCompleteAsync(withTryCatch(logger,
                (message, error) -> handleFetchResponse(connection, res, message, error)), CALLER_RUNS);
    }

    private void handleFetchResponse(ClientConnection connection, SqlClientResult res, ClientMessage message, Throwable error) {
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
    void close(ClientConnection connection, QueryId queryId) {
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

    public ClientConnection getQueryConnection(int partitionId) {
        try {
            final UUID nodeId = client.getClientPartitionService().getPartitionOwner(partitionId);
            if (nodeId == null) {
                return getQueryConnection();
            }

            ClientConnection connection = client.getConnectionManager().getConnection(nodeId);

            if (connection == null) {
                return getQueryConnection();
            }

            return connection;
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    /**
     * For testing only.
     */
    public ClientMessage invokeOnConnection(ClientConnection connection, ClientMessage request) {
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

    private ClientInvocationFuture invokeAsync(ClientMessage request, ClientConnection connection) {
        ClientInvocation invocation = new ClientInvocation(client, request, null, connection);

        return invocation.invoke();
    }

    private ClientMessage invoke(ClientMessage request, ClientConnection connection) throws Exception {
        ClientInvocationFuture fut = invokeAsync(request, connection);

        return fut.get();
    }

    private Integer extractPartitionId(SqlStatement statement, int argIndex) {
        if (!isSmartRouting) {
            return null;
        }

        if (statement.getParameters().isEmpty()) {
            return null;
        }

        if (argIndex >= statement.getParameters().size() || argIndex < 0) {
            return null;
        }

        final Object key = statement.getParameters().get(argIndex);
        if (key == null) {
            return null;
        }

        return client.getClientPartitionService().getPartitionId(key);
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

    private RuntimeException rethrow(Throwable cause, ClientConnection connection) {
        if (!connection.isAlive()) {
            return CoreQueryUtils.toPublicException(
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

        return CoreQueryUtils.toPublicException(cause, getClientId());
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
