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

package com.hazelcast.sql.impl.state;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.jet.impl.util.NamedCompletableFuture;
import com.hazelcast.logging.ILogger;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.impl.AbstractSqlResult;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.ResultIterator;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * The state for SQL query submitted from a client. It's created
 * immediately after receiving a query. Later, when the job is submitted,
 * {@link #initResult(AbstractSqlResult)} is called, which is used to fetch
 * the rows.
 * <p>
 * When a member shuts down, it passes the request to {@link
 * AbstractSqlResult}, or, before the result is initialized, caches the
 * requests and forwards them to {@link AbstractSqlResult} when it's
 * initialized.
 * <p>
 * Before removing the client state from collection, {@link
 * #close(QueryException)} must be called.
 */
public class QueryClientState {

    private final Object lock = new Object();
    private final UUID clientId;
    private final QueryId queryId;
    private final ILogger logger;
    private final long createdAt;

    private volatile boolean closed;
    private final Map<UUID, CompletableFuture<Void>> shutdownFutures = new HashMap<>();
    private volatile AbstractSqlResult sqlResult;

    private volatile ResultIterator<SqlRow> iterator;

    public QueryClientState(@Nonnull UUID clientId, @Nonnull QueryId queryId, boolean closed, ILogger logger) {
        this.clientId = clientId;
        this.queryId = queryId;
        this.closed = closed;
        this.logger = logger;

        createdAt = System.nanoTime();
    }

    public UUID getClientId() {
        return clientId;
    }

    public QueryId getQueryId() {
        return queryId;
    }

    public boolean isClosed() {
        return closed;
    }

    @Nonnull
    public AbstractSqlResult getSqlResult() {
        if (sqlResult == null) {
            throw new HazelcastException("No result associated with this query");
        }
        return sqlResult;
    }

    /**
     * Adds the result reference to this query client state.
     *
     * @return true if success, false if it was closed.
     */
    public boolean initResult(@Nonnull AbstractSqlResult sqlResult) {
        synchronized (lock) {
            if (closed) {
                return false;
            }
            assert this.sqlResult == null : "Duplicate result";
            this.sqlResult = sqlResult;

            // Invoke `onParticipantGracefulShutdown()` on the actual job
            for (Entry<UUID, CompletableFuture<Void>> en : shutdownFutures.entrySet()) {
                boolean isParticipant = sqlResult.onParticipantGracefulShutdown(en.getKey());
                if (!isParticipant) {
                    en.getValue().complete(null);
                }
            }
        }
        return true;
    }

    public long getCreatedAtNano() {
        return createdAt;
    }

    public ResultIterator<SqlRow> getIterator() {
        assert sqlResult != null;

        if (iterator == null) {
            iterator = sqlResult.iterator();
        }

        return iterator;
    }

    @Nullable
    public CompletableFuture<Void> onGracefulParticipantShutdown(UUID memberId) {
        synchronized (lock) {
            if (closed) {
                return null;
            }
            if (sqlResult != null) {
                if (!sqlResult.onParticipantGracefulShutdown(memberId)) {
                    // if there is a result and the memberId isn't a participant, we don't have to wait for the completion.
                    return null;
                }
            } else {
                // If there is no result, we don't know. We have to wait. There can be a result later (see
                // initResult()). Or if there won't be a result for this query (e.g. a DDL query), we have no other
                // option - but these queries complete quickly anyway.
                // dummy statement to make checkstyle happy
                assert true;
            }

            return shutdownFutures.computeIfAbsent(memberId, x -> new NamedCompletableFuture<>("sql " + queryId));
        }
    }

    public void close(@Nullable QueryException exception) {
        synchronized (lock) {
            closed = true;
            // note that if the result was already initialized, this collection is empty
            for (CompletableFuture<Void> future : shutdownFutures.values()) {
                future.complete(null);
            }
            if (sqlResult != null) {
                sqlResult.close(exception);
            }
        }
    }
}
