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

package com.hazelcast.sql.impl.state;

import com.hazelcast.sql.SqlRowMetadata;
import com.hazelcast.sql.impl.ClockProvider;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.QueryResultProducer;
import com.hazelcast.sql.impl.plan.Plan;
import com.hazelcast.sql.impl.plan.cache.CachedPlanInvalidationCallback;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry that tracks active queries on a member.
 */
public class QueryStateRegistry {
    /** IDs of locally started queries. */
    private final ConcurrentHashMap<QueryId, QueryState> states = new ConcurrentHashMap<>();

    private final ClockProvider clockProvider;

    private volatile boolean shutdown;

    public QueryStateRegistry(ClockProvider clockProvider) {
        this.clockProvider = clockProvider;
    }

    /**
     * Registers a query on the initiator member before the query is started on participants.
     */
    public QueryState onInitiatorQueryStarted(
        QueryId queryId,
        UUID localMemberId,
        long initiatorTimeout,
        Plan initiatorPlan,
        CachedPlanInvalidationCallback initiatorPlanInvalidationCallback,
        SqlRowMetadata initiatorRowMetadata,
        QueryResultProducer initiatorResultProducer,
        QueryStateCompletionCallback completionCallback
    ) {
        QueryState state = QueryState.createInitiatorState(
            queryId,
            localMemberId,
            completionCallback,
            initiatorTimeout,
            initiatorPlan,
            initiatorPlanInvalidationCallback,
            initiatorRowMetadata,
            initiatorResultProducer,
            clockProvider
        );

        states.put(queryId, state);

        if (shutdown) {
            // No members or fragments observed the state so far. So we just remove it from map and throw the proper exception.
            states.remove(queryId);

            throw shutdownException();
        }

        return state;
    }

    /**
     * Registers a distributed query in response to query start message or query batch message.
     * <p>
     * The method is guaranteed to be invoked after initiator state is created on the initiator member.
     * <p>
     * It is possible that the method will be invoked after the query is declared completed. For example. a batch
     * may arrive from the remote concurrently after query cancellation, because there is no distributed coordination
     * of these events. This is not a problem, because {@link QueryStateRegistryUpdater} will eventually detect that
     * the query is not longer active on the initiator member.
     *
     * @param localMemberId cached local member ID
     * @param queryId query ID
     * @param completionCallback callback that will be invoked when the query is completed
     * @param cancelled if the query should be created in the cancelled state
     * @return query state or {@code null} if the query with the given ID is guaranteed to be already completed
     */
    public QueryState onDistributedQueryStarted(
        UUID localMemberId,
        QueryId queryId,
        QueryStateCompletionCallback completionCallback,
        boolean cancelled
    ) {
        QueryState state = onDistributedQueryStarted0(localMemberId, queryId, completionCallback, cancelled);

        if (state != null) {
            state.updateLastActivityTime();
        }

        return state;
    }

    private QueryState onDistributedQueryStarted0(
        UUID localMemberId,
        QueryId queryId,
        QueryStateCompletionCallback completionCallback,
        boolean cancelled
    ) {
        UUID initiatorMemberId = queryId.getMemberId();

        boolean local = localMemberId.equals(initiatorMemberId);

        if (local) {
            // For locally initiated query, the state must already exist. If not - the query has been completed.
            return states.get(queryId);
        } else {
            /// Otherwise either get an existing state, or create it on demand.
            QueryState state = states.get(queryId);

            if (state == null) {
                state = QueryState.createDistributedState(
                    queryId,
                    localMemberId,
                    completionCallback,
                    cancelled,
                    clockProvider
                );

                QueryState oldState = states.putIfAbsent(queryId, state);

                if (oldState != null) {
                    state = oldState;
                }

                if (shutdown) {
                    cancelOnShutdown(state);

                    return null;
                }
            }

            return state;
        }
    }

    /**
     * Invoked from the {@link QueryStateCompletionCallback} when the execution of the query is completed.
     *
     * @param queryId Query ID.
     */
    public void onQueryCompleted(QueryId queryId) {
        states.remove(queryId);
    }

    public void shutdown() {
        // Set shutdown flag.
        shutdown = true;

        // Cancel active queries.
        for (QueryState state : states.values()) {
            cancelOnShutdown(state);
        }
    }

    public QueryState getState(QueryId queryId) {
        return states.get(queryId);
    }

    public Collection<QueryState> getStates() {
        return states.values();
    }

    private static void cancelOnShutdown(QueryState state) {
        state.cancel(shutdownException(), true);
    }

    private static QueryException shutdownException() {
        return QueryException.error("SQL query has been cancelled due to member shutdown");
    }
}
