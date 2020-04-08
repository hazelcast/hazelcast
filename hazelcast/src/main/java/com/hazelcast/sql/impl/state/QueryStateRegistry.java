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

package com.hazelcast.sql.impl.state;

import com.hazelcast.sql.impl.ClockProvider;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.QueryResultProducer;
import com.hazelcast.sql.impl.plan.Plan;

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

    public QueryStateRegistry(ClockProvider clockProvider) {
        this.clockProvider = clockProvider;
    }

    /**
     * Registers a query on the initiator member before the query is started on participants.
     *
     * @param localMemberId Cache local member ID.
     * @param initiatorTimeout Query timeout.
     * @param initiatorPlan Query plan.
     * @param initiatorResultProducer An object that will produce final query results.
     * @param completionCallback Callback that will be invoked when the query is completed.
     * @param register Whether th query should be registered. {@code true} for distributed queries, {@code false} for queries
     *                 that return predefined values, e.g. EXPLAIN.
     * @return Query state.
     */
    public QueryState onInitiatorQueryStarted(
        UUID localMemberId,
        long initiatorTimeout,
        Plan initiatorPlan,
        QueryResultProducer initiatorResultProducer,
        QueryStateCompletionCallback completionCallback,
        boolean register
    ) {
        QueryId queryId = QueryId.create(localMemberId);

        QueryState state = QueryState.createInitiatorState(
            queryId,
            localMemberId,
            completionCallback,
            initiatorTimeout,
            initiatorPlan,
            initiatorResultProducer,
            clockProvider
        );

        if (register) {
            states.put(queryId, state);
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
     * @param localMemberId Cache local member ID.
     * @param queryId Query ID.
     * @param completionCallback Callback that will be invoked when the query is completed.
     * @return Query state or {@code null} if the query with the given ID is guaranteed to be already completed.
     */
    public QueryState onDistributedQueryStarted(
        UUID localMemberId,
        QueryId queryId,
        QueryStateCompletionCallback completionCallback
    ) {
        UUID initiatorMemberId =  queryId.getMemberId();

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
                    clockProvider
                );

                QueryState oldState = states.putIfAbsent(queryId, state);

                if (oldState != null) {
                    state = oldState;
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

    /**
     * Clears the registry. The method is called in case of recovery from the split brain.
     * <p>
     *  No additional precautions (such as forceful completion of already running queries) are needed, because a new ID
     *  is assigned to the local member, and a member with the previous ID is declared dead. As a result,
     *  {@link QueryStateRegistryUpdater} will detect that old queries have missing members, and will cancel them.
     */
    public void reset() {
        states.clear();
    }

    public QueryState getState(QueryId queryId) {
        return states.get(queryId);
    }

    public Collection<QueryState> getStates() {
        return states.values();
    }
}
