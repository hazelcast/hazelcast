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

import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.QueryMetadata;
import com.hazelcast.sql.impl.QueryResultProducer;
import com.hazelcast.sql.impl.operation.QueryCheckOperation;
import com.hazelcast.sql.impl.operation.QueryOperationHandler;
import com.hazelcast.sql.impl.plan.Plan;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Query state implementation.
 */
public class QueryStateRegistry {
    /** IDs of locally started queries. */
    private final ConcurrentHashMap<QueryId, QueryState> states = new ConcurrentHashMap<>();

    /** Local member ID. */
    private UUID localMemberId;

    public void start(UUID localMemberId) {
        this.localMemberId = localMemberId;
    }

    public QueryState onInitiatorQueryStarted(
        long initiatorTimeout,
        Plan initiatorPlan,
        QueryMetadata initiatorMetadata,
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
            initiatorMetadata,
            initiatorResultProducer
        );

        if (register) {
            states.put(queryId, state);
        }

        return state;
    }

    public QueryState onDistributedQueryStarted(QueryId queryId, QueryStateCompletionCallback completionCallback) {
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
                    completionCallback
                );

                QueryState oldState = states.putIfAbsent(queryId, state);

                if (oldState != null) {
                    state = oldState;
                }
            }

            return state;
        }
    }

    public void complete(QueryId queryId) {
        states.remove(queryId);
    }

    public QueryState getState(QueryId queryId) {
        return states.get(queryId);
    }

    public void reset() {
        states.clear();
    }

    public void update(Collection<UUID> memberIds, QueryOperationHandler operationHandler) {
        Map<UUID, Collection<QueryId>> checkMap = new HashMap<>();

        for (QueryState state : states.values()) {
            // 1. Check if the query has timed out.
            if (state.tryCancelOnTimeout()) {
                continue;
            }

            // 2. Check whether the member required for the query has left.
            if (state.tryCancelOnMemberLeave(memberIds)) {
                continue;
            }

            // 3. Check whether the query is not initialized for too long. If yes, trigger check process.
            if (state.requestQueryCheck()) {
                QueryId queryId = state.getQueryId();

                checkMap.computeIfAbsent(queryId.getMemberId(), (key) -> new ArrayList<>(1)).add(queryId);
            }
        }

        // Send batched check requests.
        for (Map.Entry<UUID, Collection<QueryId>> checkEntry : checkMap.entrySet()) {
            QueryCheckOperation operation = new QueryCheckOperation(checkEntry.getValue());

            operationHandler.submit(checkEntry.getKey(), operation);
        }
    }
}
