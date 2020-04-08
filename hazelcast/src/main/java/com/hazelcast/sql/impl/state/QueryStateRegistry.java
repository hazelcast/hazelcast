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

    public void complete(QueryId queryId) {
        states.remove(queryId);
    }

    public QueryState getState(QueryId queryId) {
        return states.get(queryId);
    }

    public Collection<QueryState> getStates() {
        return states.values();
    }

    public void reset() {
        states.clear();
    }
}
