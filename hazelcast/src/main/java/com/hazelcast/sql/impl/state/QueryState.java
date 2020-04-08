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

import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.impl.ClockProvider;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.QueryMetadata;
import com.hazelcast.sql.impl.QueryResultProducer;
import com.hazelcast.sql.impl.plan.Plan;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.sql.impl.SqlInternalService.STATE_CHECK_FREQUENCY;

public final class QueryState implements QueryStateCallback {
    /** Query ID. */
    private final QueryId queryId;

    /** Completion guard. */
    private final AtomicBoolean completionGuard = new AtomicBoolean();

    /** Completion callback. */
    private final QueryStateCompletionCallback completionCallback;

    /** Initiator state. */
    private final QueryInitiatorState initiatorState;

    /** Distributed state. */
    private final QueryDistributedState distributedState = new QueryDistributedState();

    /** Clock provider. */
    private final ClockProvider clockProvider;

    /** Start time. */
    private final long startTime;

    /** Local member ID. */
    private final UUID localMemberId;

    /** Error which caused query completion. */
    private volatile QueryException completionError;

    /** Time when the a check was performed for the last time. */
    private volatile long checkTime;

    private QueryState(
        QueryId queryId,
        UUID localMemberId,
        QueryStateCompletionCallback completionCallback,
        boolean initiator,
        long initiatorTimeout,
        Plan initiatorPlan,
        QueryMetadata initiatorMetadata,
        QueryResultProducer initiatorRowSource,
        ClockProvider clockProvider
    ) {
        // Set common state.
        this.queryId = queryId;
        this.completionCallback = completionCallback;
        this.localMemberId = localMemberId;

        if (initiator) {
            initiatorState = new QueryInitiatorState(
                queryId,
                initiatorPlan,
                initiatorMetadata,
                initiatorRowSource,
                initiatorTimeout
            );
        } else {
            initiatorState = null;
        }

        this.clockProvider = clockProvider;

        startTime = clockProvider.currentTimeMillis();
        checkTime = startTime;
    }

    public static QueryState createInitiatorState(
        QueryId queryId,
        UUID localMemberId,
        QueryStateCompletionCallback completionCallback,
        long initiatorTimeout,
        Plan initiatorPlan,
        QueryMetadata initiatorMetadata,
        QueryResultProducer initiatorResultProducer,
        ClockProvider clockProvider
    ) {
        return new QueryState(
            queryId,
            localMemberId,
            completionCallback,
            true,
            initiatorTimeout,
            initiatorPlan,
            initiatorMetadata,
            initiatorResultProducer,
            clockProvider
        );
    }

    /**
     * Create distributed state when only query ID is known.
     *
     * @param queryId Query ID.
     * @return State or {@code null} if the state of the given query is known to be cleared up.
     */
    public static QueryState createDistributedState(
        QueryId queryId,
        UUID localMemberId,
        QueryStateCompletionCallback completionCallback,
        ClockProvider clockProvider
    ) {
        return new QueryState(
            queryId,
            localMemberId,
            completionCallback,
            false,
            -1,
            null,
            null,
            null,
            clockProvider
        );
    }

    public QueryId getQueryId() {
        return queryId;
    }

    public UUID getLocalMemberId() {
        return localMemberId;
    }

    public boolean isInitiator() {
        return initiatorState != null;
    }

    public QueryInitiatorState getInitiatorState() {
        return initiatorState;
    }

    public QueryDistributedState getDistributedState() {
        return distributedState;
    }

    @Override
    public void onFragmentFinished() {
        if (distributedState.onFragmentFinished()) {
            assert completionCallback != null;

            if (!completionGuard.compareAndSet(false, true)) {
                return;
            }

            completionCallback.onCompleted(queryId);
        }
    }

    @Override
    public void cancel(Exception error) {
        // Make sure that this thread changes the state.
        if (!completionGuard.compareAndSet(false, true)) {
            return;
        }

        // Wrap into common SQL exception if needed.
        if (!(error instanceof QueryException)) {
            error = QueryException.error(SqlErrorCode.GENERIC, error.getMessage(), error);
        }

        QueryException error0 = (QueryException) error;

        // Calculate the originating member.
        UUID originatingMemberId = error0.getOriginatingMemberId();

        if (originatingMemberId == null) {
            originatingMemberId = localMemberId;
        }

        // Determine members which should be notified.
        Collection<UUID> memberIds;

        if (isInitiator()) {
            // Cancel is performed on an initiator. Broadcast to all participants.
            memberIds = getParticipantsWithoutInitiator();
        } else {
            // Cancel is performed on a participant.
            if (error0.getOriginatingMemberId() == null) {
                // The cancel was just triggered. Propagate to the initiator.
                memberIds = Collections.singletonList(queryId.getMemberId());
            } else {
                // The cancel was propagated from coordinator. Do not notify again.
                memberIds = Collections.emptyList();
            }
        }

        // Invoke the completion callback.
        assert completionCallback != null;

        completionCallback.onError(
            queryId,
            error0.getCode(),
            error0.getMessage(),
            originatingMemberId,
            memberIds
        );

        completionError = error0;

        // If this is the initiator
        if (isInitiator()) {
            initiatorState.getResultProducer().onError(error0);
        }
    }

    public boolean tryCancelOnMemberLeave(Collection<UUID> memberIds) {
        Set<UUID> missingMemberIds;

        if (isInitiator()) {
            // Initiator checks the liveness of all query participants.
            missingMemberIds = getParticipantsWithoutInitiator();

            missingMemberIds.removeAll(memberIds);
        } else {
            // Participant checks only the liveness of the initiator.
             UUID initiatorMemberId = queryId.getMemberId();

             if (memberIds.contains(initiatorMemberId)) {
                 missingMemberIds = Collections.emptySet();
             } else {
                 missingMemberIds = memberIds.contains(initiatorMemberId)
                                        ? Collections.emptySet() : Collections.singleton(initiatorMemberId);
             }
        }

        // Cancel the query if some members are missing.
        if (missingMemberIds.isEmpty()) {
            return false;
        }

        QueryException error = QueryException.memberLeave(missingMemberIds);

        cancel(error);

        return true;
    }

    /**
     * Attempts to cancel the query if timeout has reached.
     */
    public boolean tryCancelOnTimeout() {
        if (!isInitiator()) {
            return false;
        }

        long timeout = initiatorState.getTimeout();

        if (timeout > 0 && clockProvider.currentTimeMillis() > startTime + timeout) {
            cancel(QueryException.timeout(timeout));

            return true;
        } else {
            return false;
        }
    }

    /**
     * Check if the query check is required for the given query.
     *
     * @return {@code true} if query check should be initiated, {@code false} otherwise.
     */
    public boolean requestQueryCheck() {
        // No need to check the query that has initiator state.
        if (isInitiator()) {
            return false;
        }

        // Distributed query is initialized, no need to worry.
        if (distributedState.isStarted()) {
            return false;
        }

        // Don't bother if the query is relatively recent.
        long currentTime = clockProvider.currentTimeMillis();

        if (currentTime - checkTime < STATE_CHECK_FREQUENCY * 2) {
            return false;
        }

        checkTime = currentTime;

        return true;
    }

    @Override
    public void checkCancelled() {
        QueryException completionError0 = completionError;

        if (completionError0 != null) {
            throw completionError0;
        }
    }

    /**
     * Get query participants. Should be invoked on initiator node only.
     *
     * @return Query participants.
     */
    public Set<UUID> getParticipantsWithoutInitiator() {
        assert isInitiator();

        Set<UUID> res = new HashSet<>(initiatorState.getPlan().getMemberIds());

        res.remove(queryId.getMemberId());

        return res;
    }
}
