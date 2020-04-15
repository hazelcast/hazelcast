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
import com.hazelcast.sql.impl.QueryResultProducer;
import com.hazelcast.sql.impl.plan.Plan;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

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
            clockProvider
        );
    }

    public QueryId getQueryId() {
        return queryId;
    }

    public UUID getLocalMemberId() {
        return localMemberId;
    }

    public long getStartTime() {
        return startTime;
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

        QueryException error0 = prepareCancelError(error);

        // Determine members which should be notified.
        Collection<UUID> memberIds;

        if (isInitiator()) {
            // Cancel is performed on an initiator. Broadcast to all participants.
            memberIds = new HashSet<>(getParticipants());
            memberIds.remove(localMemberId);
        } else {
            boolean isLocal = error0.getOriginatingMemberId().equals(localMemberId);

            if (isLocal) {
                // The cancel has been triggered locally. Notify initiator.
                memberIds = Collections.singletonList(queryId.getMemberId());
            } else {
                // The cancel has been triggered remotely. No need to propagate it.
                memberIds = Collections.emptyList();
            }
        }

        // Invoke the completion callback.
        assert completionCallback != null;

        completionCallback.onError(
            queryId,
            error0.getCode(),
            error0.getMessage(),
            error0.getOriginatingMemberId(),
            memberIds
        );

        completionError = error0;

        // If this is the initiator
        if (isInitiator()) {
            initiatorState.getResultProducer().onError(error0);
        }
    }

    private QueryException prepareCancelError(Exception error) {
        if (error instanceof QueryException) {
            QueryException error0 = (QueryException) error;

            if (error0.getOriginatingMemberId() == null) {
                error0 = QueryException.error(error0.getCode(), error0.getMessage(), error0.getCause(), localMemberId);
            }

            return error0;
        } else {
            return QueryException.error(SqlErrorCode.GENERIC, error.getMessage(), error, localMemberId);
        }
    }

    /**
     * Cancel the query if some of its participants are down.
     *
     * @param memberIds Member IDs.
     * @return {@code true} if the query cancel was initiated.
     */
    public boolean tryCancelOnMemberLeave(Collection<UUID> memberIds) {
        Set<UUID> missingMemberIds = null;

        if (isInitiator()) {
            // Initiator checks all participants.
            if (!memberIds.containsAll(getParticipants())) {
                missingMemberIds = new HashSet<>(getParticipants());
                missingMemberIds.removeAll(memberIds);
            }
        } else {
            // Participant checks only initiator. If it is down, we will fail the query locally. If it is up, then it will
            // notice failure of other participants.
            UUID initiatorMemberId = queryId.getMemberId();

            if (!memberIds.contains(initiatorMemberId)) {
                missingMemberIds = Collections.singleton(initiatorMemberId);
            }
        }

        if (missingMemberIds == null) {
            return false;
        }

        assert !missingMemberIds.isEmpty();

        cancel(QueryException.memberLeave(missingMemberIds));

        return true;
    }

    /**
     * Attempts to cancel the query if timeout has reached.
     *
     * @return {@code true} if the query cancel was initiated.
     */
    public boolean tryCancelOnTimeout() {
        if (!isInitiator()) {
            return false;
        }

        long timeout = initiatorState.getTimeout();

        if (timeout > 0 && clockProvider.currentTimeMillis() - startTime > timeout) {
            cancel(QueryException.timeout(timeout));

            return true;
        } else {
            return false;
        }
    }

    /**
     * Check if the query check is required for the given query.
     *
     * @param checkFrequency Frequency of state checks in milliseconds.
     * @return {@code true} if query check should be initiated, {@code false} otherwise.
     */
    public boolean requestQueryCheck(long checkFrequency) {
        // No need to check the initiator because creation of its state happens-before sending of any messages,
        // so it is never stale.
        if (isInitiator()) {
            return false;
        }

        // If the state received an EXECUTE request, then no need to check it because EXECUTE happens-before any CANCEL
        // request.
        if (distributedState.isStarted()) {
            return false;
        }

        long currentTime = clockProvider.currentTimeMillis();

        if (currentTime - checkTime < checkFrequency) {
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

    private Collection<UUID> getParticipants() {
        return initiatorState.getPlan().getMemberIds();
    }
}
