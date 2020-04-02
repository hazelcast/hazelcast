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

import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.impl.QueryMetadata;
import com.hazelcast.sql.impl.QueryResultProducer;
import com.hazelcast.sql.impl.QueryId;
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

    /** Start time. */
    private final long startTime;

    /** Local member ID. */
    private final UUID localMemberId;

    /** Error which caused query completion. */
    private volatile HazelcastSqlException completionError;

    /** Whether query check was requested. */
    private volatile boolean queryCheckRequested;

    private QueryState(
        QueryId queryId,
        long startTime,
        UUID localMemberId,
        QueryStateCompletionCallback completionCallback,
        boolean initiator,
        long initiatorTimeout,
        Plan initiatorPlan,
        QueryMetadata initiatorMetadata,
        QueryResultProducer initiatorRowSource
    ) {
        // Set common state.
        this.queryId = queryId;
        this.completionCallback = completionCallback;
        this.startTime = startTime;
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
    }

    public static QueryState createInitiatorState(
        QueryId queryId,
        UUID localMemberId,
        QueryStateCompletionCallback completionCallback,
        long initiatorTimeout,
        Plan initiatorPlan,
        QueryMetadata initiatorMetadata,
        QueryResultProducer initiatorResultProducer
    ) {
        return new QueryState(
            queryId,
            System.currentTimeMillis(),
            localMemberId,
            completionCallback,
            true,
            initiatorTimeout,
            initiatorPlan,
            initiatorMetadata,
            initiatorResultProducer
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
        QueryStateCompletionCallback completionCallback
    ) {
        return new QueryState(
            queryId,
            System.currentTimeMillis(),
            localMemberId,
            completionCallback,
            false,
            -1,
            null,
            null,
            null
        );
    }

    public QueryId getQueryId() {
        return queryId;
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
        // Wrap into common SQL exception if needed.
        if (!(error instanceof HazelcastSqlException)) {
            error = HazelcastSqlException.error(SqlErrorCode.GENERIC, error.getMessage(), error);
        }

        HazelcastSqlException error0 = (HazelcastSqlException) error;

        // Calculate the originating member.
        UUID originatingMemberId = error0.getOriginatingMemberId();

        if (originatingMemberId == null) {
            originatingMemberId = localMemberId;
        }

        // Determine members which should be notified.
        boolean initiator = queryId.getMemberId().equals(localMemberId);
        boolean propagate = originatingMemberId.equals(localMemberId) || initiator;

        Collection<UUID> memberIds = propagate ? initiator
            ? getParticipantsWithoutInitiator() : Collections.singletonList(queryId.getMemberId()) : Collections.emptyList();

        // Invoke the completion callback.
        if (!completionGuard.compareAndSet(false, true)) {
            return;
        }

        assert completionCallback != null;

        completionCallback.onError(
            queryId,
            error0.getCode(),
            error0.getMessage(),
            originatingMemberId,
            memberIds
        );

        completionError = error0;

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

        HazelcastSqlException error = HazelcastSqlException.memberLeave(missingMemberIds);

        cancel(error);

        return true;
    }

    /**
     * Attempts to cancel the query if timeout has reached.
     */
    public boolean tryCancelOnTimeout() {
        // Get timeout from either initiator state of initialized distributed state.
        Long timeout;

        if (isInitiator()) {
            timeout = initiatorState.getTimeout();
        } else {
            timeout = distributedState.getTimeout();
        }

        if (timeout != null && timeout > 0 && System.currentTimeMillis() > startTime + timeout) {
            HazelcastSqlException error = HazelcastSqlException.error(
                SqlErrorCode.TIMEOUT, "Query is cancelled due to timeout (" + timeout + " ms)"
            );

            cancel(error);

            return true;
        } else {
            return false;
        }
    }

    public boolean requestQueryCheck() {
        // We never need to check queries started locally.
        if (isInitiator()) {
            return false;
        }

        // Distributed query is initialized, no need to worry.
        if (distributedState.isStarted()) {
            return false;
        }

        // Don't bother if the query is relatively recent.
        if (System.currentTimeMillis() - startTime < STATE_CHECK_FREQUENCY * 2) {
            return false;
        }

        // Staleness check already requested, do not bother.
        if (queryCheckRequested) {
            return false;
        }

        // Otherwise schedule the check,
        queryCheckRequested = true;

        return true;
    }

    @Override
    public void checkCancelled() {
        HazelcastSqlException completionError0 = completionError;

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
        assert queryId.getMemberId().equals(localMemberId);

        Set<UUID> res = new HashSet<>(initiatorState.getPlan().getDataMemberIds());

        res.remove(localMemberId);

        return res;
    }
}
