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

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.cluster.Member;
import com.hazelcast.core.IndeterminateOperationState;
import com.hazelcast.core.LocalMemberResetException;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.CPMember;
import com.hazelcast.cp.exception.LeaderDemotedException;
import com.hazelcast.cp.exception.NotLeaderException;
import com.hazelcast.cp.exception.StaleAppendRequestException;
import com.hazelcast.cp.internal.IndeterminateOperationStateAware;
import com.hazelcast.cluster.Address;
import com.hazelcast.spi.exception.CallerNotMemberException;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.operationservice.ExceptionAction;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.impl.RaftInvocationContext.MemberCursor;

import static com.hazelcast.spi.impl.operationservice.ExceptionAction.RETRY_INVOCATION;
import static com.hazelcast.spi.impl.operationservice.ExceptionAction.THROW_EXCEPTION;
import static com.hazelcast.spi.impl.operationservice.InvocationBuilder.DEFAULT_DESERIALIZE_RESULT;

/**
 * A {@link Invocation} implementation that realizes an operation invocation on the leader node of the given Raft group.
 * Internally handles Raft-related exceptions.
 */
public class RaftInvocation extends Invocation<CPMember> {

    private final RaftInvocationContext raftInvocationContext;
    private final CPGroupId groupId;
    private volatile MemberCursor memberCursor;
    private volatile CPMember lastInvocationEndpoint;
    private volatile Throwable indeterminateException;

    public RaftInvocation(Context context, RaftInvocationContext raftInvocationContext, CPGroupId groupId, Operation op,
                          int retryCount, long retryPauseMillis, long callTimeoutMillis) {
        this(context, raftInvocationContext, groupId, op, retryCount, retryPauseMillis, callTimeoutMillis,
                DEFAULT_DESERIALIZE_RESULT);
    }

    public RaftInvocation(Context context, RaftInvocationContext raftInvocationContext, CPGroupId groupId, Operation op,
            int retryCount, long retryPauseMillis, long callTimeoutMillis, boolean deserializeResponse) {
        super(context, op, null, retryCount, retryPauseMillis, callTimeoutMillis, deserializeResponse, null);
        this.raftInvocationContext = raftInvocationContext;
        this.groupId = groupId;

        int partitionId = raftInvocationContext.getCPGroupPartitionId(groupId);
        op.setPartitionId(partitionId);
    }

    @Override
    CPMember getInvocationTarget() {
        CPMember target = getTargetEndpoint();
        lastInvocationEndpoint = target;
        return target;
    }

    @Override
    Address toTargetAddress(CPMember target) {
        return target.getAddress();
    }

    @Override
    Member toTargetMember(CPMember target) {
        // CPMember.uuid can be different from Member.uuid
        // During split-brain merge, Member.uuid changes but CPMember.uuid remains the same.
        return context.clusterService.getMember(target.getAddress());
    }

    @Override
    void notifyNormalResponse(Object value, int expectedBackups) {
        assert !(value instanceof Throwable) : "Throwable value " + value + " not allowed";

        super.notifyNormalResponse(value, expectedBackups);
        // TODO [basri] maybe we should update known leader only if the result is not an exception?
        raftInvocationContext.setKnownLeader(groupId, lastInvocationEndpoint);
    }

    @Override
    void notifyError(Object error) {
        if (error instanceof Throwable
                && ((Throwable) error).getCause() instanceof LocalMemberResetException) {
            // Raft does not care about split-brain merge.
            return;
        }
        super.notifyError(error);
    }

    @Override
    protected void notifyThrowable(Throwable cause, int expectedBackups) {
        if (!(cause instanceof IndeterminateOperationState) && indeterminateException != null && isRetryable(cause)) {
            cause = indeterminateException;
        }
        super.notifyThrowable(cause, expectedBackups);
    }

    @Override
    protected ExceptionAction onException(Throwable t) {
        raftInvocationContext.updateKnownLeaderOnFailure(groupId, t);

        if (t instanceof IndeterminateOperationState) {
            if (isRetryableOnIndeterminateOperationState()) {
                if (indeterminateException == null) {
                    indeterminateException = t;
                }
                return RETRY_INVOCATION;
            } else if (shouldFailOnIndeterminateOperationState()) {
                return THROW_EXCEPTION;
            } else if (indeterminateException == null) {
                indeterminateException = t;
            }
        }

        return isRetryable(t) ? RETRY_INVOCATION : op.onInvocationException(t);
    }

    @SuppressWarnings("checkstyle:booleanexpressioncomplexity")
    private boolean isRetryable(Object cause) {
        return cause instanceof NotLeaderException
                || cause instanceof LeaderDemotedException
                || cause instanceof StaleAppendRequestException
                || cause instanceof MemberLeftException
                || cause instanceof CallerNotMemberException
                || cause instanceof TargetNotMemberException;
    }

    @Override
    boolean skipTimeoutDetection() {
        return false;
    }

    private CPMember getTargetEndpoint() {
        CPMember target = raftInvocationContext.getKnownLeader(groupId);
        if (target != null) {
            return target;
        }

        MemberCursor cursor = memberCursor;
        if (cursor == null || !cursor.advance()) {
            cursor = raftInvocationContext.newMemberCursor();
            if (!cursor.advance()) {
                return null;
            }
            memberCursor = cursor;
        }
        return cursor.get();
    }

    private boolean isRetryableOnIndeterminateOperationState() {
        if (op instanceof IndeterminateOperationStateAware) {
            return ((IndeterminateOperationStateAware) op).isRetryableOnIndeterminateOperationState();
        }

        return false;
    }

    @Override
    protected boolean shouldFailOnIndeterminateOperationState() {
        return raftInvocationContext.shouldFailOnIndeterminateOperationState();
    }
}
