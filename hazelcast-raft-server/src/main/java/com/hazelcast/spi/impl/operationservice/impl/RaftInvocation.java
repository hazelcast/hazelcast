/*
 *  Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.MemberLeftException;
import com.hazelcast.nio.Address;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.exception.LeaderDemotedException;
import com.hazelcast.raft.exception.NotLeaderException;
import com.hazelcast.raft.impl.RaftMemberImpl;
import com.hazelcast.spi.ExceptionAction;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.exception.CallerNotMemberException;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.operationservice.impl.RaftInvocationContext.MemberCursor;

import static com.hazelcast.spi.ExceptionAction.RETRY_INVOCATION;
import static com.hazelcast.spi.ExceptionAction.THROW_EXCEPTION;
import static com.hazelcast.spi.InvocationBuilder.DEFAULT_CALL_TIMEOUT;
import static com.hazelcast.spi.InvocationBuilder.DEFAULT_DESERIALIZE_RESULT;
import static com.hazelcast.spi.InvocationBuilder.DEFAULT_TRY_COUNT;
import static com.hazelcast.spi.InvocationBuilder.DEFAULT_TRY_PAUSE_MILLIS;

/**
 * TODO: Javadoc Pending...
 */
public class RaftInvocation extends Invocation {

    private final RaftInvocationContext raftInvocationContext;
    private final RaftGroupId groupId;
    private final boolean canFailOnIndeterminateOperationState;
    private volatile MemberCursor memberCursor;
    private volatile RaftMemberImpl lastInvocationEndpoint;

    public RaftInvocation(Context context, RaftInvocationContext raftInvocationContext, RaftGroupId groupId,
            Operation op, boolean canFailOnIndeterminateOperationState) {
        super(context, op, null, DEFAULT_TRY_COUNT, DEFAULT_TRY_PAUSE_MILLIS, DEFAULT_CALL_TIMEOUT, DEFAULT_DESERIALIZE_RESULT);
        this.raftInvocationContext = raftInvocationContext;
        this.groupId = groupId;
        this.canFailOnIndeterminateOperationState = canFailOnIndeterminateOperationState;

        int partitionId = context.partitionService.getPartitionId(groupId);
        op.setPartitionId(partitionId);
    }

    @Override
    protected Address getTarget() {
        RaftMemberImpl targetEndpoint = getTargetEndpoint();
        lastInvocationEndpoint = targetEndpoint;
        return targetEndpoint != null ? targetEndpoint.getAddress() : null;
    }

    @Override
    void notifyNormalResponse(Object value, int expectedBackups) {
        super.notifyNormalResponse(value, expectedBackups);
        raftInvocationContext.setKnownLeader(groupId, lastInvocationEndpoint);
    }

    @Override
    protected ExceptionAction onException(Throwable t) {
        raftInvocationContext.updateKnownLeaderOnFailure(groupId, t);

        if (shouldFailOnIndeterminateOperationState() && (t instanceof MemberLeftException)) {
            return THROW_EXCEPTION;
        }
        return isRetryable(t) ? RETRY_INVOCATION : op.onInvocationException(t);
    }

    private boolean isRetryable(Throwable cause) {
        return cause instanceof NotLeaderException
                || cause instanceof LeaderDemotedException
                || cause instanceof MemberLeftException
                || cause instanceof CallerNotMemberException
                || cause instanceof TargetNotMemberException;
    }

    private RaftMemberImpl getTargetEndpoint() {
        RaftMemberImpl target = raftInvocationContext.getKnownLeader(groupId);
        if (target != null) {
            return target;
        }

        MemberCursor cursor = memberCursor;
        if (cursor == null || !cursor.advance()) {
            cursor = raftInvocationContext.newMemberCursor(groupId);
            if (!cursor.advance()) {
                return null;
            }
            memberCursor = cursor;
        }
        return cursor.get();
    }

    @Override
    protected boolean shouldFailOnIndeterminateOperationState() {
        return canFailOnIndeterminateOperationState && raftInvocationContext.failOnIndeterminateOperationState;
    }
}
