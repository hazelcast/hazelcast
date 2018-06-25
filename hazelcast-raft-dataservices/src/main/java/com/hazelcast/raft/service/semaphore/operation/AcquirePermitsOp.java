/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.raft.service.semaphore.operation;

import com.hazelcast.core.ISemaphore;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.IndeterminateOperationStateAware;
import com.hazelcast.raft.impl.util.PostponedResponse;
import com.hazelcast.raft.service.semaphore.RaftSemaphore;
import com.hazelcast.raft.service.semaphore.RaftSemaphoreDataSerializerHook;
import com.hazelcast.raft.service.semaphore.RaftSemaphoreService;
import com.hazelcast.raft.service.semaphore.SemaphoreInvocationKey;

import java.io.IOException;
import java.util.UUID;

import static com.hazelcast.raft.service.session.AbstractSessionManager.NO_SESSION_ID;

/**
 * Operation for {@link ISemaphore#acquire()}
 *
 * @see RaftSemaphore#acquire(SemaphoreInvocationKey, boolean)
 */
public class AcquirePermitsOp extends AbstractSemaphoreOp implements IndeterminateOperationStateAware {

    private int permits;
    private long timeoutMs;

    public AcquirePermitsOp() {
    }

    public AcquirePermitsOp(String name, long sessionId, long threadId, UUID invocationUid, int permits, long timeoutMs) {
        super(name, sessionId, threadId, invocationUid);
        this.permits = permits;
        this.timeoutMs = timeoutMs;
    }

    @Override
    public Object run(RaftGroupId groupId, long commitIndex) {
        RaftSemaphoreService service = getService();
        boolean acquired = service.acquirePermits(groupId, commitIndex, name, sessionId, threadId,
                invocationUid, permits, timeoutMs);
        if (!acquired && timeoutMs != 0) {
            return PostponedResponse.INSTANCE;
        }
        return acquired;
    }

    @Override
    public boolean isRetryableOnIndeterminateOperationState() {
        return sessionId != NO_SESSION_ID;
    }

    @Override
    public int getId() {
        return RaftSemaphoreDataSerializerHook.ACQUIRE_PERMITS_OP;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeInt(permits);
        out.writeLong(timeoutMs);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        permits = in.readInt();
        timeoutMs = in.readLong();
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);
        sb.append(", permits=").append(permits)
          .append(", timeoutMs=").append(timeoutMs);
    }
}
