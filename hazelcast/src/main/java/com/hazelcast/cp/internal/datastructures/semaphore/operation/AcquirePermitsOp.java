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

package com.hazelcast.cp.internal.datastructures.semaphore.operation;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.ISemaphore;
import com.hazelcast.cp.internal.CallerAware;
import com.hazelcast.cp.internal.IndeterminateOperationStateAware;
import com.hazelcast.cp.internal.datastructures.semaphore.AcquireInvocationKey;
import com.hazelcast.cp.internal.datastructures.semaphore.AcquireResult;
import com.hazelcast.cp.internal.datastructures.semaphore.Semaphore;
import com.hazelcast.cp.internal.datastructures.semaphore.SemaphoreDataSerializerHook;
import com.hazelcast.cp.internal.datastructures.semaphore.SemaphoreService;
import com.hazelcast.cp.internal.raft.impl.util.PostponedResponse;
import com.hazelcast.cluster.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.UUID;

import static com.hazelcast.cp.internal.datastructures.semaphore.AcquireResult.AcquireStatus.SUCCESSFUL;
import static com.hazelcast.cp.internal.datastructures.semaphore.AcquireResult.AcquireStatus.WAIT_KEY_ADDED;
import static com.hazelcast.cp.internal.session.AbstractProxySessionManager.NO_SESSION_ID;

/**
 * Operation for {@link ISemaphore#acquire()}
 *
 * @see Semaphore#acquire(AcquireInvocationKey, boolean)
 */
public class AcquirePermitsOp extends AbstractSemaphoreOp implements CallerAware, IndeterminateOperationStateAware {

    private int permits;
    private long timeoutMs;
    private Address callerAddress;
    private long callId;

    public AcquirePermitsOp() {
    }

    public AcquirePermitsOp(String name, long sessionId, long threadId, UUID invocationUid, int permits, long timeoutMs) {
        super(name, sessionId, threadId, invocationUid);
        this.permits = permits;
        this.timeoutMs = timeoutMs;
    }

    @Override
    public Object run(CPGroupId groupId, long commitIndex) {
        SemaphoreService service = getService();
        AcquireInvocationKey key = new AcquireInvocationKey(commitIndex, invocationUid, callerAddress, callId,
                getSemaphoreEndpoint(), permits);
        AcquireResult result = service.acquirePermits(groupId, name, key, timeoutMs);

        if (result.status() == WAIT_KEY_ADDED) {
            return PostponedResponse.INSTANCE;
        }

        return result.status() == SUCCESSFUL;
    }

    @Override
    public void setCaller(Address callerAddress, long callId) {
        this.callerAddress = callerAddress;
        this.callId = callId;
    }

    @Override
    public boolean isRetryableOnIndeterminateOperationState() {
        return sessionId != NO_SESSION_ID;
    }

    @Override
    public int getClassId() {
        return SemaphoreDataSerializerHook.ACQUIRE_PERMITS_OP;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeInt(permits);
        out.writeLong(timeoutMs);
        out.writeObject(callerAddress);
        out.writeLong(callId);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        permits = in.readInt();
        timeoutMs = in.readLong();
        callerAddress = in.readObject();
        callId = in.readLong();
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);
        sb.append(", permits=").append(permits)
          .append(", timeoutMs=").append(timeoutMs)
          .append(", callerAddress=").append(callerAddress)
          .append(", callId=").append(callId);
    }
}
