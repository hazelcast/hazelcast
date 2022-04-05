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

package com.hazelcast.cp.internal.datastructures.lock.operation;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.CallerAware;
import com.hazelcast.cp.internal.IndeterminateOperationStateAware;
import com.hazelcast.cp.internal.datastructures.lock.AcquireResult;
import com.hazelcast.cp.internal.datastructures.lock.Lock;
import com.hazelcast.cp.internal.datastructures.lock.LockInvocationKey;
import com.hazelcast.cp.internal.datastructures.lock.LockDataSerializerHook;
import com.hazelcast.cp.internal.datastructures.lock.LockService;
import com.hazelcast.cp.internal.raft.impl.util.PostponedResponse;
import com.hazelcast.cp.lock.FencedLock;
import com.hazelcast.cluster.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.UUID;

import static com.hazelcast.cp.internal.datastructures.lock.AcquireResult.AcquireStatus.WAIT_KEY_ADDED;

/**
 * Operation for {@link FencedLock#lock()}
 *
 * @see Lock#acquire(LockInvocationKey, boolean)
 */
public class LockOp extends AbstractLockOp implements CallerAware, IndeterminateOperationStateAware {

    private Address callerAddress;
    private long callId;

    public LockOp() {
    }

    public LockOp(String name, long sessionId, long threadId, UUID invUid) {
        super(name, sessionId, threadId, invUid);
    }

    @Override
    public Object run(CPGroupId groupId, long commitIndex) {
        LockService service = getService();
        LockInvocationKey key = new LockInvocationKey(commitIndex, invocationUid, callerAddress, callId, getLockEndpoint());
        AcquireResult result = service.acquire(groupId, name, key, -1L);
        if (result.status() == WAIT_KEY_ADDED) {
            return PostponedResponse.INSTANCE;
        }

        // SUCCESSFUL or FAILED
        return result.fence();
    }

    @Override
    public void setCaller(Address callerAddress, long callId) {
        this.callerAddress = callerAddress;
        this.callId = callId;
    }

    @Override
    public boolean isRetryableOnIndeterminateOperationState() {
        return true;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeObject(callerAddress);
        out.writeLong(callId);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        callerAddress = in.readObject();
        callId = in.readLong();
    }

    @Override
    public int getClassId() {
        return LockDataSerializerHook.LOCK_OP;
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);
        sb.append(", callerAddress=").append(callerAddress);
        sb.append(", callId=").append(callId);
    }
}
