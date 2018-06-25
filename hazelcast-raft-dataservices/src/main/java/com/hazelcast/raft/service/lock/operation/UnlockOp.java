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

package com.hazelcast.raft.service.lock.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.IndeterminateOperationStateAware;
import com.hazelcast.raft.service.lock.FencedLock;
import com.hazelcast.raft.service.lock.LockEndpoint;
import com.hazelcast.raft.service.lock.RaftLockDataSerializerHook;
import com.hazelcast.raft.service.lock.RaftLockService;

import java.io.IOException;
import java.util.UUID;

/**
 * Operation for {@link FencedLock#unlock()}
 *
 * @see com.hazelcast.raft.service.lock.RaftLock#release(LockEndpoint, UUID, int)
 */
public class UnlockOp extends AbstractLockOp implements IndeterminateOperationStateAware {

    private int lockCount;

    public UnlockOp() {
    }

    public UnlockOp(String name, long sessionId, long threadId, UUID invUid, int lockCount) {
        super(name, sessionId, threadId, invUid);
        this.lockCount = lockCount;
    }

    @Override
    public Object run(RaftGroupId groupId, long commitIndex) {
        RaftLockService service = getService();
        service.release(groupId, name, getLockEndpoint(), invocationUid, lockCount);
        return true;
    }

    @Override
    public boolean isRetryableOnIndeterminateOperationState() {
        return true;
    }

    @Override
    public int getId() {
        return RaftLockDataSerializerHook.UNLOCK_OP;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeInt(lockCount);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        lockCount = in.readInt();
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);
        sb.append(", lockCount=").append(lockCount);
    }
}
