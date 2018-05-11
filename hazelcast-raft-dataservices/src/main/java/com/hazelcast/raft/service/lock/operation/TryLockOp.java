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

package com.hazelcast.raft.service.lock.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.util.PostponedResponse;
import com.hazelcast.raft.service.lock.RaftLockDataSerializerHook;
import com.hazelcast.raft.service.lock.RaftLockService;

import java.io.IOException;
import java.util.UUID;

/**
 * TODO: Javadoc Pending...
 */
public class TryLockOp extends AbstractLockOp {

    private long timeoutMs;

    public TryLockOp() {
    }

    public TryLockOp(String name, long sessionId, long threadId, UUID invUid, long timeoutMs) {
        super(name, sessionId, threadId, invUid);
        this.timeoutMs = timeoutMs;
    }

    @Override
    public Object run(RaftGroupId groupId, long commitIndex) {
        RaftLockService service = getService();
        if (service.tryAcquire(groupId, name, getLockEndpoint(), commitIndex, invocationUid, timeoutMs)) {
            return commitIndex;
        }

        return timeoutMs > 0 ? PostponedResponse.INSTANCE : RaftLockService.INVALID_FENCE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeLong(timeoutMs);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        timeoutMs = in.readLong();
    }

    @Override
    public int getId() {
        return RaftLockDataSerializerHook.TRY_LOCK_OP;
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);
        sb.append(", timeoutMs=").append(timeoutMs);
    }
}
