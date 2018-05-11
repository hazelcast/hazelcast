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
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftOp;
import com.hazelcast.raft.service.lock.LockEndpoint;
import com.hazelcast.raft.service.lock.RaftLockDataSerializerHook;
import com.hazelcast.raft.service.lock.RaftLockService;

import java.io.IOException;

/**
 * TODO: Javadoc Pending...
 */
public class GetLockCountOp extends RaftOp implements IdentifiedDataSerializable {

    private static int NO_SESSION = -1;

    private String name;
    private long sessionId = NO_SESSION;
    private long threadId;

    public GetLockCountOp() {
    }

    public GetLockCountOp(String name) {
        this.name = name;
    }

    public GetLockCountOp(String name, long sessionId, long threadId) {
        this.name = name;
        this.sessionId = sessionId;
        this.threadId = threadId;
    }

    @Override
    public Object run(RaftGroupId groupId, long commitIndex) {
        RaftLockService service = getService();
        LockEndpoint endpoint = (sessionId != NO_SESSION) ? new LockEndpoint(sessionId, threadId) : null;

        return service.getLockCount(groupId, name, endpoint);
    }

    @Override
    public final String getServiceName() {
        return RaftLockService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return RaftLockDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftLockDataSerializerHook.GET_LOCK_COUNT_OP;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeLong(sessionId);
        out.writeLong(threadId);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        sessionId = in.readLong();
        threadId = in.readLong();
    }

    @Override
    protected void toString(StringBuilder sb) {
        sb.append(", name=").append(name);
        sb.append(", sessionId=").append(sessionId);
        sb.append(", threadId=").append(threadId);
    }
}
