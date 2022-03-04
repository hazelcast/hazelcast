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

import com.hazelcast.cp.internal.RaftOp;
import com.hazelcast.cp.internal.datastructures.semaphore.SemaphoreDataSerializerHook;
import com.hazelcast.cp.internal.datastructures.semaphore.SemaphoreService;
import com.hazelcast.cp.internal.datastructures.semaphore.SemaphoreEndpoint;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.UUID;

import static com.hazelcast.internal.util.UUIDSerializationUtil.readUUID;
import static com.hazelcast.internal.util.UUIDSerializationUtil.writeUUID;

/**
 * Base class for operations of Raft-based semaphore
 */
abstract class AbstractSemaphoreOp extends RaftOp implements IdentifiedDataSerializable {

    protected String name;
    protected long sessionId;
    protected long threadId;
    protected UUID invocationUid;

    AbstractSemaphoreOp() {
    }

    AbstractSemaphoreOp(String name, long sessionId, long threadId, UUID invocationUid) {
        this.name = name;
        this.sessionId = sessionId;
        this.threadId = threadId;
        this.invocationUid = invocationUid;
    }

    SemaphoreEndpoint getSemaphoreEndpoint() {
        return new SemaphoreEndpoint(sessionId, threadId);
    }

    @Override
    protected String getServiceName() {
        return SemaphoreService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return SemaphoreDataSerializerHook.F_ID;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(name);
        out.writeLong(sessionId);
        out.writeLong(threadId);
        writeUUID(out, invocationUid);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readString();
        sessionId = in.readLong();
        threadId = in.readLong();
        invocationUid = readUUID(in);
    }

    @Override
    protected void toString(StringBuilder sb) {
        sb.append(", name=").append(name)
          .append(", threadId=").append(threadId)
          .append(", sessionId=").append(sessionId)
          .append(", invocationUid=").append(invocationUid);
    }
}
