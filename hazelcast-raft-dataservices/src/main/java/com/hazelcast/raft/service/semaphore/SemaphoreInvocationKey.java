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

package com.hazelcast.raft.service.semaphore;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.service.blocking.WaitKey;

import java.io.IOException;
import java.util.UUID;

import static com.hazelcast.util.Preconditions.checkTrue;

/**
 * Represents acquire() invocation of a semaphore endpoint
 * A SemaphoreInvocationKey either holds some permits or resides in the wait queue.
 * Combination of a session id and a thread id a single-threaded unique entity.
 * When it sends a request X, it can either retry this request X, or send a new request Y.
 * After it sends request Y, it will not retry request X anymore.
 */
public class SemaphoreInvocationKey implements WaitKey, IdentifiedDataSerializable {

    private String name;
    private long commitIndex;
    private long sessionId;
    private long threadId;
    private UUID invocationUid;
    private int permits;

    SemaphoreInvocationKey() {
    }

    SemaphoreInvocationKey(String name, long commitIndex, long sessionId, long threadId, UUID invocationUid, int permits) {
        checkTrue(permits > 0, "permits must be positive");
        this.name = name;
        this.commitIndex = commitIndex;
        this.sessionId = sessionId;
        this.threadId = threadId;
        this.invocationUid = invocationUid;
        this.permits = permits;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public long commitIndex() {
        return commitIndex;
    }

    @Override
    public long sessionId() {
        return sessionId;
    }

    public long threadId() {
        return threadId;
    }

    public UUID invocationUid() {
        return invocationUid;
    }

    public int permits() {
        return permits;
    }

    @Override
    public int getFactoryId() {
        return RaftSemaphoreDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftSemaphoreDataSerializerHook.SEMAPHORE_INVOCATION_KEY;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeLong(commitIndex);
        out.writeLong(sessionId);
        out.writeLong(threadId);
        out.writeLong(invocationUid.getLeastSignificantBits());
        out.writeLong(invocationUid.getMostSignificantBits());
        out.writeInt(permits);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        commitIndex = in.readLong();
        sessionId = in.readLong();
        threadId = in.readLong();
        long least = in.readLong();
        long most = in.readLong();
        invocationUid = new UUID(most, least);
        permits = in.readInt();
    }

    @Override
    @SuppressWarnings("checkstyle:npathcomplexity")
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SemaphoreInvocationKey that = (SemaphoreInvocationKey) o;

        if (commitIndex != that.commitIndex) {
            return false;
        }
        if (sessionId != that.sessionId) {
            return false;
        }
        if (threadId != that.threadId) {
            return false;
        }
        if (permits != that.permits) {
            return false;
        }
        if (!name.equals(that.name)) {
            return false;
        }
        return invocationUid.equals(that.invocationUid);
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + (int) (commitIndex ^ (commitIndex >>> 32));
        result = 31 * result + (int) (sessionId ^ (sessionId >>> 32));
        result = 31 * result + (int) (threadId ^ (threadId >>> 32));
        result = 31 * result + invocationUid.hashCode();
        result = 31 * result + permits;
        return result;
    }

    @Override
    public String toString() {
        return "SemaphoreInvocationKey{" + "name='" + name + '\'' + ", commitIndex=" + commitIndex + ", sessionId=" + sessionId
                + ", threadId=" + threadId + ", invocationUid=" + invocationUid + ", permits=" + permits + '}';
    }
}
